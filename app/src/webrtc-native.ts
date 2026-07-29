/**
 * WebRTC Native Manager
 *
 * Manages WebRTC sessions using the BaichuanWebRTCServer from the library.
 * Multiple browsers (phone + desktop) may view the same camera/profile at once:
 * one BaichuanWebRTCServer per camera+profile fans out a single native Preview
 * to N peer connections (library-side). Do not evict healthy sessions when a
 * new client joins — that was the multi-device kick bug.
 */

import {
  BaichuanWebRTCServer,
  type WebRTCOffer,
  type WebRTCAnswer,
  type WebRTCIceCandidate,
  type WebRTCSessionInfo,
} from "@apocaliss92/nodelink-js";
import { createSourceLogger } from "./logger.js";
import {
  getOrCreateApiConnection,
  getCameraInfo,
  sanitizeCameraName,
} from "./rtsp-manager.js";
import { getConfig, getSettings } from "./settings-store.js";
import { emitStreamClientsChanged } from "./events-manager.js";

const logger = createSourceLogger("webrtc-native");

function parsePortRange(
  value: string | undefined,
): [number, number] | undefined {
  if (!value) return undefined;
  const m = value.trim().match(/^\s*(\d+)\s*[-:]\s*(\d+)\s*$/);
  if (!m) return undefined;
  const min = Number(m[1]);
  const max = Number(m[2]);
  if (!Number.isFinite(min) || !Number.isFinite(max)) return undefined;
  if (min <= 0 || max <= 0 || min >= max) return undefined;
  return [min, max];
}

function parseCsv(value: string | undefined): string[] | undefined {
  if (!value) return undefined;
  const out = value
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  return out.length ? out : undefined;
}

// ============================================================================
// Types
// ============================================================================

interface WebRTCCameraSession {
  cameraId: string;
  profile: "main" | "sub" | "ext";
  server: BaichuanWebRTCServer;
  sessionId: string;
  serverKey: string;
}

interface SharedServerEntry {
  server: BaichuanWebRTCServer;
  cameraId: string;
  profile: "main" | "sub" | "ext";
  /** sessionIds attached to this server */
  sessionIds: Set<string>;
  enableIntercom: boolean;
}

// ============================================================================
// State
// ============================================================================

/** sessionId → session meta */
const activeSessions = new Map<string, WebRTCCameraSession>();

/** `${cameraId}:${profile}` → shared BaichuanWebRTCServer */
const sharedServers = new Map<string, SharedServerEntry>();

function serverKey(cameraId: string, profile: string): string {
  return `${cameraId}:${profile}`;
}

// ============================================================================
// Public API
// ============================================================================

/**
 * Create a new WebRTC session for a camera.
 * Concurrent viewers of the same camera/profile share one native Preview.
 */
export async function createWebRTCSession(
  cameraId: string,
  profile: "main" | "sub" | "ext",
  enableIntercom: boolean = false,
): Promise<{ sessionId: string; offer: WebRTCOffer }> {
  const config = getConfig();
  const camera = config.cameras.find(
    (c) => c.id === cameraId || sanitizeCameraName(c.name) === cameraId,
  );

  if (!camera) {
    throw new Error(`Camera ${cameraId} not found`);
  }

  const camInfo = getCameraInfo(camera.id);
  if (!camInfo || camInfo.status !== "connected") {
    throw new Error(`Camera ${camera.id} is not connected`);
  }

  const api = await getOrCreateApiConnection(camera.id);
  if (!api) {
    throw new Error(`Failed to get API connection for camera ${camera.id}`);
  }

  const key = serverKey(camera.id, profile);
  logger.info(
    `Creating WebRTC session for ${camera.name}/${profile} (intercom: ${enableIntercom}, sharedPeers=${sharedServers.get(key)?.sessionIds.size ?? 0})`,
  );

  // Prune only *dead* sessions (ICE failed / closed) so reconnects from the
  // same browser do not pile up, without kicking other healthy viewers.
  const dead = [...activeSessions.values()].filter((s) => {
    if (s.cameraId !== camera.id || s.profile !== profile) return false;
    const info = s.server.getSession(s.sessionId);
    if (!info) return true;
    return info.state === "failed" || info.state === "disconnected";
  });
  if (dead.length > 0) {
    logger.info(
      `Cleaning ${dead.length} dead WebRTC session(s) for ${camera.name}/${profile}`,
    );
    await Promise.allSettled(dead.map((s) => closeWebRTCSession(s.sessionId)));
  }

  let entry = sharedServers.get(key);
  if (!entry) {
    const channel = camera.rtspChannel ?? 0;
    const settings = getSettings();
    const icePortRange = parsePortRange(settings.webrtc?.icePortRange);
    const iceAdditionalHostAddresses = parseCsv(
      settings.webrtc?.iceAdditionalHostAddresses,
    );

    // Prefer intercom-capable server so a later talk client can use the DC.
    const server = new BaichuanWebRTCServer({
      api,
      channel,
      profile,
      enableIntercom: enableIntercom,
      icePortRange,
      iceAdditionalHostAddresses,
      logger: (
        level: "debug" | "info" | "warn" | "error",
        message: string,
      ) => {
        logger[level](message);
      },
    });

    server.on("session-connected", ({ sessionId }: { sessionId: string }) => {
      logger.info(`WebRTC session ${sessionId} connected`);
    });

    server.on("session-closed", ({ sessionId }: { sessionId: string }) => {
      logger.info(`WebRTC session ${sessionId} closed (library event)`);
      // Library already closed the peer; drop our bookkeeping if still present.
      const meta = activeSessions.get(sessionId);
      if (meta) {
        activeSessions.delete(sessionId);
        const se = sharedServers.get(meta.serverKey);
        se?.sessionIds.delete(sessionId);
        if (se && se.sessionIds.size === 0) {
          sharedServers.delete(meta.serverKey);
          se.server.stop().catch(() => {});
        }
        const count = [...activeSessions.values()].filter(
          (s) =>
            s.cameraId === meta.cameraId && s.profile === meta.profile,
        ).length;
        emitStreamClientsChanged(
          meta.cameraId,
          "webrtc",
          meta.profile,
          count,
        );
      }
    });

    server.on("intercom-started", ({ sessionId }: { sessionId: string }) => {
      logger.info(`Intercom started for session ${sessionId}`);
    });

    server.on("intercom-stopped", ({ sessionId }: { sessionId: string }) => {
      logger.info(`Intercom stopped for session ${sessionId}`);
    });

    entry = {
      server,
      cameraId: camera.id,
      profile,
      sessionIds: new Set(),
      enableIntercom,
    };
    sharedServers.set(key, entry);
    logger.info(
      `Created shared WebRTC server for ${camera.name}/${profile}`,
    );
  } else if (enableIntercom && !entry.enableIntercom) {
    logger.warn(
      `Shared server for ${camera.name}/${profile} was created without intercom; talk may be unavailable for this peer until all viewers disconnect`,
    );
  }

  const { sessionId, offer } = await entry.server.createSession();

  entry.sessionIds.add(sessionId);
  activeSessions.set(sessionId, {
    cameraId: camera.id,
    profile,
    server: entry.server,
    sessionId,
    serverKey: key,
  });

  const count = entry.sessionIds.size;
  emitStreamClientsChanged(camera.id, "webrtc", profile, count);

  logger.info(
    `WebRTC session ${sessionId} created for ${camera.name}/${profile} (viewers=${count})`,
  );

  return { sessionId, offer };
}

/**
 * Handle WebRTC answer from browser
 */
export async function handleWebRTCAnswer(
  sessionId: string,
  answer: WebRTCAnswer,
): Promise<void> {
  const session = activeSessions.get(sessionId);
  if (!session) {
    throw new Error(`Session ${sessionId} not found`);
  }

  await session.server.handleAnswer(sessionId, answer);
}

/**
 * Add ICE candidate from browser
 */
export async function addIceCandidate(
  sessionId: string,
  candidate: WebRTCIceCandidate,
): Promise<void> {
  const session = activeSessions.get(sessionId);
  if (!session) {
    throw new Error(`Session ${sessionId} not found`);
  }

  await session.server.addIceCandidate(sessionId, candidate);
}

/**
 * Close a WebRTC session
 */
export async function closeWebRTCSession(sessionId: string): Promise<void> {
  const session = activeSessions.get(sessionId);
  if (!session) {
    logger.warn(`Session ${sessionId} not found for close`);
    return;
  }

  activeSessions.delete(sessionId);
  const entry = sharedServers.get(session.serverKey);
  entry?.sessionIds.delete(sessionId);

  try {
    await session.server.closeSession(sessionId);
  } catch (e) {
    logger.warn(
      `closeSession failed for ${sessionId}: ${(e as Error).message}`,
    );
  }

  // Tear down shared server only when no peers remain.
  if (entry && entry.sessionIds.size === 0) {
    sharedServers.delete(session.serverKey);
    try {
      await session.server.stop();
    } catch {
      /* noop */
    }
  }

  const count = [...activeSessions.values()].filter(
    (s) => s.cameraId === session.cameraId && s.profile === session.profile,
  ).length;
  emitStreamClientsChanged(session.cameraId, "webrtc", session.profile, count);
}

/**
 * Get status of all WebRTC sessions
 */
export function getWebRTCStatus(): {
  sessions: Array<{
    sessionId: string;
    cameraId: string;
    profile: string;
    state: string;
    createdAt: string;
    stats: WebRTCSessionInfo["stats"];
  }>;
} {
  const sessions: Array<{
    sessionId: string;
    cameraId: string;
    profile: string;
    state: string;
    createdAt: string;
    stats: WebRTCSessionInfo["stats"];
  }> = [];

  for (const [sessionId, session] of activeSessions) {
    const info = session.server.getSession(sessionId);
    if (info) {
      sessions.push({
        sessionId,
        cameraId: session.cameraId,
        profile: session.profile,
        state: info.state,
        createdAt: info.createdAt.toISOString(),
        stats: info.stats,
      });
    }
  }

  return { sessions };
}

/**
 * Stop all WebRTC sessions
 */
export async function stopAllWebRTCSessions(): Promise<void> {
  logger.info(`Stopping all WebRTC sessions (${activeSessions.size} active)`);

  const promises: Promise<void>[] = [];
  for (const entry of sharedServers.values()) {
    promises.push(
      entry.server
        .stop()
        .catch((err: unknown) =>
          logger.error(`Error stopping shared server: ${err}`),
        ),
    );
  }

  await Promise.all(promises);
  activeSessions.clear();
  sharedServers.clear();

  logger.info("All WebRTC sessions stopped");
}

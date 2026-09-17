import type {
  Agent,
  AgentEvent,
  AgentSession,
  ControlResult,
  Conversation,
  CursorPage,
  Group,
  McodexIssue,
  Message,
  PageResult,
} from "./types";

function uniqueValues(values: string[]): string[] {
  return [...new Set(values.filter(Boolean).map((value) => value.replace(/\/$/, "")))];
}

function defaultApiBases(): string[] {
  if (typeof window === "undefined") {
    return ["http://127.0.0.1:8765"];
  }
  const protocol = window.location.protocol || "http:";
  const host = window.location.hostname || "127.0.0.1";
  return uniqueValues([`${protocol}//${host}:8765`, "http://127.0.0.1:8765"]);
}

function defaultEventStreamBases(): string[] {
  if (typeof window === "undefined") {
    return ["http://127.0.0.1:8765"];
  }
  const protocol = window.location.protocol || "http:";
  const host = window.location.hostname || "127.0.0.1";
  return uniqueValues([`${protocol}//${host}:8765`, "http://127.0.0.1:8765"]);
}

const API_BASES = (import.meta.env.VITE_SERVER_LOCAL_URL as string | undefined)?.trim()
  ? [(import.meta.env.VITE_SERVER_LOCAL_URL as string).replace(/\/$/, "")]
  : defaultApiBases();

const EVENT_STREAM_BASES = (import.meta.env.VITE_SERVER_LOCAL_URL as string | undefined)?.trim()
  ? [(import.meta.env.VITE_SERVER_LOCAL_URL as string).replace(/\/$/, "")]
  : defaultEventStreamBases();

export const DEFAULT_GROUP_MESSAGE_LIMIT = 80;
export const DEFAULT_HISTORY_LIMIT = 100;
export type IssueStatusFilter = "open" | "handled" | "all";
export type GroupStatusFilter = "active" | "archived" | "all";

let preferredApiBase: string | null = null;

function orderedApiBases(): string[] {
  if (!preferredApiBase) {
    return API_BASES;
  }
  return uniqueValues([preferredApiBase, ...API_BASES]);
}

async function fetchWithTimeout(url: string, init?: RequestInit, timeoutMs = 1600): Promise<Response> {
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    window.clearTimeout(timeout);
  }
}

async function fetchFromApi(path: string, init?: RequestInit): Promise<Response> {
  let lastError: unknown = null;
  for (const apiBase of orderedApiBases()) {
    let response: Response;
    try {
      response = await fetchWithTimeout(`${apiBase}${path}`, init);
    } catch (error) {
      if (preferredApiBase === apiBase) {
        preferredApiBase = null;
      }
      lastError = error;
      continue;
    }
    preferredApiBase = apiBase;
    if (response.ok) {
      return response;
    }
    let message = `HTTP ${response.status} from ${apiBase}`;
    try {
      const payload = (await response.json()) as { error?: unknown };
      if (typeof payload.error === "string" && payload.error.trim()) {
        message = payload.error;
      }
    } catch {
      // Keep the bounded HTTP fallback when the server did not return JSON.
    }
    throw new Error(message);
  }
  throw lastError instanceof Error ? lastError : new Error("server-local is unavailable");
}

async function readJson<T>(path: string): Promise<T> {
  const response = await fetchFromApi(path);
  return (await response.json()) as T;
}

async function sendJson<T>(path: string, body: Record<string, unknown>): Promise<T> {
  const response = await fetchFromApi(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  return (await response.json()) as T;
}

async function postWithoutBody<T>(path: string): Promise<T> {
  const response = await fetchFromApi(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: "{}",
  });
  return (await response.json()) as T;
}

export async function fetchGroups(status: GroupStatusFilter = "active"): Promise<Group[]> {
  const query = status === "active" ? "" : `?status=${encodeURIComponent(status)}`;
  const payload = await readJson<{ groups: Group[]; ok: boolean }>(`/api/groups${query}`);
  return payload.groups;
}

export async function archiveGroup(groupId: string): Promise<Group> {
  const payload = await postWithoutBody<{ group: Group; ok: boolean }>(
    `/api/groups/${encodeURIComponent(groupId)}/archive`,
  );
  return payload.group;
}

export async function restoreGroup(groupId: string): Promise<Group> {
  const payload = await postWithoutBody<{ group: Group; ok: boolean }>(
    `/api/groups/${encodeURIComponent(groupId)}/restore`,
  );
  return payload.group;
}

export async function fetchGroupAgents(groupId: string): Promise<Agent[]> {
  const payload = await readJson<{ agents: Agent[]; ok: boolean }>(`/api/groups/${groupId}/agents`);
  return payload.agents;
}

export async function fetchGroupConversations(groupId: string): Promise<Conversation[]> {
  const payload = await readJson<{ conversations: Conversation[]; ok: boolean }>(`/api/groups/${groupId}/conversations`);
  return payload.conversations;
}

export async function fetchGroupMessages(groupId: string, limit = DEFAULT_GROUP_MESSAGE_LIMIT): Promise<Message[]> {
  return (await fetchGroupMessagesPage(groupId, limit)).items;
}

function pageQuery(limit: number, cursor?: string): string {
  const params = new URLSearchParams({ limit: String(Math.max(0, limit)) });
  if (cursor) {
    params.set("cursor", cursor);
  }
  return `?${params.toString()}`;
}

export async function fetchGroupMessagesPage(
  groupId: string,
  limit: number,
  cursor?: string,
): Promise<PageResult<Message>> {
  const payload = await readJson<CursorPage<Message, "messages">>(
    `/api/groups/${groupId}/messages${pageQuery(limit, cursor)}`,
  );
  return { items: payload.messages, nextCursor: payload.next_cursor };
}

export async function fetchGroupIssues(groupId: string, limit = 50, status: IssueStatusFilter = "all"): Promise<McodexIssue[]> {
  const query = `?limit=${encodeURIComponent(String(Math.max(0, limit)))}&status=${encodeURIComponent(status)}`;
  const payload = await readJson<{ issues: McodexIssue[]; ok: boolean }>(`/api/groups/${groupId}/issues${query}`);
  return payload.issues;
}

export async function fetchConversationMessages(groupId: string, conversationId: string): Promise<Message[]> {
  return (await fetchConversationMessagesPage(groupId, conversationId, DEFAULT_HISTORY_LIMIT)).items;
}

export async function fetchConversationMessagesPage(
  groupId: string,
  conversationId: string,
  limit: number,
  cursor?: string,
): Promise<PageResult<Message>> {
  const payload = await readJson<CursorPage<Message, "messages">>(
    `/api/groups/${groupId}/conversations/${conversationId}/messages${pageQuery(limit, cursor)}`,
  );
  return { items: payload.messages, nextCursor: payload.next_cursor };
}

export async function fetchAgentSessions(agentId: string): Promise<AgentSession[]> {
  return (await fetchAgentSessionsPage(agentId, DEFAULT_HISTORY_LIMIT)).items;
}

export async function fetchAgentSessionsPage(
  agentId: string,
  limit: number,
  cursor?: string,
): Promise<PageResult<AgentSession>> {
  const payload = await readJson<CursorPage<AgentSession, "sessions">>(
    `/api/agents/${agentId}/sessions${pageQuery(limit, cursor)}`,
  );
  return { items: payload.sessions, nextCursor: payload.next_cursor };
}

export async function fetchAgentEvents(agentId: string): Promise<AgentEvent[]> {
  return (await fetchAgentEventsPage(agentId, DEFAULT_HISTORY_LIMIT)).items;
}

export async function fetchAgentEventsPage(
  agentId: string,
  limit: number,
  cursor?: string,
): Promise<PageResult<AgentEvent>> {
  const payload = await readJson<CursorPage<AgentEvent, "events">>(
    `/api/agents/${agentId}/events${pageQuery(limit, cursor)}`,
  );
  return { items: payload.events, nextCursor: payload.next_cursor };
}

export async function sendGroupMessage(groupId: string, text: string, senderName = "Human"): Promise<Message> {
  const payload = await sendJson<{ message: Message; ok: boolean }>(`/api/groups/${groupId}/messages`, {
    sender_name: senderName,
    text,
  });
  return payload.message;
}

export async function cancelMessage(messageId: string, recipientAgentId: string): Promise<{ state: string }> {
  const payload = await sendJson<{ delivery: { state: string }; ok: boolean }>(`/api/messages/${messageId}/cancel`, {
    recipient_agent_id: recipientAgentId,
  });
  return payload.delivery;
}

export async function startAgent(agentId: string): Promise<ControlResult> {
  const payload = await postWithoutBody<{ control: ControlResult; ok: boolean }>(`/api/agents/${agentId}/start`);
  return payload.control;
}

export async function stopAgent(agentId: string): Promise<ControlResult> {
  const payload = await postWithoutBody<{ control: ControlResult; ok: boolean }>(`/api/agents/${agentId}/stop`);
  return payload.control;
}

export async function reconnectAgent(agentId: string): Promise<ControlResult> {
  const payload = await postWithoutBody<{ control: ControlResult; ok: boolean }>(`/api/agents/${agentId}/reconnect`);
  return payload.control;
}

export async function fetchMetricsText(): Promise<string> {
  const response = await fetchFromApi("/metrics");
  return response.text();
}

export function metricsUrl(): string {
  return `${preferredApiBase ?? orderedApiBases()[0]}/metrics`;
}

export function openEventStream(): EventSource {
  return new EventSource(`${EVENT_STREAM_BASES[0]}/api/events/stream`);
}

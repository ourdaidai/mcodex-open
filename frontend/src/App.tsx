import { useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties, FormEvent } from "react";
import {
  DEFAULT_GROUP_MESSAGE_LIMIT,
  archiveGroup,
  cancelMessage,
  fetchAgentEvents,
  fetchAgentSessions,
  fetchGroupAgents,
  fetchGroupIssues,
  fetchGroupMessages,
  fetchGroups,
  fetchMetricsText,
  metricsUrl,
  openEventStream,
  reconnectAgent,
  restoreGroup,
  sendGroupMessage,
  startAgent,
  stopAgent,
} from "./api";
import {
  formatMessageCount,
  hasOlderMessages,
  reconcileSelectedGroupId,
  shouldLoadArchivedGroups,
} from "./groups";
import { PerformanceView } from "./PerformanceView";
import {
  DebouncedRefresh,
  realtimeRefreshPlan,
  scheduleFullRefresh,
  shouldRunRecoveryPoll,
  type SseState,
} from "./realtime";
import type { Agent, AgentEvent, AgentSession, ControlResult, Group, McodexIssue, Message } from "./types";

type AgentDensity = "cozy" | "compact";
type ViewMode = "workspace" | "status" | "performance";
type ControlAction = "start" | "stop" | "reconnect";
type EventFilter = "all" | "lifecycle" | "control" | "message";
type PendingControl = {
  action: ControlAction;
  requestId: string;
};
type IdleAlertPermission = NotificationPermission | "unsupported";

const IDLE_ALERT_ENABLED_STORAGE_KEY = "mcodex.idleAlert.enabled";
const REALTIME_EVENT_TYPES = [
  "group_updated",
  "agent_updated",
  "agent_control",
  "message_created",
  "message_delivery_updated",
  "issue_created",
  "issue_updated",
  "resync_required",
] as const;

function getIdleAlertPermission(): IdleAlertPermission {
  if (typeof window === "undefined" || !("Notification" in window)) {
    return "unsupported";
  }
  return window.Notification.permission;
}

function loadIdleAlertPreference(): boolean {
  if (typeof window === "undefined") {
    return false;
  }
  return window.localStorage.getItem(IDLE_ALERT_ENABLED_STORAGE_KEY) === "true";
}

function saveIdleAlertPreference(enabled: boolean) {
  window.localStorage.setItem(IDLE_ALERT_ENABLED_STORAGE_KEY, enabled ? "true" : "false");
}

function isAllIdleGroup(agents: Agent[] | undefined): agents is Agent[] {
  if (!agents || agents.length === 0) {
    return false;
  }
  return agents.every((agent) => agent.status === "idle");
}

function idleAlertPermissionLabel(permission: IdleAlertPermission): string {
  if (permission === "granted") {
    return "system notification + sound";
  }
  if (permission === "denied") {
    return "sound only; browser notification denied";
  }
  if (permission === "unsupported") {
    return "sound only; browser notification unsupported";
  }
  return "will request browser notification permission";
}

function formatTime(value: string | null): string {
  if (!value) {
    return "N/A";
  }
  try {
    return new Intl.DateTimeFormat("zh-CN", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    }).format(new Date(value));
  } catch {
    return value;
  }
}

function hashHue(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) % 360;
  }
  return hash;
}

function avatarStyle(name: string) {
  const hue = hashHue(name);
  return {
    background: `linear-gradient(145deg, hsl(${hue} 72% 72%), hsl(${(hue + 34) % 360} 66% 54%))`,
  };
}

function agentToneStyle(name: string): CSSProperties {
  const hue = hashHue(name);
  return {
    "--agent-hue": String(hue),
    "--agent-accent": `hsl(${hue} 66% 43%)`,
    "--agent-soft": `hsl(${hue} 72% 92%)`,
  } as CSSProperties;
}

function messageSenderLabel(message: Message): string {
  return message.sender_display_name ?? message.sender_agent_id;
}

function messageRecipientLabel(message: Message): string {
  return message.recipient_display_name ?? message.recipient_agent_id;
}

function issueReporterLabel(issue: McodexIssue): string {
  return issue.reporter_display_name ?? issue.reporter_agent_id ?? "unknown";
}

function issueTypeLabel(value: McodexIssue["issue_type"]): string {
  return value.replace(/_/g, " ");
}

function latestAgentActivity(agent: Agent): string | null {
  if (agent.status === "offline") {
    return agent.last_seen_at ?? agent.updated_at ?? agent.last_heartbeat_at;
  }
  return agent.last_heartbeat_at ?? agent.last_seen_at ?? agent.updated_at;
}

function agentStatusSince(agent: Agent): string | null {
  return agent.status_changed_at ?? latestAgentActivity(agent);
}

function agentActivityLabel(agent: Agent): string {
  const timestamp = formatTime(agentStatusSince(agent));
  switch (agent.status) {
    case "offline":
      return `Offline since ${timestamp}`;
    case "idle":
      return `Idle since ${timestamp}`;
    case "busy":
      return `Busy since ${timestamp}`;
    default:
      return `Online since ${timestamp}`;
  }
}

function agentTransportLabel(agent: Agent): string | null {
  return agent.transport === "api" ? "API" : null;
}

function agentCardClass(baseClass: string, active: boolean, status: Agent["status"]): string {
  return [baseClass, active ? "active" : "", `status-${status}`].filter(Boolean).join(" ");
}

function mentionRecipientQuery(text: string): string | null {
  if (!text.trim()) {
    return "";
  }
  const match = text.match(/^@([A-Za-z0-9._-]*)$/);
  return match ? match[1].toLowerCase() : null;
}

function requestIdFromPayload(payload: Record<string, unknown>): string | null {
  return typeof payload.request_id === "string" ? payload.request_id : null;
}

function shortRequestId(payload: Record<string, unknown>): string {
  const requestId = requestIdFromPayload(payload);
  return requestId ? ` [${requestId.slice(0, 8)}]` : "";
}

function eventSummary(event: AgentEvent): string {
  const peer = typeof event.payload.peer_agent_id === "string" ? event.payload.peer_agent_id : null;
  const body = typeof event.payload.body === "string" ? event.payload.body : null;
  const channel = typeof event.payload.channel === "string" ? event.payload.channel : null;

  switch (event.type) {
    case "session_registered":
      return `session attached to ${String(event.payload.tmux_session ?? "tmux")}${shortRequestId(event.payload)}`;
    case "heartbeat":
      return `status -> ${String(event.payload.status ?? "unknown")}${shortRequestId(event.payload)}`;
    case "api_agent_registered":
      return "API agent registered";
    case "api_status_updated":
      return `API status -> ${String(event.payload.status ?? "unknown")}`;
    case "session_disconnected":
      return `session disconnected${shortRequestId(event.payload)}`;
    case "direct_message_sent":
      return `sent to ${peer ?? "peer"}${body ? `: ${body}` : ""}`;
    case "direct_message_pending":
      return `pending from ${peer ?? "peer"}${body ? `: ${body}` : ""}`;
    case "delivery_claimed":
      return `delivery claimed${peer ? ` from ${peer}` : ""}${channel ? ` via ${channel}` : ""}`;
    case "delivery_acked":
      return `delivery acked${peer ? ` from ${peer}` : ""}`;
    case "delivery_canceled":
      return `delivery canceled${peer ? ` from ${peer}` : ""}`;
    case "start_requested":
      return `start requested in ${String(event.payload.cwd ?? ".")}${shortRequestId(event.payload)}`;
    case "stop_requested":
      return `stop requested for ${String(event.payload.tmux_session ?? "tmux session")}${shortRequestId(event.payload)}`;
    case "reconnect_requested":
      return `reconnect requested in ${String(event.payload.cwd ?? ".")}${shortRequestId(event.payload)}`;
    default:
      return event.type;
  }
}

function eventFilterLabel(filter: EventFilter): string {
  switch (filter) {
    case "lifecycle":
      return "Lifecycle";
    case "control":
      return "Control";
    case "message":
      return "Messages";
    default:
      return "All";
  }
}

function eventMatchesFilter(event: AgentEvent, filter: EventFilter): boolean {
  if (filter === "all") {
    return true;
  }
  if (filter === "lifecycle") {
    return [
      "session_registered",
      "session_disconnected",
      "heartbeat",
      "api_agent_registered",
      "api_status_updated",
    ].includes(event.type);
  }
  if (filter === "control") {
    return ["start_requested", "stop_requested", "reconnect_requested"].includes(event.type);
  }
  return [
    "direct_message_sent",
    "direct_message_pending",
    "delivery_claimed",
    "delivery_acked",
    "delivery_canceled",
  ].includes(event.type);
}

function latestRelatedMessage(agentId: string, messages: Message[]): Message | null {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message.sender_agent_id === agentId || message.recipient_agent_id === agentId) {
      return message;
    }
  }
  return null;
}

export function App() {
  const [view, setView] = useState<ViewMode>("workspace");
  const [groups, setGroups] = useState<Group[]>([]);
  const [archivedGroups, setArchivedGroups] = useState<Group[]>([]);
  const [archivedGroupsExpanded, setArchivedGroupsExpanded] = useState(false);
  const [archivedGroupsLoaded, setArchivedGroupsLoaded] = useState(false);
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [groupAgentsById, setGroupAgentsById] = useState<Record<string, Agent[]>>({});
  const [messages, setMessages] = useState<Message[]>([]);
  const [issues, setIssues] = useState<McodexIssue[]>([]);
  const [focusedAgentId, setFocusedAgentId] = useState<string | null>(null);
  const [statusAgentId, setStatusAgentId] = useState<string | null>(null);
  const [sessions, setSessions] = useState<AgentSession[]>([]);
  const [events, setEvents] = useState<AgentEvent[]>([]);
  const [composerText, setComposerText] = useState("");
  const [composerFocused, setComposerFocused] = useState(false);
  const [isSending, setIsSending] = useState(false);
  const [cancelingMessageId, setCancelingMessageId] = useState<string | null>(null);
  const [isControlling, setIsControlling] = useState(false);
  const [pendingControl, setPendingControl] = useState<PendingControl | null>(null);
  const [controlNotice, setControlNotice] = useState<string | null>(null);
  const [eventFilter, setEventFilter] = useState<EventFilter>("all");
  const [density, setDensity] = useState<AgentDensity>("cozy");
  const [idleAlertsEnabled, setIdleAlertsEnabled] = useState(loadIdleAlertPreference);
  const [idleAlertPermission, setIdleAlertPermission] = useState<IdleAlertPermission>(getIdleAlertPermission);
  const [idleAlertNotice, setIdleAlertNotice] = useState<string | null>(null);
  const [groupsVersion, setGroupsVersion] = useState(0);
  const [archivedGroupsVersion, setArchivedGroupsVersion] = useState(0);
  const [groupDataVersion, setGroupDataVersion] = useState(0);
  const [agentDetailsVersion, setAgentDetailsVersion] = useState(0);
  const [sseState, setSseState] = useState<SseState>("connecting");
  const [loading, setLoading] = useState(true);
  const [pendingGroupAction, setPendingGroupAction] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const selectedGroupIdRef = useRef<string | null>(selectedGroupId);
  const archivedGroupsLoadedRef = useRef(archivedGroupsLoaded);
  const preferredSelectedGroupIdRef = useRef<string | null>(null);
  const statusAgentIdRef = useRef<string | null>(statusAgentId);
  const composerInputRef = useRef<HTMLTextAreaElement | null>(null);
  const feedScrollRef = useRef<HTMLElement | null>(null);
  const idleAlertStatesRef = useRef<Record<string, boolean>>({});
  const idleAlertAudioRef = useRef<AudioContext | null>(null);
  const realtimeRefreshRef = useRef<DebouncedRefresh | null>(null);
  const realtimeRefresh = realtimeRefreshRef.current ?? new DebouncedRefresh(100);
  realtimeRefreshRef.current = realtimeRefresh;
  selectedGroupIdRef.current = selectedGroupId;
  archivedGroupsLoadedRef.current = archivedGroupsLoaded;
  statusAgentIdRef.current = statusAgentId;

  useEffect(() => {
    const stream = openEventStream();
    const refresh = realtimeRefresh;

    const refreshGroups = () => setGroupsVersion((current) => current + 1);
    const refreshArchivedGroups = () => {
      if (archivedGroupsLoadedRef.current) {
        setArchivedGroupsVersion((current) => current + 1);
      }
    };
    const refreshGroupData = () => setGroupDataVersion((current) => current + 1);
    const refreshAgentDetails = () => setAgentDetailsVersion((current) => current + 1);

    const onRealtimeEvent = (event: Event) => {
      let payload: { data?: unknown; type?: unknown };
      try {
        payload = JSON.parse((event as MessageEvent<string>).data) as { data?: unknown; type?: unknown };
      } catch {
        return;
      }
      if (
        typeof payload.type !== "string" ||
        payload.data === null ||
        typeof payload.data !== "object" ||
        Array.isArray(payload.data)
      ) {
        return;
      }
      const plan = realtimeRefreshPlan(
        payload.type,
        payload.data as Record<string, unknown>,
        selectedGroupIdRef.current,
        statusAgentIdRef.current,
      );

      if (plan.full) {
        scheduleFullRefresh(refresh, {
          groups: refreshGroups,
          ...(selectedGroupIdRef.current ? { groupData: refreshGroupData } : {}),
          ...(statusAgentIdRef.current ? { agentDetails: refreshAgentDetails } : {}),
        });
        if (plan.archivedGroups) {
          refresh.schedule("archived-groups", refreshArchivedGroups);
        }
        return;
      }
      if (plan.groups) {
        refresh.schedule("groups", refreshGroups);
      }
      if (plan.archivedGroups) {
        refresh.schedule("archived-groups", refreshArchivedGroups);
      }
      if (plan.groupData && selectedGroupIdRef.current) {
        refresh.schedule("group-data", refreshGroupData);
      }
      if (plan.agentDetails && statusAgentIdRef.current) {
        refresh.schedule("agent-details", refreshAgentDetails);
      }
    };

    stream.onopen = () => {
      setSseState("connected");
      scheduleFullRefresh(refresh, {
        groups: refreshGroups,
        ...(selectedGroupIdRef.current ? { groupData: refreshGroupData } : {}),
        ...(statusAgentIdRef.current ? { agentDetails: refreshAgentDetails } : {}),
      });
      refresh.schedule("archived-groups", refreshArchivedGroups);
    };
    stream.onerror = () => setSseState("disconnected");
    for (const eventType of REALTIME_EVENT_TYPES) {
      stream.addEventListener(eventType, onRealtimeEvent);
    }

    return () => {
      stream.onopen = null;
      stream.onerror = null;
      for (const eventType of REALTIME_EVENT_TYPES) {
        stream.removeEventListener(eventType, onRealtimeEvent);
      }
      refresh.close();
      stream.close();
    };
  }, []);

  useEffect(() => {
    if (!shouldRunRecoveryPoll(sseState)) {
      return;
    }
    const interval = window.setInterval(() => {
      setGroupsVersion((current) => current + 1);
      if (archivedGroupsLoadedRef.current) {
        setArchivedGroupsVersion((current) => current + 1);
      }
      if (selectedGroupIdRef.current) {
        setGroupDataVersion((current) => current + 1);
      }
      if (statusAgentIdRef.current) {
        setAgentDetailsVersion((current) => current + 1);
      }
    }, 5000);

    return () => window.clearInterval(interval);
  }, [sseState]);

  useEffect(() => {
    let cancelled = false;

    async function loadGroups() {
      try {
        const nextGroups = await fetchGroups();
        if (cancelled) {
          return;
        }
        setGroups(nextGroups);
        setSelectedGroupId((current) => {
          const preferred = preferredSelectedGroupIdRef.current;
          const nextSelected = reconcileSelectedGroupId(current, nextGroups, preferred);
          if (preferred && nextSelected === preferred) {
            preferredSelectedGroupIdRef.current = null;
          }
          return nextSelected;
        });
        setError(null);
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : String(loadError));
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void loadGroups();
    return () => {
      cancelled = true;
    };
  }, [groupsVersion]);

  useEffect(() => {
    if (!shouldLoadArchivedGroups(archivedGroupsExpanded, archivedGroupsLoaded)) {
      return;
    }
    let cancelled = false;
    async function loadArchivedGroups() {
      try {
        const nextGroups = await fetchGroups("archived");
        if (!cancelled) {
          setArchivedGroups(nextGroups);
          setArchivedGroupsLoaded(true);
          setError(null);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : String(loadError));
        }
      }
    }
    void loadArchivedGroups();
    return () => {
      cancelled = true;
    };
  }, [archivedGroupsExpanded, archivedGroupsLoaded]);

  useEffect(() => {
    if (!archivedGroupsLoadedRef.current || archivedGroupsVersion === 0) {
      return;
    }
    let cancelled = false;
    async function refreshArchivedGroups() {
      try {
        const nextGroups = await fetchGroups("archived");
        if (!cancelled) {
          setArchivedGroups(nextGroups);
          setError(null);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : String(loadError));
        }
      }
    }
    void refreshArchivedGroups();
    return () => {
      cancelled = true;
    };
  }, [archivedGroupsVersion]);

  const groupIdsKey = useMemo(() => groups.map((group) => group.group_id).join("\0"), [groups]);

  useEffect(() => {
    if (!idleAlertsEnabled || groups.length === 0) {
      setGroupAgentsById({});
      return;
    }

    let cancelled = false;
    async function loadGroupAgentSnapshots() {
      try {
        const entries = await Promise.all(
          groups.map(async (group) => [group.group_id, await fetchGroupAgents(group.group_id)] as const),
        );
        if (!cancelled) {
          setGroupAgentsById((current) => ({ ...current, ...Object.fromEntries(entries) }));
        }
      } catch {
        return;
      }
    }

    void loadGroupAgentSnapshots();
    return () => {
      cancelled = true;
    };
  }, [groupIdsKey, groups, groupsVersion, idleAlertsEnabled]);

  useEffect(() => {
    if (!selectedGroupId) {
      setAgents([]);
      setMessages([]);
      setIssues([]);
      setFocusedAgentId(null);
      setStatusAgentId(null);
      return;
    }

    const groupId = selectedGroupId;
    let cancelled = false;
    async function loadGroupData() {
      try {
        const [nextAgents, nextMessages, nextIssues] = await Promise.all([
          fetchGroupAgents(groupId),
          fetchGroupMessages(groupId),
          fetchGroupIssues(groupId),
        ]);
        if (cancelled) {
          return;
        }
        setAgents(nextAgents);
        setMessages(nextMessages);
        setIssues(nextIssues);
        setGroupAgentsById((current) => ({ ...current, [groupId]: nextAgents }));
        setFocusedAgentId((current) => {
          if (current && nextAgents.some((agent) => agent.agent_id === current)) {
            return current;
          }
          return null;
        });
        setStatusAgentId((current) => {
          if (current && nextAgents.some((agent) => agent.agent_id === current)) {
            return current;
          }
          return nextAgents[0]?.agent_id ?? null;
        });
        setError(null);
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : String(loadError));
        }
      }
    }

    void loadGroupData();
    return () => {
      cancelled = true;
    };
  }, [groupDataVersion, selectedGroupId]);

  useEffect(() => {
    if (!statusAgentId) {
      setSessions([]);
      setEvents([]);
      return;
    }

    const agentId = statusAgentId;
    let cancelled = false;
    async function loadAgentDetails() {
      try {
        const [nextSessions, nextEvents] = await Promise.all([fetchAgentSessions(agentId), fetchAgentEvents(agentId)]);
        if (cancelled) {
          return;
        }
        setSessions(nextSessions);
        setEvents(nextEvents);
        setError(null);
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : String(loadError));
        }
      }
    }

    void loadAgentDetails();
    return () => {
      cancelled = true;
    };
  }, [agentDetailsVersion, statusAgentId]);

  const selectedGroup = useMemo(
    () => groups.find((group) => group.group_id === selectedGroupId) ?? null,
    [groups, selectedGroupId],
  );

  const focusedAgent = useMemo(
    () => agents.find((agent) => agent.agent_id === focusedAgentId) ?? null,
    [agents, focusedAgentId],
  );

  const statusAgent = useMemo(
    () => agents.find((agent) => agent.agent_id === statusAgentId) ?? null,
    [agents, statusAgentId],
  );
  const statusAgentTransportLabel = statusAgent ? agentTransportLabel(statusAgent) : null;
  const statusAgentIsApi = statusAgent?.transport === "api";

  const filteredMessages = useMemo(() => {
    if (!focusedAgentId) {
      return messages;
    }
    return messages.filter(
      (message) => message.sender_agent_id === focusedAgentId || message.recipient_agent_id === focusedAgentId,
    );
  }, [focusedAgentId, messages]);

  const latestFilteredMessageId =
    filteredMessages.length > 0 ? filteredMessages[filteredMessages.length - 1].message_id : null;
  const isRecentMessageWindow =
    selectedGroup !== null && hasOlderMessages(selectedGroup, Math.max(messages.length, DEFAULT_GROUP_MESSAGE_LIMIT));
  const messageWindowLabel =
    selectedGroup && hasOlderMessages(selectedGroup, messages.length)
      ? `Recent ${messages.length} of ${formatMessageCount(selectedGroup)} loaded`
      : `${messages.length} loaded`;
  const emptyMessageText =
    focusedAgent && isRecentMessageWindow
      ? `No recent messages with ${focusedAgent.display_name}. Older messages may exist.`
      : focusedAgent
        ? "No messages for this agent yet."
        : "No messages yet.";

  useEffect(() => {
    const feed = feedScrollRef.current;
    if (!feed) {
      return;
    }
    window.requestAnimationFrame(() => {
      feed.scrollTo({ top: feed.scrollHeight, behavior: "smooth" });
    });
  }, [focusedAgentId, latestFilteredMessageId, selectedGroupId]);

  const hasRunningSession = useMemo(
    () => sessions.some((session) => session.status === "running"),
    [sessions],
  );

  const runningSessionCount = useMemo(
    () => sessions.filter((session) => session.status === "running").length,
    [sessions],
  );

  const filteredEvents = useMemo(
    () => events.filter((event) => eventMatchesFilter(event, eventFilter)),
    [eventFilter, events],
  );

  const lastAgentMessage = useMemo(
    () => (statusAgentId ? latestRelatedMessage(statusAgentId, messages) : null),
    [messages, statusAgentId],
  );
  const openIssueCount = useMemo(
    () => issues.filter((issue) => issue.status === "open").length,
    [issues],
  );

  const recipientQuery = mentionRecipientQuery(composerText);
  const recipientSuggestions = useMemo(() => {
    if (recipientQuery === null) {
      return [];
    }
    return agents
      .filter((agent) => {
        const agentId = agent.agent_id.toLowerCase();
        const displayName = agent.display_name.toLowerCase();
        return agentId.startsWith(recipientQuery) || displayName.startsWith(recipientQuery);
      })
      .slice(0, 8);
  }, [agents, recipientQuery]);

  const showRecipientSuggestions =
    Boolean(selectedGroupId) && composerFocused && recipientQuery !== null && recipientSuggestions.length > 0;

  const idleAlertStatusText =
    idleAlertNotice ??
    (idleAlertsEnabled
      ? `Idle alert on: ${idleAlertPermissionLabel(idleAlertPermission)}`
      : "Notify when every agent in a group is idle");

  async function playIdleAlertTone() {
    if (!("AudioContext" in window)) {
      return;
    }
    const context = idleAlertAudioRef.current ?? new AudioContext();
    idleAlertAudioRef.current = context;
    if (context.state === "suspended") {
      await context.resume();
    }

    const oscillator = context.createOscillator();
    const gain = context.createGain();
    oscillator.type = "sine";
    oscillator.frequency.setValueAtTime(880, context.currentTime);
    oscillator.frequency.exponentialRampToValueAtTime(660, context.currentTime + 0.18);
    gain.gain.setValueAtTime(0.0001, context.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.14, context.currentTime + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.0001, context.currentTime + 0.24);
    oscillator.connect(gain);
    gain.connect(context.destination);
    oscillator.start();
    oscillator.stop(context.currentTime + 0.26);
  }

  async function handleToggleIdleAlerts() {
    const nextEnabled = !idleAlertsEnabled;
    if (!nextEnabled) {
      saveIdleAlertPreference(false);
      setIdleAlertsEnabled(false);
      setIdleAlertNotice(null);
      return;
    }

    let permission = getIdleAlertPermission();
    if (permission === "default") {
      permission = await window.Notification.requestPermission();
    }
    setIdleAlertPermission(permission);
    saveIdleAlertPreference(true);
    setIdleAlertsEnabled(true);
    setIdleAlertNotice(`Idle alert on: ${idleAlertPermissionLabel(permission)}`);
    try {
      await playIdleAlertTone();
    } catch {
      setIdleAlertNotice(`Idle alert on: ${idleAlertPermissionLabel(permission)}; sound blocked`);
    }
  }

  useEffect(() => {
    for (const group of groups) {
      const groupAgents = groupAgentsById[group.group_id];
      if (!groupAgents) {
        continue;
      }

      const allIdle = isAllIdleGroup(groupAgents);
      const wasAllIdle = idleAlertStatesRef.current[group.group_id];
      idleAlertStatesRef.current[group.group_id] = allIdle;
      if (wasAllIdle === undefined || !idleAlertsEnabled || !allIdle || wasAllIdle) {
        continue;
      }

      const agentNames = groupAgents.map((agent) => agent.display_name).join(", ");
      const title = `mcodex: ${group.name} all idle`;
      const body = `${groupAgents.length} agents are idle: ${agentNames}`;
      const permission = getIdleAlertPermission();
      setIdleAlertPermission(permission);
      setIdleAlertNotice(`${group.name}: all agents idle`);
      if (permission === "granted") {
        const notification = new Notification(title, {
          body,
          tag: `mcodex-all-idle-${group.group_id}`,
        });
        notification.onclick = () => window.focus();
      }
      void playIdleAlertTone().catch(() => undefined);
    }
  }, [groupAgentsById, groups, idleAlertsEnabled]);

  useEffect(() => {
    setPendingControl(null);
    setControlNotice(null);
    setEventFilter("all");
  }, [statusAgentId]);

  useEffect(() => {
    if (!pendingControl || !statusAgent) {
      return;
    }

    const matchingEvents = events.filter((event) => requestIdFromPayload(event.payload) === pendingControl.requestId);

    if (
      pendingControl.action === "start" &&
      statusAgent.status !== "offline" &&
      matchingEvents.some((event) => event.type === "session_registered" || event.type === "heartbeat")
    ) {
      setPendingControl(null);
      setControlNotice("start confirmed");
      return;
    }

    if (
      pendingControl.action === "stop" &&
      statusAgent.status === "offline" &&
      matchingEvents.some((event) => event.type === "session_disconnected")
    ) {
      setPendingControl(null);
      setControlNotice("stop confirmed");
      return;
    }

    if (
      pendingControl.action === "reconnect" &&
      statusAgent.status !== "offline" &&
      matchingEvents.some((event) => event.type === "session_registered" || event.type === "heartbeat")
    ) {
      setPendingControl(null);
      setControlNotice("reconnect confirmed");
    }
  }, [events, pendingControl, statusAgent]);

  async function handleSendMessage(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedGroupId || !composerText.trim()) {
      return;
    }

    setIsSending(true);
    try {
      await sendGroupMessage(selectedGroupId, composerText.trim(), "Human");
      const nextMessages = await fetchGroupMessages(selectedGroupId);
      setMessages(nextMessages);
      setComposerText("");
      setError(null);
    } catch (sendError) {
      setError(sendError instanceof Error ? sendError.message : String(sendError));
    } finally {
      setIsSending(false);
    }
  }

  async function handleCancelMessage(message: Message) {
    if (!selectedGroupId || message.delivery_state !== "pending") {
      return;
    }

    setCancelingMessageId(message.message_id);
    try {
      await cancelMessage(message.message_id, message.recipient_agent_id);
      const nextMessages = await fetchGroupMessages(selectedGroupId);
      setMessages(nextMessages);
      setError(null);
    } catch (cancelError) {
      setError(cancelError instanceof Error ? cancelError.message : String(cancelError));
    } finally {
      setCancelingMessageId(null);
    }
  }

  async function handleControl(action: ControlAction) {
    if (!statusAgentId || statusAgentIsApi) {
      return;
    }

    setIsControlling(true);
    setControlNotice(`${action} requested`);
    try {
      let control: ControlResult;
      if (action === "start") {
        control = await startAgent(statusAgentId);
      } else if (action === "stop") {
        control = await stopAgent(statusAgentId);
      } else {
        control = await reconnectAgent(statusAgentId);
      }
      setPendingControl({ action, requestId: control.request_id });
      setError(null);
    } catch (controlError) {
      setPendingControl(null);
      setError(controlError instanceof Error ? controlError.message : String(controlError));
    } finally {
      setIsControlling(false);
    }
  }

  async function handleArchiveGroup(group: Group) {
    if (group.online_count > 0) {
      setError(`Stop all agents in ${group.name} before archiving it.`);
      return;
    }
    if (!window.confirm(`Archive ${group.name}? Its history will remain available for restore.`)) {
      return;
    }
    setPendingGroupAction(`archive:${group.group_id}`);
    try {
      await archiveGroup(group.group_id);
      setGroupsVersion((current) => current + 1);
      if (archivedGroupsLoadedRef.current) {
        setArchivedGroupsVersion((current) => current + 1);
      }
      setError(null);
    } catch (archiveError) {
      setError(archiveError instanceof Error ? archiveError.message : String(archiveError));
    } finally {
      setPendingGroupAction(null);
    }
  }

  async function handleRestoreGroup(group: Group) {
    setPendingGroupAction(`restore:${group.group_id}`);
    try {
      await restoreGroup(group.group_id);
      preferredSelectedGroupIdRef.current = group.group_id;
      setGroupsVersion((current) => current + 1);
      setArchivedGroupsVersion((current) => current + 1);
      setError(null);
    } catch (restoreError) {
      setError(restoreError instanceof Error ? restoreError.message : String(restoreError));
    } finally {
      setPendingGroupAction(null);
    }
  }

  function insertRecipientMention(agentId: string) {
    setComposerText(`@${agentId} `);
    window.requestAnimationFrame(() => {
      composerInputRef.current?.focus();
    });
  }

  function renderGroupsSidebar() {
    return (
      <aside className="sidebar sidebar-groups">
        <div className="sidebar-header sidebar-header-stacked">
          <div>
            <p className="kicker">Navigation</p>
            <h1>mcodex</h1>
          </div>
          <nav className="view-switcher" aria-label="View">
            <button
              aria-current={view === "workspace" ? "page" : undefined}
              className={view === "workspace" ? "view-button active" : "view-button"}
              onClick={() => setView("workspace")}
              type="button"
            >
              Workspace
            </button>
            <button
              aria-current={view === "status" ? "page" : undefined}
              className={view === "status" ? "view-button active" : "view-button"}
              onClick={() => setView("status")}
              type="button"
            >
              Status
            </button>
            <button
              aria-current={view === "performance" ? "page" : undefined}
              className={view === "performance" ? "view-button active" : "view-button"}
              onClick={() => setView("performance")}
              type="button"
            >
              Performance
            </button>
          </nav>
          <div className="idle-alert-control">
            <button
              className={idleAlertsEnabled ? "idle-alert-button active" : "idle-alert-button"}
              onClick={() => void handleToggleIdleAlerts()}
              type="button"
            >
              {idleAlertsEnabled ? "Idle alert on" : "Idle alert off"}
            </button>
            <span className="idle-alert-note">{idleAlertStatusText}</span>
          </div>
        </div>

        <div className="sidebar-header">
          <div>
            <p className="kicker">Groups</p>
            <h2>{view === "workspace" ? "Message Workspace" : view === "status" ? "Runtime Status" : "Local Performance"}</h2>
          </div>
          {view !== "performance" ? <span className="badge">{loading ? "Loading" : `${groups.length}`}</span> : null}
        </div>

        <div className="group-list">
          {groups.map((group) => (
            <div className="group-card-row" key={group.group_id}>
              <button
                className={group.group_id === selectedGroupId ? "group-card active" : "group-card"}
                onClick={() => setSelectedGroupId(group.group_id)}
                type="button"
              >
                <div className="group-card-title">
                  <strong>{group.name}</strong>
                  <span className="badge subtle">{group.group_id}</span>
                </div>
                <div className="group-card-meta">
                  <span>{group.online_count}/{group.agent_count} online</span>
                  <span>{formatMessageCount(group)} messages</span>
                </div>
              </button>
              <button
                aria-label={`Archive ${group.name}`}
                className="group-card-action"
                disabled={group.online_count > 0 || pendingGroupAction !== null}
                onClick={() => void handleArchiveGroup(group)}
                title={group.online_count > 0 ? "Stop all agents before archiving" : `Archive ${group.name}`}
                type="button"
              >
                {pendingGroupAction === `archive:${group.group_id}` ? "Archiving" : "Archive"}
              </button>
            </div>
          ))}
          {groups.length === 0 ? <div className="empty-panel">No groups yet.</div> : null}
          <div className="archived-groups-section">
            <button
              aria-expanded={archivedGroupsExpanded}
              className="archived-groups-toggle"
              onClick={() => setArchivedGroupsExpanded((current) => !current)}
              type="button"
            >
              <span>Archived groups</span>
              <span>{archivedGroupsExpanded ? "−" : "+"}</span>
            </button>
            {archivedGroupsExpanded ? (
              <div className="archived-group-list">
                {!archivedGroupsLoaded ? <div className="empty-panel">Loading archived groups…</div> : null}
                {archivedGroupsLoaded && archivedGroups.length === 0 ? (
                  <div className="empty-panel">No archived groups.</div>
                ) : null}
                {archivedGroups.map((group) => (
                  <div className="archived-group-row" key={group.group_id}>
                    <div>
                      <strong>{group.name}</strong>
                      <div className="group-card-meta">
                        <span>{group.group_id}</span>
                        <span>{formatMessageCount(group)} messages</span>
                      </div>
                    </div>
                    <button
                      className="group-card-action restore"
                      disabled={pendingGroupAction !== null}
                      onClick={() => void handleRestoreGroup(group)}
                      type="button"
                    >
                      {pendingGroupAction === `restore:${group.group_id}` ? "Restoring" : "Restore"}
                    </button>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </div>
      </aside>
    );
  }

  function renderWorkspace() {
    return (
      <>
        <main className="feed-column">
          <header className="feed-header">
            <div>
              <p className="kicker">Group Feed</p>
              <h2>{selectedGroup?.name ?? "Select a group"}</h2>
              <p className="header-note">
                {focusedAgent
                  ? `Showing messages with ${focusedAgent.display_name}`
                  : "Unified message flow for this workspace"}
              </p>
            </div>
            <div className="feed-header-meta">
              <span className="badge">{filteredMessages.length} shown</span>
              <span className="badge subtle">{messageWindowLabel}</span>
              {focusedAgent ? (
                <button className="ghost-button" onClick={() => setFocusedAgentId(null)} type="button">
                  Clear filter
                </button>
              ) : null}
            </div>
          </header>

          <section className="feed-scroll" ref={feedScrollRef}>
            {filteredMessages.map((message) => {
              const senderLabel = messageSenderLabel(message);
              const recipientLabel = messageRecipientLabel(message);
              const toneStyle = agentToneStyle(message.sender_agent_id);
              if (message.message_type === "pane_summary") {
                return (
                  <article className="message-row pane-summary-row" key={message.message_id} style={toneStyle}>
                    <div aria-hidden className="message-avatar" style={avatarStyle(senderLabel)} />
                    <div className="message-content pane-summary-content">
                      <div className="message-heading">
                        <strong>{senderLabel}</strong>
                        <span className="message-kind summary-kind">IDLE summary</span>
                        <time>{formatTime(message.created_at)}</time>
                      </div>
                      <div className="message-recipient">@{message.sender_agent_id} pane summary</div>
                      <pre className="pane-summary-text">{message.body}</pre>
                    </div>
                  </article>
                );
              }
              return (
                <article className="message-row direct-message-row" key={message.message_id} style={toneStyle}>
                  <div aria-hidden className="message-avatar" style={avatarStyle(senderLabel)} />
                  <div className="message-content">
                    <div className="message-heading">
                      <strong>{senderLabel}</strong>
                      <span className="message-kind direct-kind">agent message</span>
                      <time>{formatTime(message.created_at)}</time>
                    </div>
                    <div className="message-recipient">@{recipientLabel}</div>
                    <p className="message-text">{message.body}</p>
                    {message.delivery_state ? (
                      <div className="message-actions">
                        <span className={`delivery-pill state-${message.delivery_state}`}>{message.delivery_state}</span>
                        {message.delivery_state === "pending" ? (
                          <button
                            className="cancel-message-button"
                            disabled={cancelingMessageId === message.message_id}
                            onClick={() => void handleCancelMessage(message)}
                            type="button"
                          >
                            {cancelingMessageId === message.message_id ? "Canceling..." : "Cancel queued"}
                          </button>
                        ) : null}
                      </div>
                    ) : null}
                  </div>
                </article>
              );
            })}
            {filteredMessages.length === 0 ? (
              <div className="empty-panel">{emptyMessageText}</div>
            ) : null}
          </section>

          <form className="composer composer-docked" onSubmit={handleSendMessage}>
            <div className="composer-topline">
              <span className="badge subtle">Send as Human</span>
              <span className="composer-hint">Type @ to choose a recipient</span>
            </div>
            {showRecipientSuggestions ? (
              <div className="mention-suggestions" role="listbox" aria-label="Recipient suggestions">
                {recipientSuggestions.map((agent) => (
                  <button
                    className={`mention-suggestion status-${agent.status}`}
                    key={agent.agent_id}
                    onMouseDown={(event) => {
                      event.preventDefault();
                      insertRecipientMention(agent.agent_id);
                    }}
                    type="button"
                  >
                    <span>@{agent.agent_id}</span>
                    <small>{agent.status}</small>
                  </button>
                ))}
              </div>
            ) : null}
            <textarea
              className="composer-input"
              disabled={!selectedGroupId || isSending}
              onBlur={() => setComposerFocused(false)}
              onChange={(event) => setComposerText(event.target.value)}
              onFocus={() => setComposerFocused(true)}
              placeholder="@mail summarize the last failure"
              ref={composerInputRef}
              rows={3}
              value={composerText}
            />
            <div className="composer-actions">
              <span className="composer-hint">
                {selectedGroupId ? `group ${selectedGroupId}` : "select a group first"}
              </span>
              <button
                className="primary-button"
                disabled={!selectedGroupId || !composerText.trim() || isSending}
                type="submit"
              >
                {isSending ? "Sending..." : "Send"}
              </button>
            </div>
          </form>

          {error ? <div className="error-banner">API error: {error}</div> : null}
        </main>

        <aside className="sidebar sidebar-agents">
          <div className="sidebar-header">
            <div>
              <p className="kicker">Agents</p>
              <h2>{selectedGroup?.name ?? "No group"}</h2>
            </div>
            <div className="density-toggle" role="tablist" aria-label="Agent density">
              <button
                className={density === "cozy" ? "density-button active" : "density-button"}
                onClick={() => setDensity("cozy")}
                type="button"
              >
                Cozy
              </button>
              <button
                className={density === "compact" ? "density-button active" : "density-button"}
                onClick={() => setDensity("compact")}
                type="button"
              >
                Compact
              </button>
            </div>
          </div>

          <div className={density === "cozy" ? "agent-list cozy" : "agent-list compact"}>
            {agents.map((agent) => {
              const active = focusedAgentId === agent.agent_id;
              const showAgentId = agent.display_name !== agent.agent_id;
              const transportLabel = agentTransportLabel(agent);
              return (
                <button
                  className={agentCardClass("agent-card", active, agent.status)}
                  key={agent.agent_id}
                  onClick={() => setFocusedAgentId((current) => (current === agent.agent_id ? null : agent.agent_id))}
                  type="button"
                >
                  <div aria-hidden className="agent-avatar" style={avatarStyle(agent.display_name)} />
                  <div className="agent-card-body">
                    <div className="agent-card-heading">
                      <strong>{agent.display_name}</strong>
                      <span className="agent-card-badges">
                        {transportLabel ? <span className="transport-pill">{transportLabel}</span> : null}
                        <span className={`status-pill ${agent.status}`}>{agent.status}</span>
                      </span>
                    </div>
                    {showAgentId ? <div className="agent-card-id">{agent.agent_id}</div> : null}
                    <div className="agent-card-meta">{agentActivityLabel(agent)}</div>
                  </div>
                </button>
              );
            })}
            {agents.length === 0 ? <div className="empty-panel">No agents in this group.</div> : null}
          </div>
        </aside>
      </>
    );
  }

  function renderStatus() {
    return (
      <main className="status-shell">
        <header className="feed-header">
          <div>
            <p className="kicker">Status</p>
            <h2>{selectedGroup?.name ?? "Select a group"}</h2>
            <p className="header-note">Sessions, events, and lifecycle controls live here instead of the workspace.</p>
          </div>
          <div className="status-header-badges">
            <span className="badge">{agents.length} agents</span>
            {openIssueCount > 0 ? <span className="badge issue-badge">{openIssueCount} open issues</span> : null}
          </div>
        </header>

        <div className="status-body">
          <section className="status-panel status-panel-agents">
            <div className="status-panel-header">
              <div>
                <p className="kicker">Agents</p>
                <h3>{selectedGroup?.name ?? "No group"}</h3>
              </div>
            </div>
            <div className="status-agent-list">
              {agents.map((agent) => {
                const active = statusAgentId === agent.agent_id;
                const showAgentId = agent.display_name !== agent.agent_id;
                const transportLabel = agentTransportLabel(agent);
                return (
                  <button
                    className={agentCardClass("status-agent-card", active, agent.status)}
                    key={agent.agent_id}
                    onClick={() => setStatusAgentId(agent.agent_id)}
                    type="button"
                  >
                    <div aria-hidden className="agent-avatar" style={avatarStyle(agent.display_name)} />
                    <div className="agent-card-body">
                      <div className="agent-card-heading">
                        <strong>{agent.display_name}</strong>
                        <span className="agent-card-badges">
                          {transportLabel ? <span className="transport-pill">{transportLabel}</span> : null}
                          <span className={`status-pill ${agent.status}`}>{agent.status}</span>
                        </span>
                      </div>
                      {showAgentId ? <div className="agent-card-id">{agent.agent_id}</div> : null}
                      <div className="agent-card-meta">{agentActivityLabel(agent)}</div>
                    </div>
                  </button>
                );
              })}
              {agents.length === 0 ? <div className="empty-panel">No agents in this group.</div> : null}
            </div>
          </section>

          <section className="status-panel status-panel-details">
            <div className="status-panel-header">
              <div>
                <p className="kicker">Agent Status</p>
                <h3>{statusAgent?.display_name ?? "Select an agent"}</h3>
              </div>
              <div className="status-header-badges">
                {statusAgentTransportLabel ? <span className="transport-pill">{statusAgentTransportLabel}</span> : null}
                <span className="badge subtle">{statusAgent?.status ?? "n/a"}</span>
              </div>
            </div>

            <div className="control-toolbar">
              <button
                className="ghost-button"
                disabled={!statusAgentId || isControlling || statusAgentIsApi || statusAgent?.status !== "offline"}
                onClick={() => void handleControl("start")}
                type="button"
              >
                Start
              </button>
              <button
                className="ghost-button"
                disabled={!statusAgentId || isControlling || statusAgentIsApi || !hasRunningSession}
                onClick={() => void handleControl("stop")}
                type="button"
              >
                Stop
              </button>
              <button
                className="ghost-button"
                disabled={!statusAgentId || isControlling || statusAgentIsApi}
                onClick={() => void handleControl("reconnect")}
                type="button"
              >
                Reconnect
              </button>
            </div>

            <div className={pendingControl ? "status-notice pending" : "status-notice"}>
              {pendingControl
                ? `${pendingControl.action} requested, waiting for agent confirmation`
                : statusAgentIsApi
                  ? "API agent: tmux lifecycle controls unavailable"
                : controlNotice ?? `agent status: ${statusAgent?.status ?? "n/a"}`}
            </div>

            <div className="status-summary-grid">
              <div className="summary-card">
                <span className="summary-label">Current State</span>
                <strong className="summary-value">{statusAgent?.status ?? "n/a"}</strong>
              </div>
              <div className="summary-card">
                <span className="summary-label">Last Seen</span>
                <strong className="summary-value">
                  {statusAgent ? formatTime(latestAgentActivity(statusAgent)) : "N/A"}
                </strong>
              </div>
              <div className="summary-card">
                <span className="summary-label">Running Sessions</span>
                <strong className="summary-value">{statusAgentIsApi ? "N/A" : runningSessionCount}</strong>
              </div>
              <div className="summary-card summary-card-wide">
                <span className="summary-label">Latest Message</span>
                <strong className="summary-value summary-message">
                  {lastAgentMessage
                    ? `${messageSenderLabel(lastAgentMessage)} -> ${messageRecipientLabel(lastAgentMessage)}`
                    : "No messages yet"}
                </strong>
                {lastAgentMessage ? (
                  <span className="summary-footnote">{formatTime(lastAgentMessage.created_at)}</span>
                ) : null}
              </div>
            </div>

            <section className="status-subpanel issue-subpanel">
              <div className="status-subpanel-header">
                <div>
                  <p className="kicker">mcodex Issues</p>
                  <h4>Mechanism feedback</h4>
                </div>
                <span className="badge">{openIssueCount} open / {issues.length} total</span>
              </div>
              <div className="issue-list">
                {issues.map((issue) => {
                  const reporter = issueReporterLabel(issue);
                  const handler = issue.handled_by_display_name ?? issue.handled_by_agent_id;
                  return (
                    <article className={`issue-card issue-card-${issue.status}`} key={issue.issue_id} style={agentToneStyle(reporter)}>
                      <div className="issue-card-header">
                        <div className="issue-pill-row">
                          <span className="issue-pill">{issueTypeLabel(issue.issue_type)}</span>
                          <span className={`issue-status-pill issue-status-${issue.status}`}>
                            {issue.status}
                          </span>
                        </div>
                        <time>{formatTime(issue.created_at)}</time>
                      </div>
                      <strong className="issue-title">{issue.title}</strong>
                      <div className="issue-meta">
                        <span>from {reporter}</span>
                        <span>{issue.source}</span>
                        {issue.status === "handled" ? (
                          <span>
                            handled{handler ? ` by ${handler}` : ""}{issue.handled_at ? ` ${formatTime(issue.handled_at)}` : ""}
                          </span>
                        ) : null}
                      </div>
                      <p className="issue-body">{issue.body}</p>
                    </article>
                  );
                })}
                {issues.length === 0 ? <div className="empty-panel">No mcodex mechanism issues.</div> : null}
              </div>
            </section>

            <div className="status-grid">
              <section className="status-subpanel">
                <div className="status-subpanel-header">
                  <p className="kicker">Sessions</p>
                  <span className="badge">{statusAgentIsApi ? "N/A" : sessions.length}</span>
                </div>
                <div className="status-list">
                  {statusAgentIsApi ? <div className="empty-panel">API agent: no tmux sessions.</div> : null}
                  {!statusAgentIsApi && sessions.map((session) => (
                    <div className="status-card" key={session.session_id}>
                      <div className="status-card-row">
                        <strong>{session.tmux_session}</strong>
                        <span className="badge subtle">{session.status}</span>
                      </div>
                      <div className="status-meta-grid">
                        <span>Pane {session.pane_id}</span>
                        <span>{session.cwd}</span>
                        <span>Started {formatTime(session.started_at)}</span>
                        <span>{session.ended_at ? `Ended ${formatTime(session.ended_at)}` : "Active session"}</span>
                      </div>
                    </div>
                  ))}
                  {!statusAgentIsApi && sessions.length === 0 ? <div className="empty-panel">No sessions yet.</div> : null}
                </div>
              </section>

              <section className="status-subpanel">
                <div className="status-subpanel-header">
                  <p className="kicker">Events</p>
                  <div className="status-subpanel-actions">
                    <div className="event-filter-bar" role="tablist" aria-label="Event filter">
                      {(["all", "lifecycle", "control", "message"] as EventFilter[]).map((filter) => (
                        <button
                          className={eventFilter === filter ? "event-filter-button active" : "event-filter-button"}
                          key={filter}
                          onClick={() => setEventFilter(filter)}
                          type="button"
                        >
                          {eventFilterLabel(filter)}
                        </button>
                      ))}
                    </div>
                    <span className="badge">{filteredEvents.length}</span>
                  </div>
                </div>
                <div className="status-list">
                  {filteredEvents.map((event) => (
                    <div className="status-card" key={event.event_id}>
                      <div className="status-card-row">
                        <strong className="status-event-type">{event.type}</strong>
                        <time>{formatTime(event.created_at)}</time>
                      </div>
                      <p className="status-card-copy">{eventSummary(event)}</p>
                    </div>
                  ))}
                  {filteredEvents.length === 0 ? <div className="empty-panel">No events in this filter.</div> : null}
                </div>
              </section>
            </div>
          </section>
        </div>

        {error ? <div className="error-banner">API error: {error}</div> : null}
      </main>
    );
  }

  return (
    <div className={view === "workspace" ? "app-shell" : "app-shell status-mode"}>
      {renderGroupsSidebar()}
      {view === "workspace" ? renderWorkspace() : view === "status" ? renderStatus() : null}
      <PerformanceView
        fetchMetrics={fetchMetricsText}
        rawMetricsUrl={metricsUrl()}
        visible={view === "performance"}
      />
    </div>
  );
}

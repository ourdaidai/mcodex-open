export type SseState = "connecting" | "connected" | "disconnected";
export type RefreshPlan = {
  agentDetails: boolean;
  archivedGroups: boolean;
  full: boolean;
  groupData: boolean;
  groups: boolean;
};

const EMPTY_PLAN: RefreshPlan = {
  agentDetails: false,
  archivedGroups: false,
  full: false,
  groupData: false,
  groups: false,
};

export function shouldRunRecoveryPoll(state: SseState): boolean {
  return state === "disconnected";
}

export function realtimeRefreshPlan(
  eventType: string,
  data: Record<string, unknown>,
  selectedGroupId: string | null,
  statusAgentId: string | null,
): RefreshPlan {
  if (eventType === "resync_required") {
    return { agentDetails: true, archivedGroups: true, full: true, groupData: true, groups: true };
  }
  const groupMatches = data.group_id === selectedGroupId;
  const agentMatches =
    data.agent_id === statusAgentId ||
    data.sender_agent_id === statusAgentId ||
    data.recipient_agent_id === statusAgentId;
  if (eventType === "group_updated") {
    return { ...EMPTY_PLAN, archivedGroups: true, groups: true, groupData: groupMatches };
  }
  if (eventType === "agent_updated") {
    return { ...EMPTY_PLAN, groups: true, groupData: groupMatches, agentDetails: agentMatches };
  }
  if (eventType === "agent_control") {
    return { ...EMPTY_PLAN, groupData: groupMatches, agentDetails: agentMatches };
  }
  if (eventType === "message_created") {
    return { ...EMPTY_PLAN, groups: true, groupData: groupMatches, agentDetails: agentMatches };
  }
  if (eventType === "message_delivery_updated") {
    return { ...EMPTY_PLAN, groupData: groupMatches, agentDetails: agentMatches };
  }
  if (eventType === "issue_created" || eventType === "issue_updated") {
    return { ...EMPTY_PLAN, groupData: groupMatches };
  }
  return EMPTY_PLAN;
}

export class DebouncedRefresh {
  private readonly timers = new Map<string, number>();

  constructor(private readonly delayMs: number) {}

  schedule(key: string, callback: () => void): void {
    if (this.timers.has(key)) return;
    this.timers.set(key, window.setTimeout(() => {
      this.timers.delete(key);
      callback();
    }, this.delayMs));
  }

  close(): void {
    for (const timer of this.timers.values()) window.clearTimeout(timer);
    this.timers.clear();
  }
}

export type FullRefreshCallbacks = {
  agentDetails?: () => void;
  groupData?: () => void;
  groups: () => void;
};

export function scheduleFullRefresh(
  refresh: DebouncedRefresh,
  callbacks: FullRefreshCallbacks,
): void {
  refresh.schedule("groups", callbacks.groups);
  if (callbacks.groupData) refresh.schedule("group-data", callbacks.groupData);
  if (callbacks.agentDetails) refresh.schedule("agent-details", callbacks.agentDetails);
}

export type Group = {
  agent_count: number;
  archived_at: string | null;
  created_at: string;
  group_id: string;
  message_count: number;
  message_count_capped: boolean;
  name: string;
  online_count: number;
};

export type Agent = {
  agent_id: string;
  created_at: string;
  display_name: string;
  group_id: string;
  is_system?: boolean;
  last_heartbeat_at: string | null;
  last_seen_at: string | null;
  pane_summary: string | null;
  pane_summary_updated_at: string | null;
  presence_expires_at: string | null;
  status: "offline" | "online" | "busy" | "idle";
  status_changed_at: string | null;
  transport: "tmux" | "api" | "system";
  updated_at: string;
};

export type Conversation = {
  conversation_id: string;
  created_at: string;
  group_id: string;
  last_message_at: string | null;
  last_message_body: string | null;
  participant_a: string;
  participant_b: string;
  updated_at: string;
};

export type Message = {
  body: string;
  client_request_id?: string | null;
  conversation_id: string;
  created_at: string;
  delivery_state?: "pending" | "claimed" | "acked" | "canceled";
  group_id: string;
  message_id: string;
  message_type?: "direct" | "pane_summary";
  recipient_agent_id: string;
  recipient_display_name?: string;
  sender_agent_id: string;
  sender_display_name?: string;
  sender_is_system?: boolean;
};

export type McodexIssue = {
  body: string;
  created_at: string;
  group_id: string;
  issue_id: string;
  issue_type:
    | "api_failed"
    | "watcher_incomplete"
    | "tmux_fallback_used"
    | "message_delivery_suspect"
    | "dashboard_mismatch";
  reporter_agent_id: string | null;
  reporter_display_name?: string | null;
  handled_at?: string | null;
  handled_by_agent_id?: string | null;
  handled_by_display_name?: string | null;
  source: string;
  status: "open" | "handled";
  title: string;
};

export type AgentSession = {
  agent_id: string;
  cwd: string;
  ended_at: string | null;
  pane_id: string;
  session_id: string;
  started_at: string;
  status: string;
  tmux_session: string;
};

export type AgentEvent = {
  agent_id: string;
  created_at: string;
  event_id: string;
  group_id: string;
  payload: Record<string, unknown>;
  payload_json: string;
  session_id: string | null;
  type: string;
};

export type CursorPage<T, K extends string> = {
  ok: boolean;
  next_cursor: string | null;
} & Record<K, T[]>;

export type PageResult<T> = {
  items: T[];
  nextCursor: string | null;
};

export type ControlResult = {
  action: "start" | "stop" | "reconnect";
  agent_id: string;
  cwd?: string;
  group_id: string;
  request_id: string;
  tmux_session?: string;
};

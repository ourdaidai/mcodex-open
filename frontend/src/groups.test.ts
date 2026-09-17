import { describe, expect, it } from "vitest";
import type { Group } from "./types";
import {
  formatMessageCount,
  groupRefreshTargets,
  hasOlderMessages,
  reconcileSelectedGroupId,
  shouldLoadArchivedGroups,
} from "./groups";

function group(
  groupId: string,
  messageCount: number,
  messageCountCapped = false,
): Group {
  return {
    agent_count: 0,
    archived_at: null,
    created_at: "2026-08-05T00:00:00.000000Z",
    group_id: groupId,
    message_count: messageCount,
    message_count_capped: messageCountCapped,
    name: groupId,
    online_count: 0,
  };
}

describe("group sidebar state", () => {
  it("formats exact and capped message counts", () => {
    expect(formatMessageCount(group("exact", 998))).toBe("998");
    expect(formatMessageCount(group("boundary", 999))).toBe("999");
    expect(formatMessageCount(group("capped", 1000, true))).toBe("999+");
  });

  it("keeps reporting older history for a fully loaded capped window", () => {
    expect(hasOlderMessages(group("capped", 1000, true), 1000)).toBe(true);
    expect(hasOlderMessages(group("exact", 80), 80)).toBe(false);
    expect(hasOlderMessages(group("older", 81), 80)).toBe(true);
  });

  it("preserves valid selection and falls back after archive", () => {
    const groups = [group("first", 0), group("second", 0)];
    expect(reconcileSelectedGroupId("second", groups)).toBe("second");
    expect(reconcileSelectedGroupId("removed", groups)).toBe("first");
    expect(reconcileSelectedGroupId("removed", [])).toBeNull();
  });

  it("selects a restored group after the active refresh includes it", () => {
    const groups = [group("first", 0), group("restored", 0)];
    expect(reconcileSelectedGroupId("first", groups, "restored")).toBe("restored");
  });

  it("loads archived groups only after first expansion", () => {
    expect(shouldLoadArchivedGroups(false, false)).toBe(false);
    expect(shouldLoadArchivedGroups(true, false)).toBe(true);
    expect(shouldLoadArchivedGroups(true, true)).toBe(false);
  });

  it("refreshes a cached archived collection on group updates", () => {
    expect(groupRefreshTargets(false)).toEqual({ active: true, archived: false });
    expect(groupRefreshTargets(true)).toEqual({ active: true, archived: true });
  });
});

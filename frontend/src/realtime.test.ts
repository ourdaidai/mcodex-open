import { afterEach, describe, expect, it, vi } from "vitest";
import {
  DebouncedRefresh,
  realtimeRefreshPlan,
  scheduleFullRefresh,
  shouldRunRecoveryPoll,
} from "./realtime";

afterEach(() => vi.useRealTimers());

describe("realtime refresh", () => {
  it("refreshes only resources affected by a normal message event", () => {
    expect(
      realtimeRefreshPlan(
        "message_created",
        { group_id: "default", sender_agent_id: "mail" },
        "default",
        "mail",
      ),
    ).toEqual({
      agentDetails: true,
      archivedGroups: false,
      full: false,
      groupData: true,
      groups: true,
    });
  });

  it("requests exactly one complete refresh after resync", () => {
    expect(realtimeRefreshPlan("resync_required", {}, "default", "mail").full).toBe(true);
  });

  it("refreshes active and cached archived groups after a group update", () => {
    expect(realtimeRefreshPlan("group_updated", { group_id: "old" }, "default", null)).toMatchObject({
      archivedGroups: true,
      groups: true,
    });
  });

  it("enables five-second recovery polling only while disconnected", () => {
    expect(shouldRunRecoveryPoll("connected")).toBe(false);
    expect(shouldRunRecoveryPoll("connecting")).toBe(false);
    expect(shouldRunRecoveryPoll("disconnected")).toBe(true);
  });

  it("schedules a catch-up refresh when SSE reconnects before recovery polling", () => {
    vi.useFakeTimers();
    const groups = vi.fn();
    const groupData = vi.fn();
    const agentDetails = vi.fn();
    const debouncer = new DebouncedRefresh(100);

    scheduleFullRefresh(debouncer, { groups, groupData, agentDetails });
    vi.advanceTimersByTime(100);
    vi.advanceTimersByTime(1_000);
    scheduleFullRefresh(debouncer, { groups, groupData, agentDetails });
    vi.advanceTimersByTime(100);

    expect(groups).toHaveBeenCalledTimes(2);
    expect(groupData).toHaveBeenCalledTimes(2);
    expect(agentDetails).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
    debouncer.close();
  });

  it("coalesces bursts by resource key", () => {
    vi.useFakeTimers();
    const refresh = vi.fn();
    const debouncer = new DebouncedRefresh(100);
    debouncer.schedule("groups", refresh);
    debouncer.schedule("groups", refresh);
    vi.advanceTimersByTime(100);
    expect(refresh).toHaveBeenCalledTimes(1);
    debouncer.close();
  });

  it("keeps the first deadline during a continuous burst and opens a new window afterward", () => {
    vi.useFakeTimers();
    const refresh = vi.fn();
    const debouncer = new DebouncedRefresh(100);

    debouncer.schedule("groups", refresh);
    vi.advanceTimersByTime(50);
    debouncer.schedule("groups", refresh);
    vi.advanceTimersByTime(50);
    expect(refresh).toHaveBeenCalledTimes(1);

    debouncer.schedule("groups", refresh);
    vi.advanceTimersByTime(50);
    debouncer.schedule("groups", refresh);
    vi.advanceTimersByTime(50);
    expect(refresh).toHaveBeenCalledTimes(2);
    debouncer.close();
  });

  it("keeps the first callback when later work shares its pending key", () => {
    vi.useFakeTimers();
    const resyncRefresh = vi.fn();
    const normalRefresh = vi.fn();
    const debouncer = new DebouncedRefresh(100);

    debouncer.schedule("groups", resyncRefresh);
    vi.advanceTimersByTime(50);
    debouncer.schedule("groups", normalRefresh);
    vi.advanceTimersByTime(50);

    expect(resyncRefresh).toHaveBeenCalledTimes(1);
    expect(normalRefresh).not.toHaveBeenCalled();
    debouncer.close();
  });

  it("cancels pending callbacks when closed", () => {
    vi.useFakeTimers();
    const refresh = vi.fn();
    const debouncer = new DebouncedRefresh(100);

    debouncer.schedule("groups", refresh);
    debouncer.close();
    vi.advanceTimersByTime(100);

    expect(refresh).not.toHaveBeenCalled();
  });
});

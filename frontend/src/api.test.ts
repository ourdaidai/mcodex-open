import { afterEach, describe, expect, it, vi } from "vitest";
import { archiveGroup, fetchGroups, restoreGroup } from "./api";

afterEach(() => {
  vi.unstubAllGlobals();
});

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    headers: { "Content-Type": "application/json" },
    status,
  });
}

describe("group archive API", () => {
  it("uses separate active and archived list contracts", async () => {
    const fetchMock = vi.fn().mockImplementation(async () => jsonResponse({ ok: true, groups: [] }));
    vi.stubGlobal("fetch", fetchMock);

    await fetchGroups();
    await fetchGroups("archived");

    expect(String(fetchMock.mock.calls[0][0])).toContain("/api/groups");
    expect(String(fetchMock.mock.calls[0][0])).not.toContain("status=");
    expect(String(fetchMock.mock.calls[1][0])).toContain("/api/groups?status=archived");
  });

  it("posts archive and restore mutations", async () => {
    const archived = { group_id: "old", archived_at: "2026-08-05T00:00:00Z" };
    const restored = { group_id: "old", archived_at: null };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ ok: true, group: archived }))
      .mockResolvedValueOnce(jsonResponse({ ok: true, group: restored }));
    vi.stubGlobal("fetch", fetchMock);

    expect(await archiveGroup("old")).toEqual(archived);
    expect(await restoreGroup("old")).toEqual(restored);
    expect(String(fetchMock.mock.calls[0][0])).toContain("/api/groups/old/archive");
    expect(fetchMock.mock.calls[0][1]).toMatchObject({ method: "POST" });
    expect(String(fetchMock.mock.calls[1][0])).toContain("/api/groups/old/restore");
  });

  it("surfaces one authoritative server conflict without retrying another base", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({ ok: false, error: "cannot archive group old while it has active agents" }, 409),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(archiveGroup("old")).rejects.toThrow("cannot archive group old while it has active agents");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});

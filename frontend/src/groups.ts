import type { Group } from "./types";

export function formatMessageCount(group: Pick<Group, "message_count" | "message_count_capped">): string {
  return group.message_count_capped ? "999+" : String(group.message_count);
}

export function hasOlderMessages(
  group: Pick<Group, "message_count" | "message_count_capped">,
  loadedCount: number,
): boolean {
  return group.message_count_capped || group.message_count > loadedCount;
}

export function reconcileSelectedGroupId(
  selectedGroupId: string | null,
  groups: Group[],
  preferredGroupId: string | null = null,
): string | null {
  if (preferredGroupId && groups.some((group) => group.group_id === preferredGroupId)) {
    return preferredGroupId;
  }
  if (selectedGroupId && groups.some((group) => group.group_id === selectedGroupId)) {
    return selectedGroupId;
  }
  return groups[0]?.group_id ?? null;
}

export function shouldLoadArchivedGroups(expanded: boolean, loaded: boolean): boolean {
  return expanded && !loaded;
}

export function groupRefreshTargets(archivedLoaded: boolean): { active: true; archived: boolean } {
  return { active: true, archived: archivedLoaded };
}

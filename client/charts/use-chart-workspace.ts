/**
 * use-chart-workspace.ts — Manages chart workspace configuration state.
 *
 * Provides workspace state and a helper to update individual workspace fields.
 * Keeps state management logic separate from UI rendering.
 */

import { useState } from "react";
import type { WorkspaceState } from "./chart-prototype.types";
import { DEFAULT_WORKSPACE } from "./chart-prototype.constants";

export function useChartWorkspace() {
  const [ws, setWs] = useState<WorkspaceState>(DEFAULT_WORKSPACE);

  const setWsField = <K extends keyof WorkspaceState>(
    k: K,
    v: WorkspaceState[K]
  ) => setWs((prev) => ({ ...prev, [k]: v }));

  return { ws, setWsField, setWs };
}

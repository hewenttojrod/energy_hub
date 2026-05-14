/**
 * saved-charts-shelf.tsx — Display saved chart definitions at bottom of main area.
 *
 * Shows horizontally scrollable list of saved chart configs.
 * Supports load and delete operations.
 */

import type { ChartDefinition } from "./chart-api";

interface SavedChartsShelfProps {
  savedCharts: ChartDefinition[];
  activeChartId: number | null;
  onLoad: (chart: ChartDefinition) => void;
  onDelete: (id: number) => void;
}

export function SavedChartsShelf({
  savedCharts,
  activeChartId,
  onLoad,
  onDelete,
}: SavedChartsShelfProps) {
  if (savedCharts.length === 0) {
    return null;
  }

  return (
    <div className="border-t border-slate-200 bg-white px-4 py-3 dark:border-slate-700 dark:bg-slate-900">
      <p className="mb-2 text-xs font-semibold text-slate-500 uppercase tracking-wide">
        Saved Charts
      </p>
      <div className="flex gap-2 overflow-x-auto pb-1">
        {savedCharts.map((chart) => (
          <div
            key={chart.chart_definition_id}
            className={`flex shrink-0 items-center gap-1.5 rounded border px-3 py-1.5 text-xs transition-colors cursor-pointer ${
              activeChartId === chart.chart_definition_id
                ? "border-blue-400 bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300"
                : "border-slate-200 bg-slate-50 text-slate-600 hover:border-slate-300 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"
            }`}
            onClick={() => onLoad(chart)}
          >
            <span className="max-w-30 truncate font-medium">
              {chart.name}
            </span>
            <span className="text-slate-400">{chart.chart_type}</span>
            <button
              className="ml-1 text-slate-400 hover:text-red-500"
              onClick={(e) => {
                e.stopPropagation();
                onDelete(chart.chart_definition_id);
              }}
              title="Delete"
            >
              ×
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}

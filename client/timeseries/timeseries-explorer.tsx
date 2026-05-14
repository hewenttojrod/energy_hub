/**
 * Timeseries point explorer page.
 *
 * Provides a filter bar (source system, dataset key, date range, row limit, column mapping
 * checkboxes) that, when applied, passes query params to a `DataGrid` showing
 * `timeseries_point` rows. Clicking a row opens `TimeseriesRawRecordModal` to inspect
 * the originating CSV row.
 *
 * Column mappings are loaded from the API on mount (auto-all-selected) and can be
 * reloaded manually after changing source system or dataset key. The "Apply" button
 * propagates filter state into `appliedParams` which is the sole dependency driving
 * the grid's data fetch.
 */
import { useEffect, useMemo, useRef, useState } from "react";

import DataGrid from "@templates/data-grid";

import {
  fetchColumnMappings,
  TIMESERIES_POINTS_ENDPOINT,
  type ColumnMappingOption,
  type TimeseriesPointRow,
} from "./timeseries-api";
import TimeseriesRawRecordModal from "./timeseries-raw-record-modal";
import {
  buildAppliedParams,
  groupMappingsByDataset,
  TIMESERIES_COLUMNS,
} from "./timeseries-explorer.utils";

export default function TimeseriesExplorer() {
  // --- filter state ---
  const [sourceSystem, setSourceSystem] = useState("nyiso");
  const [datasetKey, setDatasetKey] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [limit, setLimit] = useState("500");

  // column mapping picker
  const [mappings, setMappings] = useState<ColumnMappingOption[]>([]);
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());
  const [mappingsLoading, setMappingsLoading] = useState(false);

  // applied params sent to DataGrid
  const [appliedParams, setAppliedParams] = useState<Record<string, string>>({});

  // raw record modal
  const [activePointId, setActivePointId] = useState<number | null>(null);

  const isMountedRef = useRef(false);

  // Load column mappings whenever source_system or dataset_key changes
  const loadMappings = async (ss: string, dk: string) => {
    if (!ss) return;
    setMappingsLoading(true);
    setSelectedIds(new Set());
    try {
      const rows = await fetchColumnMappings(ss || undefined, dk || undefined);
      if (isMountedRef.current) {
        setMappings(rows);
        // Auto-select all by default
        setSelectedIds(new Set(rows.map((r) => r.column_mapping_id)));
      }
    } catch {
      if (isMountedRef.current) setMappings([]);
    } finally {
      if (isMountedRef.current) setMappingsLoading(false);
    }
  };

  // Load column mappings once on mount with the default source system.
  // `isMountedRef` prevents stale state writes if the component unmounts during the async call.
  // Subsequent reloads are triggered manually via `handleReloadMappings` (not a useEffect)
  // so that the user controls when the filter is applied, avoiding race conditions.
  useEffect(() => {
    isMountedRef.current = true;
    void loadMappings(sourceSystem, datasetKey);
    return () => {
      isMountedRef.current = false;
    };
  }, []); // only on mount; user triggers reload via Apply

  const handleApply = () => {
    setAppliedParams(
      buildAppliedParams({
        selectedIds,
        sourceSystem,
        datasetKey,
        dateFrom,
        dateTo,
        limit,
      })
    );
  };

  const handleReloadMappings = () => {
    void loadMappings(sourceSystem, datasetKey);
  };

  const toggleMapping = (id: number) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleAll = () => {
    if (selectedIds.size === mappings.length) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(mappings.map((m) => m.column_mapping_id)));
    }
  };

  const handleRowClick = (row: TimeseriesPointRow) => {
    setActivePointId(row.timeseries_point_id);
  };

  const gridParams = useMemo(() => appliedParams, [appliedParams]);

  // Group mappings by dataset_key for display
  const mappingsByDataset = useMemo(() => groupMappingsByDataset(mappings), [mappings]);

  return (
    <div className="flex h-full flex-col gap-4 p-4">
      {/* ── Filter bar ─────────────────────────────────────────────────── */}
      <div className="rounded-lg border border-ui-border bg-ui-surface p-4">
        <h1 className="mb-3 text-sm font-semibold text-primary">Timeseries Explorer</h1>

        {/* Row 1: source / dataset / date range / limit */}
        <div className="mb-3 flex flex-wrap items-end gap-3">
          <label className="flex flex-col gap-1 text-xs text-muted">
            Source System
            <input
              className="w-32 rounded border border-ui-border bg-ui-bg px-2 py-1 text-xs text-primary outline-none focus:border-primary"
              value={sourceSystem}
              onChange={(e) => setSourceSystem(e.target.value)}
            />
          </label>

          <label className="flex flex-col gap-1 text-xs text-muted">
            Dataset Key
            <input
              className="w-28 rounded border border-ui-border bg-ui-bg px-2 py-1 text-xs text-primary outline-none focus:border-primary"
              placeholder="e.g. P-63"
              value={datasetKey}
              onChange={(e) => setDatasetKey(e.target.value)}
            />
          </label>

          <button
            className="self-end rounded border border-ui-border bg-ui-bg px-3 py-1 text-xs text-primary hover:bg-ui-hover disabled:opacity-50"
            disabled={mappingsLoading}
            onClick={handleReloadMappings}
          >
            {mappingsLoading ? "Loading…" : "Load Columns"}
          </button>

          <span className="self-end h-6 w-px bg-ui-border" />

          <label className="flex flex-col gap-1 text-xs text-muted">
            Date From
            <input
              type="date"
              className="rounded border border-ui-border bg-ui-bg px-2 py-1 text-xs text-primary outline-none focus:border-primary"
              value={dateFrom}
              onChange={(e) => setDateFrom(e.target.value)}
            />
          </label>

          <label className="flex flex-col gap-1 text-xs text-muted">
            Date To
            <input
              type="date"
              className="rounded border border-ui-border bg-ui-bg px-2 py-1 text-xs text-primary outline-none focus:border-primary"
              value={dateTo}
              onChange={(e) => setDateTo(e.target.value)}
            />
          </label>

          <label className="flex flex-col gap-1 text-xs text-muted">
            Limit
            <input
              type="number"
              min={1}
              max={2000}
              className="w-20 rounded border border-ui-border bg-ui-bg px-2 py-1 text-xs text-primary outline-none focus:border-primary"
              value={limit}
              onChange={(e) => setLimit(e.target.value)}
            />
          </label>

          <button
            className="self-end rounded bg-primary px-4 py-1 text-xs font-medium text-white hover:opacity-90"
            onClick={handleApply}
          >
            Apply
          </button>
        </div>

        {/* Row 2: column mapping checkboxes */}
        {mappings.length > 0 && (
          <div className="rounded border border-ui-border bg-ui-bg p-2">
            <div className="mb-2 flex items-center gap-3">
              <span className="text-xs font-medium text-muted">Columns</span>
              <button
                className="text-xs text-primary underline"
                onClick={toggleAll}
              >
                {selectedIds.size === mappings.length ? "Deselect all" : "Select all"}
              </button>
            </div>
            <div className="flex flex-wrap gap-x-4 gap-y-1">
              {Object.entries(mappingsByDataset).map(([dk, cols]) => (
                <div key={dk} className="flex flex-wrap gap-x-3 gap-y-1">
                  {cols.map((m) => (
                    <label
                      key={m.column_mapping_id}
                      className="flex cursor-pointer items-center gap-1 text-xs text-primary"
                    >
                      <input
                        type="checkbox"
                        checked={selectedIds.has(m.column_mapping_id)}
                        onChange={() => toggleMapping(m.column_mapping_id)}
                        className="h-3 w-3"
                      />
                      <span className="font-mono">{m.column_label || m.raw_column}</span>
                      {m.dataset_key !== datasetKey && (
                        <span className="text-muted">({m.dataset_key})</span>
                      )}
                    </label>
                  ))}
                </div>
              ))}
            </div>
          </div>
        )}

        {mappings.length === 0 && !mappingsLoading && (
          <p className="text-xs text-muted">
            Enter a source system and click <strong>Load Columns</strong> to filter by column mapping.
          </p>
        )}
      </div>

      {/* ── Data grid ──────────────────────────────────────────────────── */}
      <div className="min-h-0 flex-1 overflow-hidden rounded-lg border border-ui-border bg-ui-surface">
        {Object.keys(gridParams).length === 0 ? (
          <div className="flex h-full items-center justify-center text-sm text-muted">
            Configure filters above and click <strong className="mx-1">Apply</strong> to load data.
          </div>
        ) : (
          <DataGrid<TimeseriesPointRow>
            columns={TIMESERIES_COLUMNS}
            endpoint={TIMESERIES_POINTS_ENDPOINT}
            params={gridParams}
            rowKey="timeseries_point_id"
            onRowClick={handleRowClick}
            layoutOptions={{ stickyHeader: true, stretchToContainer: true, maxHeight: "calc(100vh - 280px)" }}
          />
        )}
      </div>

      {/* ── Raw record modal ───────────────────────────────────────────── */}
      {activePointId !== null && (
        <TimeseriesRawRecordModal
          pointId={activePointId}
          onClose={() => setActivePointId(null)}
        />
      )}
    </div>
  );
}

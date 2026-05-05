import { useEffect, useMemo, useRef, useState } from "react";

import type { ColumnDef } from "@app-types/api";
import DataGrid from "@templates/data-grid";

import {
  fetchColumnMappings,
  fetchRawRecord,
  TIMESERIES_POINTS_ENDPOINT,
  type ColumnMappingOption,
  type RawRecord,
  type TimeseriesPointRow,
} from "./timeseries-api";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatValueJson(value_json: Record<string, unknown>): string {
  const entries = Object.entries(value_json);
  if (entries.length === 0) return "—";
  if (entries.length === 1) {
    const [k, v] = entries[0];
    return `${k}: ${String(v)}`;
  }
  return entries
    .slice(0, 3)
    .map(([k, v]) => `${k}: ${String(v)}`)
    .join("  |  ")
    .concat(entries.length > 3 ? "  …" : "");
}

function formatTs(ts: string): string {
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return ts;
  }
}

// ---------------------------------------------------------------------------
// Raw-record modal
// ---------------------------------------------------------------------------

type RawRecordModalProps = {
  pointId: number;
  onClose: () => void;
};

function RawRecordModal({ pointId, onClose }: RawRecordModalProps) {
  const [record, setRecord] = useState<RawRecord | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setRecord(null);
    setError(null);

    fetchRawRecord(pointId)
      .then((r) => {
        if (!cancelled) setRecord(r);
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : "Failed to load raw record.");
      });

    return () => {
      cancelled = true;
    };
  }, [pointId]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
      onClick={onClose}
    >
      <div
        className="relative w-full max-w-4xl mx-auto max-h-[85vh] overflow-y-auto rounded-lg border border-ui-border p-6 shadow-2xl bg-black"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          className="absolute right-4 top-4 text-sm font-medium text-muted hover:text-primary"
          onClick={onClose}
        >
          ✕ Close
        </button>

        <h2 className="mb-4 text-lg font-semibold text-primary">Raw CSV Record</h2>

        {!record && !error && (
          <p className="mt-4 text-sm text-muted">Loading…</p>
        )}

        {error && (
          <p className="mt-4 text-sm text-red-500">{error}</p>
        )}

        {record && (
          <>
            {/* Metadata section */}
            <div className="mb-4 rounded border border-ui-border bg-ui-bg p-3">
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <p className="text-xs font-medium text-muted">Record ID</p>
                  <p className="font-mono text-primary">{record.raw_record_id}</p>
                </div>
                <div>
                  <p className="text-xs font-medium text-muted">Row Number</p>
                  <p className="font-mono text-primary">{record.row_number}</p>
                </div>
                <div>
                  <p className="text-xs font-medium text-muted">Source File ID</p>
                  <p className="font-mono text-primary">{record.source_file_id}</p>
                </div>
                <div>
                  <p className="text-xs font-medium text-muted">Source File Name</p>
                  <p className="font-mono text-xs text-primary break-all">{record.source_file_name}</p>
                </div>
              </div>
            </div>

            {/* Raw fields section */}
            <div>
              <h3 className="mb-2 text-sm font-semibold text-primary">CSV Fields</h3>
              <div className="overflow-x-auto rounded border border-ui-border">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-ui-border bg-ui-bg text-left text-xs font-medium text-muted">
                      <th className="px-3 py-2 w-1/3">Field Name</th>
                      <th className="px-3 py-2">Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(record.row_payload_json).length === 0 ? (
                      <tr>
                        <td colSpan={2} className="px-3 py-2 text-center text-muted">
                          (No fields)
                        </td>
                      </tr>
                    ) : (
                      Object.entries(record.row_payload_json).map(([k, v], idx) => (
                        <tr
                          key={k}
                          className={`border-b border-ui-border/50 last:border-0 ${
                            idx % 2 === 0 ? "bg-ui-surface" : "bg-ui-bg"
                          }`}
                        >
                          <td className="px-3 py-2 font-mono text-xs font-medium text-muted">
                            {k}
                          </td>
                          <td className="px-3 py-2 font-mono text-xs text-primary max-w-lg">
                            {String(v ?? "—")}
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

const COLUMNS: ColumnDef<TimeseriesPointRow>[] = [
  { key: "ts_utc", label: "Timestamp (UTC)", width: "170px", sortable: true, render: (v) => formatTs(String(v)) },
  { key: "column_label", label: "Column", width: "160px", sortable: true },
  { key: "semantic_key", label: "Semantic Key", width: "140px", sortable: false },
  { key: "unit_name", label: "Unit", width: "80px" },
  { key: "value_json", label: "Values", render: (v) => formatValueJson(v as Record<string, unknown>) },
  { key: "quality_flag", label: "Quality", width: "90px" },
  { key: "source_file_name", label: "Source File", width: "220px", sortable: true },
];

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

  useEffect(() => {
    isMountedRef.current = true;
    void loadMappings(sourceSystem, datasetKey);
    return () => {
      isMountedRef.current = false;
    };
  }, []); // only on mount; user triggers reload via Apply

  const handleApply = () => {
    const params: Record<string, string> = {};
    if (selectedIds.size > 0) {
      params.column_mapping_ids = [...selectedIds].join(",");
    } else {
      if (sourceSystem) params.source_system = sourceSystem;
      if (datasetKey) params.dataset_key = datasetKey;
    }
    if (dateFrom) params.date_from = dateFrom;
    if (dateTo) params.date_to = dateTo;
    params.limit = limit || "500";
    setAppliedParams(params);
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
  const mappingsByDataset = useMemo(() => {
    const groups: Record<string, ColumnMappingOption[]> = {};
    for (const m of mappings) {
      (groups[m.dataset_key] ??= []).push(m);
    }
    return groups;
  }, [mappings]);

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
            columns={COLUMNS}
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
        <RawRecordModal
          pointId={activePointId}
          onClose={() => setActivePointId(null)}
        />
      )}
    </div>
  );
}

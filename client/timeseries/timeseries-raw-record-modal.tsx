/**
 * Modal overlay that fetches and displays the raw CSV row that produced a timeseries point.
 *
 * Appears above all other content (z-50) with a semi-transparent backdrop. Clicking the
 * backdrop or the "X Close" button calls `onClose`.
 *
 * @param pointId - The `timeseries_point_id` of the row whose raw record should be loaded.
 * @param onClose - Callback to dismiss the modal (parent is responsible for clearing state).
 */
import { useEffect, useState } from "react";

import EmptyState from "@templates/empty-state";
import ErrorBanner from "@templates/error-banner";
import LoadingState from "@templates/loading-state";

import { fetchRawRecordsByTimestamp, type RawRecord } from "./timeseries-api";

type TimeseriesRawRecordModalProps = {
  pointId: number;
  onClose: () => void;
};

export default function TimeseriesRawRecordModal({
  pointId,
  onClose,
}: TimeseriesRawRecordModalProps) {
  const [records, setRecords] = useState<RawRecord[]>([]);
  const [error, setError] = useState<string | null>(null);

  // Fetch the raw records whenever pointId changes (e.g. user clicks a different row).
  // The `cancelled` flag is a simple "mounted" guard — preferred here over AbortController
  // because `fetchRawRecordsByTimestamp` does not expose its signal externally.
  // Resets to []/null before each new fetch so stale data is not shown during loading.
  useEffect(() => {
    let cancelled = false;
    setRecords([]);
    setError(null);

    fetchRawRecordsByTimestamp(pointId)
      .then((results) => {
        if (!cancelled) setRecords(results);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load raw records.");
        }
      });

    return () => {
      cancelled = true;
    };
  }, [pointId]);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onClick={onClose}>
      <div
        className="relative mx-auto max-h-[85vh] w-full max-w-4xl overflow-y-auto rounded-lg border border-ui-border bg-black p-6 shadow-2xl"
        onClick={(event) => event.stopPropagation()}
      >
        <button
          className="absolute right-4 top-4 text-sm font-medium text-muted hover:text-primary"
          onClick={onClose}
        >
          X Close
        </button>

        <h2 className="mb-4 text-lg font-semibold text-primary">Raw CSV Records by Timestamp</h2>

        {records.length === 0 && !error && <LoadingState label="Loading raw records..." />}

        {error && <ErrorBanner message={error} />}

        {records.length > 0 && (
          <div className="space-y-6">
            {records.map((record, recordIndex) => (
              <div key={recordIndex}>
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
                      <p className="break-all font-mono text-xs text-primary">{record.source_file_name}</p>
                    </div>
                  </div>
                </div>

                <div>
                  <h3 className="mb-2 text-sm font-semibold text-primary">CSV Fields</h3>
                  <div className="overflow-x-auto rounded border border-ui-border">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-ui-border bg-ui-bg text-left text-xs font-medium text-muted">
                          <th className="w-1/3 px-3 py-2">Field Name</th>
                          <th className="px-3 py-2">Value</th>
                        </tr>
                      </thead>
                      <tbody>
                        {Object.entries(record.row_payload_json).length === 0 ? (
                          <tr>
                            <td colSpan={2} className="px-3 py-2">
                              <EmptyState label="No fields." />
                            </td>
                          </tr>
                        ) : (
                          Object.entries(record.row_payload_json).map(([key, value], index) => (
                            <tr
                              key={key}
                              className={`border-b border-ui-border/50 last:border-0 ${
                                index % 2 === 0 ? "bg-ui-surface" : "bg-ui-bg"
                              }`}
                            >
                              <td className="px-3 py-2 font-mono text-xs font-medium text-muted">{key}</td>
                              <td className="max-w-lg px-3 py-2 font-mono text-xs text-primary">
                                {String(value ?? "-")}
                              </td>
                            </tr>
                          ))
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

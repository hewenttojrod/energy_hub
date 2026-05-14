/**
 * NYISO report detail page (route: /energy_hub/reports/:reportId).
 *
 * Loads the metadata row and file content for a single report identified by the
 * `reportId` URL parameter. Also polls for task completion when the report's
 * `task_status` is in an active state (QUEUED or RUNNING).
 *
 * Two key useEffect patterns are used:
 *  1. Load on param change — re-fetches when `parsedId` changes (navigating between reports).
 *  2. Polling effect — active only while `row.task_status` is in `ACTIVE_STATES`; the
 *     interval is cleared and re-registered whenever the row or its status changes, ensuring
 *     stale closures are never used.
 */
import { useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";

import ErrorBanner from "@templates/error-banner";
import FormBody from "@templates/form-body";
import LoadingState from "@templates/loading-state";
import SectionPanel from "@templates/section-panel";
import SuccessBanner from "@templates/success-banner";
import { formatNullableTimestamp } from "@/utils/display-format";
import ReportContentPanel, { type ReportDetailContentPayload } from "./report-content-panel";

import {
  fetchNyisoReportRowContent,
  fetchNyisoReportRowById,
  pollNyisoReportRefreshStatus,
  refreshNyisoReportRow,
} from "./report-api";

type NyisoReportRow = {
  nyiso_report_id: number;
  code: string;
  name: string;
  content_type: string;
  frequency: string[];
  file_type: string[];
  source_page: string;
  latest_report_stamp: string | null;
  earliest_report_stamp: string | null;
  file_name_format: string;
  parse_status: string;
  task_status: string;
  task_updated_at: string | null;
  last_scanned_at: string | null;
  is_deprecated: boolean;
};

const ACTIVE_STATES = new Set(["QUEUED", "RUNNING"]);

export default function NyisoReportDetail() {
  const { reportId } = useParams<{ reportId: string }>();
  const parsedId = Number(reportId);

  const [row, setRow] = useState<NyisoReportRow | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [content, setContent] = useState<ReportDetailContentPayload | null>(null);
  const [contentLoading, setContentLoading] = useState(false);
  const [contentError, setContentError] = useState<string | null>(null);

  const loadContent = async (targetReportId: number) => {
    setContentLoading(true);
    try {
      const payload = await fetchNyisoReportRowContent<ReportDetailContentPayload>(targetReportId);
      setContent(payload);
      setContentError(null);
    } catch (err) {
      setContentError((err as Error).message);
      setContent(null);
    } finally {
      setContentLoading(false);
    }
  };

  const loadRow = async () => {
    if (!Number.isInteger(parsedId) || parsedId <= 0) {
      setError("Invalid report id.");
      setLoading(false);
      return;
    }

    try {
      const payload = await fetchNyisoReportRowById<NyisoReportRow>(parsedId);
      setRow(payload);
      await loadContent(payload.nyiso_report_id);
      setError(null);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  };

  // Load the report row (and its content) whenever the parsed route ID changes.
  // Validates the ID before fetching to show a user-facing error for invalid routes.
  useEffect(() => {
    void loadRow();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [parsedId]);

  // Poll every 5 seconds while this report's task is still active (QUEUED or RUNNING).
  // The effect re-runs whenever `row.nyiso_report_id` or `row.task_status` changes, so
  // the interval is always scoped to the current row state. `mounted` prevents stale
  // state updates if the component unmounts or the dependencies change before a tick fires.
  useEffect(() => {
    if (!row || !ACTIVE_STATES.has(row.task_status)) {
      return;
    }

    let mounted = true;
    const timer = window.setInterval(async () => {
      try {
        const status = await pollNyisoReportRefreshStatus();
        if (!mounted) {
          return;
        }

        const isFinished = status.finished_report_ids.includes(row.nyiso_report_id);
        const isStillActive = status.active_report_ids.includes(row.nyiso_report_id);

        if (isFinished || !isStillActive) {
          await loadRow();
          setActionMessage(null);
        }
      } catch {
        // Keep polling; next interval retries.
      }
    }, 5000);

    return () => {
      mounted = false;
      window.clearInterval(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [row?.nyiso_report_id, row?.task_status]);

  const onRefresh = async () => {
    if (!row) {
      return;
    }

    setRefreshing(true);
    setActionMessage(null);

    try {
      const payload = await refreshNyisoReportRow(row.nyiso_report_id);
      setActionMessage(payload.message);
      await loadRow();
    } catch (err) {
      setActionMessage((err as Error).message);
    } finally {
      setRefreshing(false);
    }
  };

  const detailRows = useMemo(() => {
    if (!row) {
      return [] as Array<{ label: string; value: string }>;
    }

    return [
      { label: "ID", value: String(row.nyiso_report_id) },
      { label: "Code", value: row.code },
      { label: "Name", value: row.name },
      { label: "Frequency", value: row.frequency.length ? row.frequency.join(", ") : "-" },
      { label: "File Type", value: row.file_type.length ? row.file_type.join(", ") : "-" },
      { label: "Source Page", value: row.source_page || "-" },
      { label: "File Name Format", value: row.file_name_format || "-" },
      { label: "Parse Status", value: row.parse_status },
      { label: "Task Status", value: row.task_status },
      { label: "Task Updated", value: formatNullableTimestamp(row.task_updated_at) },
      { label: "Latest Stamp", value: formatNullableTimestamp(row.latest_report_stamp) },
      { label: "Earliest Stamp", value: formatNullableTimestamp(row.earliest_report_stamp) },
      { label: "Last Scanned", value: formatNullableTimestamp(row.last_scanned_at) },
      { label: "Deprecated", value: row.is_deprecated ? "Yes" : "No" },
    ];
  }, [row]);

  return (
    <FormBody
      title="NYISO Report Detail"
      subtitle="Read-only report row data with optional backend refresh for this report."
      className="flex h-[calc(100dvh)] flex-col overflow-hidden"
      bodyClassName="flex min-h-0 flex-1 flex-col space-y-4"
    >
      <div className="mb-4 flex items-center gap-3">
        <button type="button" className="btn-primary" onClick={onRefresh} disabled={!row || refreshing}>
          {refreshing ? "Refreshing..." : "Refresh Report"}
        </button>
        <a className="btn-secondary" href="/energy_hub/reports">
          Back to Reports
        </a>
      </div>

      {actionMessage && <SuccessBanner message={actionMessage} />}

      {loading && <LoadingState label="Loading report row..." />}
      {error && <ErrorBanner message={error} onRetry={() => void loadRow()} />}

      {!loading && !error && row && (
        <>
          <SectionPanel title="Report Metadata">
            <div className="detail-table-wrap">
              <table className="detail-table">
                <tbody>
                  {detailRows.map((entry) => (
                    <tr key={entry.label} className="detail-table__row">
                      <th className="detail-table__header-cell">
                        {entry.label}
                      </th>
                      <td className="detail-table__value-cell">
                        {entry.label === "Source Page" && row.source_page ? (
                          <a
                            className="hyperlink"
                            href={`https://mis.nyiso.com/public/${row.source_page}`}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            {entry.value}
                          </a>
                        ) : (
                          entry.value
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </SectionPanel>

          <ReportContentPanel
            content={content}
            contentLoading={contentLoading}
            contentError={contentError}
            onRetry={() => {
              if (row) {
                void loadContent(row.nyiso_report_id);
                return;
              }
              void loadRow();
            }}
          />
        </>
      )}
    </FormBody>
  );
}

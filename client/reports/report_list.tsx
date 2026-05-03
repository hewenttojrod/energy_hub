import { useEffect, useMemo, useRef, useState } from "react";

import type { ColumnDef } from "@app-types/api";
import DataGrid from "@templates/data-grid";
import FormBody from "@templates/form-body";

import {
  fetchNyisoReportRowsByIds,
  NYISO_REPORT_LIST_ENDPOINT,
  pollNyisoReportRefreshStatus,
  startNyisoReportRefresh,
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

export default function NyisoReportList() {
  const [rowPatches, setRowPatches] = useState<NyisoReportRow[]>([]);
  const pollHandleRef = useRef<number | undefined>(undefined);
  const cursorRef = useRef("");
  const isMountedRef = useRef(false);

  const pollOnce = async () => {
    if (!isMountedRef.current) {
      return;
    }

    try {
      const status = await pollNyisoReportRefreshStatus(cursorRef.current);
      cursorRef.current = status.cursor || cursorRef.current;

      if (status.finished_count > 0) {
        const updatedRows = await fetchNyisoReportRowsByIds<NyisoReportRow>(status.finished_report_ids);
        if (isMountedRef.current && updatedRows.length > 0) {
          setRowPatches(updatedRows);
        }
      }

      if (status.active_count === 0 && pollHandleRef.current) {
        window.clearInterval(pollHandleRef.current);
        pollHandleRef.current = undefined;
      }
    } catch {
      // Keep polling on transient failures; next interval attempts again.
    }
  };

  const startBackgroundRefresh = async () => {
    try {
      const start = await startNyisoReportRefresh(false);
      cursorRef.current = start.cursor || cursorRef.current;

      if ((start.active_count > 0 || start.queued_count > 0) && !pollHandleRef.current) {
        pollHandleRef.current = window.setInterval(() => {
          void pollOnce();
        }, 5000);
      }
    } catch {
      // Initial queueing is best effort; the grid can still show current DB rows.
    }
  };

  useEffect(() => {
    isMountedRef.current = true;
    void startBackgroundRefresh();

    return () => {
      isMountedRef.current = false;
      if (pollHandleRef.current) {
        window.clearInterval(pollHandleRef.current);
        pollHandleRef.current = undefined;
      }
    };
  }, []);

  const renderTimestamp = (value: string | null) => {
    if (!value) {
      return "-";
    }
    const timestamp = new Date(value);
    return Number.isNaN(timestamp.getTime()) ? value : timestamp.toLocaleString();
  };

  const columns: ColumnDef<NyisoReportRow>[] = useMemo(
    () => [
      { key: "nyiso_report_id", label: "ID", width: "90px", sortable: true },
      {
        key: "code",
        label: "Report Code",
        sortable: true,
        render: (_value, row) => (
          <a className="hyperlink" href={`/energy_hub/reports/${row.nyiso_report_id}`}>
            {row.code}
          </a>
        ),
      },
      { key: "name", label: "Report Name", sortable: true },
      {
        key: "content_type",
        label: "Content Type",
        sortable: true,
        render: (_value, row) => row.content_type || "-",
      },
      {
        key: "frequency",
        label: "Frequency",
        render: (_value, row) =>
          Array.isArray(row.frequency) && row.frequency.length > 0
            ? row.frequency.join(", ")
            : "-",
      },
      {
        key: "file_type",
        label: "File Types",
        render: (_value, row) =>
          Array.isArray(row.file_type) && row.file_type.length > 0 ? row.file_type.join(", ") : "-",
      },
      {
        key: "source_page",
        label: "Source Page",
        render: (_value, row) =>
          row.source_page ? (
            <a
              className="hyperlink"
              href={`https://mis.nyiso.com/public/${row.source_page}`}
              target="_blank"
              rel="noopener noreferrer"
            >
              {row.source_page}
            </a>
          ) : (
            "-"
          ),
      },
      {
        key: "file_name_format",
        label: "File Name Format",
        render: (_value, row) => row.file_name_format || "-",
      },
      { key: "parse_status", label: "Parse Status", sortable: true },
      {
        key: "latest_report_stamp",
        label: "Latest Stamp",
        render: (_value, row) => renderTimestamp(row.latest_report_stamp),
      },
      {
        key: "earliest_report_stamp",
        label: "Earliest Stamp",
        render: (_value, row) => renderTimestamp(row.earliest_report_stamp),
      },
      { key: "task_status", label: "Task Status", sortable: true },
      {
        key: "task_updated_at",
        label: "Task Updated",
        render: (_value, row) => renderTimestamp(row.task_updated_at),
      },
      {
        key: "last_scanned_at",
        label: "Last Scanned",
        render: (_value, row) => renderTimestamp(row.last_scanned_at),
      },
      {
        key: "is_deprecated",
        label: "Deprecated",
        sortable: true,
        render: (_value, row) => (row.is_deprecated ? "Yes" : "No"),
      },
    ],
    []
  );

  return (
    <FormBody
      title="NYISO Reports"
      subtitle="All reports discovered from NYISO index data and stored in nyiso_report."
    >
      <DataGrid<NyisoReportRow>
        columns={columns}
        endpoint={NYISO_REPORT_LIST_ENDPOINT}
        rowKey="nyiso_report_id"
        rowPatches={rowPatches}
        onRefresh={startBackgroundRefresh}
      />
    </FormBody>
  );
}

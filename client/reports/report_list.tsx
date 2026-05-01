import { useMemo } from "react";

import type { ColumnDef } from "@app-types/api";
import DataGrid from "@templates/data-grid";
import FormBody from "@templates/form-body";

import { NYISO_REPORT_LIST_ENDPOINT } from "./report-api";

type NyisoReportRow = {
  nyiso_report_id: number;
  code: string;
  name: string;
  frequency: string[];
  is_deprecated: boolean;
};

export default function NyisoReportList() {
  const columns: ColumnDef<NyisoReportRow>[] = useMemo(
    () => [
      { key: "nyiso_report_id", label: "ID", width: "90px", sortable: true },
      { key: "code", label: "Report Code", sortable: true },
      { key: "name", label: "Report Name", sortable: true },
      {
        key: "frequency",
        label: "Frequency",
        render: (_value, row) =>
          Array.isArray(row.frequency) && row.frequency.length > 0
            ? row.frequency.join(", ")
            : "-",
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
      <DataGrid<NyisoReportRow> columns={columns} endpoint={NYISO_REPORT_LIST_ENDPOINT} />
    </FormBody>
  );
}

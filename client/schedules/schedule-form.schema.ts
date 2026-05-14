import type { SelectOption } from "@app-types/api";
import type { FormSchema } from "@/schemas/form-schema.types";

import type { NyisoSchedule, NyisoScheduleCreatePayload } from "./schedule-api";

const DEFAULT_TEMPLATE_VALUES_JSON = '{"fileextension":"csv"}';
const DEFAULT_SCHEDULE_NAME = "Daily Metadata Refresh";

export type ScheduleMode = "METADATA_REFRESH" | "FILE_DOWNLOAD_RANGE";

export type ScheduleFormValues = {
  name: string;
  mode: ScheduleMode;
  reportId: string;
  intervalMinutes: number;
  useCache: boolean;
  runAsync: boolean;
  isActive: boolean;
  startDate: string;
  endDate: string;
  rollingWindowDays: number | "";
  templateValuesJson: string;
};

/** Returns a fresh default state object for a new schedule form. */
export function buildDefaultScheduleFormState(): ScheduleFormValues {
  return {
    name: DEFAULT_SCHEDULE_NAME,
    mode: "METADATA_REFRESH",
    reportId: "",
    intervalMinutes: 1440,
    useCache: true,
    runAsync: true,
    isActive: true,
    startDate: "",
    endDate: "",
    rollingWindowDays: "",
    templateValuesJson: DEFAULT_TEMPLATE_VALUES_JSON,
  };
}

/**
 * Maps a loaded `NyisoSchedule` API record to the internal `ScheduleFormValues` shape
 * used by `useFormEngine`. Call this with `replaceValues` when opening an existing schedule
 * for editing so the form is pre-populated with the current record's values.
 */
export function mapScheduleToFormValues(schedule: NyisoSchedule): Partial<ScheduleFormValues> {
  return {
    name: schedule.name,
    mode: schedule.mode,
    reportId: schedule.report_id !== null ? String(schedule.report_id) : "",
    intervalMinutes: schedule.interval_minutes,
    useCache: schedule.use_cache,
    runAsync: schedule.run_async,
    isActive: schedule.is_active,
    startDate: schedule.start_date ?? "",
    endDate: schedule.end_date ?? "",
    rollingWindowDays: schedule.rolling_window_days ?? "",
    templateValuesJson:
      Object.keys(schedule.template_values_json).length > 0
        ? JSON.stringify(schedule.template_values_json, null, 2)
        : DEFAULT_TEMPLATE_VALUES_JSON,
  };
}

/**
 * Factory that produces the NYISO schedule `FormSchema`.
 *
 * Accepts `reportOptions` at runtime (loaded asynchronously from the API) so that the
 * report select field is populated with real data. Call this inside a `useMemo` that
 * depends on the loaded options to avoid re-creating the schema on every render.
 *
 * The schema includes:
 * - Three sections: Schedule Configuration, Date Window, Advanced
 * - Conditional visibility: date window fields are only active in FILE_DOWNLOAD_RANGE mode
 * - Custom `parseValue` and `validations` for numeric and JSON fields
 * - `payloadTransform` that converts form values to the `NyisoScheduleCreatePayload` API shape
 */
export function createScheduleFormSchema(
  reportOptions: SelectOption[]
): FormSchema<ScheduleFormValues, NyisoScheduleCreatePayload> {
  const defaults = buildDefaultScheduleFormState();

  return {
    name: "NyisoSchedule",
    sections: [
      {
        id: "schedule-config",
        label: "Schedule Configuration",
        fields: ["name", "mode", "reportId", "intervalMinutes"],
      },
      {
        id: "window",
        label: "Date Window",
        fields: ["startDate", "endDate", "rollingWindowDays"],
      },
      {
        id: "advanced",
        label: "Advanced",
        fields: ["templateValuesJson", "isActive", "useCache", "runAsync"],
      },
    ],
    fields: [
      {
        key: "name",
        label: "Name",
        type: "text",
        section: "schedule-config",
        required: true,
        defaultValue: defaults.name,
        hint_info: "A descriptive label for this schedule.",
      },
      {
        key: "mode",
        label: "Mode",
        type: "select",
        section: "schedule-config",
        required: true,
        defaultValue: defaults.mode,
        options: [
          { value: "METADATA_REFRESH", label: "Metadata Refresh" },
          { value: "FILE_DOWNLOAD_RANGE", label: "File Download Range" },
        ],
        hint_info:
          "Metadata Refresh updates report listings. File Download Range downloads data files for a date range.",
      },
      {
        key: "reportId",
        label: "Report",
        type: "select",
        section: "schedule-config",
        defaultValue: defaults.reportId,
        options: [{ value: "", label: "All Reports" }, ...reportOptions],
        hint_info: "Scope this schedule to a single report, or leave blank to apply to all reports.",
      },
      {
        key: "intervalMinutes",
        label: "Interval Minutes",
        type: "number",
        section: "schedule-config",
        defaultValue: defaults.intervalMinutes,
        parseValue: (rawValue) => Math.max(1, Number(rawValue || 1)),
        validations: [
          {
            message: "Interval Minutes must be 1 or greater.",
            validate: (value) => Number(value) >= 1,
          },
        ],
        hint_info: "How often this schedule runs. 1440 = daily, 60 = hourly.",
      },
      {
        key: "startDate",
        label: "Start Date",
        type: "date",
        section: "window",
        defaultValue: defaults.startDate,
        hint_info: "Optional range start. Leave blank for open start (uses earliest available report date).",
      },
      {
        key: "endDate",
        label: "End Date",
        type: "date",
        section: "window",
        defaultValue: defaults.endDate,
        hint_info: "Optional range end. Leave blank for open end (uses today).",
      },
      {
        key: "rollingWindowDays",
        label: "Rolling Window Days",
        type: "number",
        section: "window",
        defaultValue: defaults.rollingWindowDays,
        parseValue: (rawValue) => (rawValue === "" ? "" : Math.max(1, Number(rawValue))),
        validations: [
          {
            message: "Rolling Window Days must be blank or 1 or greater.",
            validate: (value) => value === "" || Number(value) >= 1,
          },
        ],
        hint_info: "If set, downloads the last N days on each run, ignoring fixed start/end dates.",
      },
      {
        key: "templateValuesJson",
        label: "Template Values JSON",
        type: "textarea",
        section: "advanced",
        wide: true,
        defaultValue: defaults.templateValuesJson,
        validations: [
          {
            message: "Template Values JSON must be valid JSON.",
            validate: (value) => {
              if (typeof value !== "string") {
                return false;
              }
              if (value.trim().length === 0) {
                return true;
              }
              try {
                JSON.parse(value);
                return true;
              } catch {
                return false;
              }
            },
          },
        ],
        hint_info:
          'Key/value pairs substituted into file URL templates, e.g. {"fileextension": "csv"}.',
      },
      {
        key: "isActive",
        label: "Active",
        type: "boolean",
        section: "advanced",
        defaultValue: defaults.isActive,
        hint_info: "When enabled, the schedule will run automatically on the next interval tick.",
      },
      {
        key: "useCache",
        label: "Use Cache",
        type: "boolean",
        section: "advanced",
        defaultValue: defaults.useCache,
        hint_info: "Skip downloading files that have already been stored in the database.",
      },
      {
        key: "runAsync",
        label: "Run Async",
        type: "boolean",
        section: "advanced",
        defaultValue: defaults.runAsync,
        hint_info: "Dispatch the task as a background Celery job instead of running inline.",
      },
    ],
    payloadTransform: (values) => {
      let parsedTemplateValues: Record<string, string> = {};

      if (values.templateValuesJson.trim()) {
        const parsed = JSON.parse(values.templateValuesJson) as Record<string, unknown>;
        parsedTemplateValues = Object.fromEntries(
          Object.entries(parsed).map(([key, value]) => [String(key), String(value)])
        );
      }

      return {
        name: values.name.trim(),
        mode: values.mode,
        report_id: values.reportId ? Number(values.reportId) : null,
        is_active: values.isActive,
        interval_minutes: Math.max(1, Number(values.intervalMinutes || 1)),
        use_cache: values.useCache,
        run_async: values.runAsync,
        start_date: values.startDate || null,
        end_date: values.endDate || null,
        rolling_window_days:
          values.rollingWindowDays === "" ? null : Math.max(1, Number(values.rollingWindowDays)),
        template_values_json: parsedTemplateValues,
      };
    },
  };
}

export const NYISO_REPORT_LIST_ENDPOINT = "/api/energy_hub/report_list/";
export const NYISO_REPORT_ROWS_ENDPOINT = "/api/energy_hub/report_rows/";
export const NYISO_REPORT_ROW_ENDPOINT = "/api/energy_hub/report_row/";
export const NYISO_REPORT_ROW_CONTENT_ENDPOINT = "/api/energy_hub/report_row/content/";
export const NYISO_REPORT_ROW_REFRESH_ENDPOINT = "/api/energy_hub/report_row/refresh/";
export const NYISO_REPORT_REFRESH_START_ENDPOINT = "/api/energy_hub/report_refresh/start/";
export const NYISO_REPORT_REFRESH_STATUS_ENDPOINT = "/api/energy_hub/report_refresh/status/";

type NyisoTaskStartPayload = {
	queued_report_ids: number[];
	active_report_ids: number[];
	queued_count: number;
	active_count: number;
	cursor: string;
};

type NyisoTaskPollPayload = {
	active_report_ids: number[];
	finished_report_ids: number[];
	active_count: number;
	finished_count: number;
	cursor: string;
};

type NyisoReportRefreshActionPayload = {
	report_id: number;
	task_id: string;
	queued: boolean;
	message: string;
};

async function fetchJson<TPayload>(url: string): Promise<TPayload> {
	const response = await fetch(url);
	if (!response.ok) {
		throw new Error(`Request failed: ${response.status} ${response.statusText}`);
	}
	return (await response.json()) as TPayload;
}

export async function startNyisoReportRefresh(forceReinsert = false): Promise<NyisoTaskStartPayload> {
	const url = new URL(NYISO_REPORT_REFRESH_START_ENDPOINT, window.location.origin);
	if (forceReinsert) {
		url.searchParams.set("force_reinsert", "true");
	}
	return fetchJson<NyisoTaskStartPayload>(url.toString());
}

export async function pollNyisoReportRefreshStatus(cursor?: string): Promise<NyisoTaskPollPayload> {
	const url = new URL(NYISO_REPORT_REFRESH_STATUS_ENDPOINT, window.location.origin);
	if (cursor) {
		url.searchParams.set("since", cursor);
	}
	return fetchJson<NyisoTaskPollPayload>(url.toString());
}

export async function fetchNyisoReportRowsByIds<TRow extends { nyiso_report_id: number }>(
	ids: number[]
): Promise<TRow[]> {
	if (ids.length === 0) {
		return [];
	}

	const uniqueIds = [...new Set(ids)].filter((id) => Number.isInteger(id) && id > 0);
	if (uniqueIds.length === 0) {
		return [];
	}

	const url = new URL(NYISO_REPORT_ROWS_ENDPOINT, window.location.origin);
	url.searchParams.set("ids", uniqueIds.join(","));
	return fetchJson<TRow[]>(url.toString());
}

export async function fetchNyisoReportRowById<TRow>(reportId: number): Promise<TRow> {
	const url = new URL(NYISO_REPORT_ROW_ENDPOINT, window.location.origin);
	url.searchParams.set("report_id", String(reportId));
	return fetchJson<TRow>(url.toString());
}

export async function fetchNyisoReportRowContent<TPayload>(reportId: number): Promise<TPayload> {
	const url = new URL(NYISO_REPORT_ROW_CONTENT_ENDPOINT, window.location.origin);
	url.searchParams.set("report_id", String(reportId));
	return fetchJson<TPayload>(url.toString());
}

export async function refreshNyisoReportRow(reportId: number): Promise<NyisoReportRefreshActionPayload> {
	const url = new URL(NYISO_REPORT_ROW_REFRESH_ENDPOINT, window.location.origin);
	url.searchParams.set("report_id", String(reportId));

	const response = await fetch(url.toString(), { method: "POST" });
	if (!response.ok) {
		throw new Error(`Request failed: ${response.status} ${response.statusText}`);
	}

	return (await response.json()) as NyisoReportRefreshActionPayload;
}

import { useCallback, useEffect, useMemo, useState } from "react";
import axios from "axios";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { http, wsURL } from "./api";

type TunnelSnap = {
  id: string;
  subdomain: string;
  local_port: number;
  connected_at: string;
  request_count: number;
};

type RequestRow = {
  id: string;
  tunnel_id: string;
  method: string;
  path: string;
  status_code?: number | null;
  latency_ms?: number | null;
  request_at: string;
};

type RequestDetail = RequestRow & {
  request_headers?: unknown;
  request_body?: string | null;
  response_headers?: unknown;
  response_body?: string | null;
};

type VolumePoint = { minute: string; count: number };

type WsEvent = { type: string; payload: Record<string, unknown> };

function formatDuration(fromIso: string) {
  const t = Date.parse(fromIso);
  if (Number.isNaN(t)) return "—";
  const s = Math.floor((Date.now() - t) / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  return `${h}h`;
}

function statusColor(code?: number | null) {
  if (code == null) return "text-slate-400";
  if (code >= 500) return "text-rose-400";
  if (code >= 400) return "text-amber-400";
  return "text-emerald-400";
}

export default function App() {
  const [active, setActive] = useState<TunnelSnap[]>([]);
  const [requests, setRequests] = useState<RequestRow[]>([]);
  const [volume, setVolume] = useState<VolumePoint[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<RequestDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refreshAll = useCallback(async () => {
    try {
      setError(null);
      const [a, r, v] = await Promise.all([
        http.get<TunnelSnap[]>("/api/tunnels/active"),
        http.get<RequestRow[]>("/api/requests"),
        http.get<VolumePoint[]>("/api/stats/volume"),
      ]);
      setActive(a.data);
      setRequests(r.data);
      setVolume(
        v.data.map((p) => ({
          minute: p.minute,
          count: p.count,
        })),
      );
    } catch (e) {
      if (axios.isAxiosError(e)) {
        setError(e.message);
      } else {
        setError(String(e));
      }
    }
  }, []);

  useEffect(() => {
    void refreshAll();
  }, [refreshAll]);

  useEffect(() => {
    const url = wsURL("/api/ws");
    const ws = new WebSocket(url);
    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(String(ev.data)) as WsEvent;
        if (msg.type === "tunnel_up") {
          const p = msg.payload as TunnelSnap & { public_url?: string };
          setActive((cur) => {
            const next = cur.filter((t) => t.id !== p.id);
            next.push({
              id: p.id,
              subdomain: p.subdomain,
              local_port: p.local_port,
              connected_at: p.connected_at,
              request_count: 0,
            });
            return next;
          });
        } else if (msg.type === "tunnel_down") {
          const p = msg.payload as { id: string };
          setActive((cur) => cur.filter((t) => t.id !== p.id));
        } else if (msg.type === "request") {
          const p = msg.payload as {
            id: string;
            tunnel_id: string;
            subdomain: string;
            method: string;
            path: string;
            status_code: number;
            latency_ms: number;
            request_at: string;
          };
          setRequests((cur) => {
            const row: RequestRow = {
              id: p.id,
              tunnel_id: p.tunnel_id,
              method: p.method,
              path: p.path,
              status_code: p.status_code,
              latency_ms: p.latency_ms,
              request_at: p.request_at,
            };
            return [row, ...cur].slice(0, 200);
          });
          setActive((cur) =>
            cur.map((t) =>
              t.id === p.tunnel_id
                ? { ...t, request_count: t.request_count + 1 }
                : t,
            ),
          );
          void http.get<VolumePoint[]>("/api/stats/volume").then((res) => {
            setVolume(
              res.data.map((x) => ({ minute: x.minute, count: x.count })),
            );
          });
        }
      } catch {
        /* ignore */
      }
    };
    ws.onclose = () => {
      /* optional: reconnect */
    };
    return () => ws.close();
  }, []);

  useEffect(() => {
    if (!selectedId) {
      setDetail(null);
      return;
    }
    setDetailLoading(true);
    http
      .get<RequestDetail>(`/api/requests/${selectedId}`)
      .then((res) => setDetail(res.data))
      .catch(() => setDetail(null))
      .finally(() => setDetailLoading(false));
  }, [selectedId]);

  const chartData = useMemo(
    () =>
      volume.map((p) => ({
        label: new Date(p.minute).toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
        }),
        count: Number(p.count),
      })),
    [volume],
  );

  return (
    <div className="min-h-screen">
      <header className="border-b border-slate-800 bg-slate-900/80 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
          <div>
            <h1 className="text-lg font-semibold tracking-tight">Horizon</h1>
            <p className="text-sm text-slate-400">
              Live tunnels, traffic, and request inspector
            </p>
          </div>
          <button
            type="button"
            onClick={() => void refreshAll()}
            className="rounded-lg bg-slate-800 px-3 py-1.5 text-sm font-medium text-slate-200 hover:bg-slate-700"
          >
            Refresh
          </button>
        </div>
      </header>

      {error && (
        <div className="mx-auto max-w-7xl px-6 pt-4 text-sm text-rose-400">
          {error}
        </div>
      )}

      <main className="mx-auto grid max-w-7xl gap-6 px-6 py-6 lg:grid-cols-2">
        <section className="rounded-xl border border-slate-800 bg-slate-900/40 p-4 shadow-lg shadow-black/20">
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-400">
            Active tunnels
          </h2>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-sm">
              <thead>
                <tr className="text-slate-500">
                  <th className="pb-2 pr-4 font-medium">Subdomain</th>
                  <th className="pb-2 pr-4 font-medium">Port</th>
                  <th className="pb-2 pr-4 font-medium">Active</th>
                  <th className="pb-2 font-medium">Requests</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {active.length === 0 && (
                  <tr>
                    <td colSpan={4} className="py-6 text-slate-500">
                      No active tunnels
                    </td>
                  </tr>
                )}
                {active.map((t) => (
                  <tr key={t.id} className="text-slate-200">
                    <td className="py-2 pr-4 font-mono text-cyan-300">
                      {t.subdomain}
                    </td>
                    <td className="py-2 pr-4">{t.local_port}</td>
                    <td className="py-2 pr-4 text-slate-400">
                      {formatDuration(t.connected_at)}
                    </td>
                    <td className="py-2">{t.request_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded-xl border border-slate-800 bg-slate-900/40 p-4 shadow-lg shadow-black/20">
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-400">
            Request volume (last 30 min)
          </h2>
          <div className="h-56 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis dataKey="label" stroke="#64748b" fontSize={11} />
                <YAxis stroke="#64748b" fontSize={11} allowDecimals={false} />
                <Tooltip
                  contentStyle={{
                    background: "#0f172a",
                    border: "1px solid #1e293b",
                    borderRadius: 8,
                  }}
                />
                <Line
                  type="monotone"
                  dataKey="count"
                  stroke="#22d3ee"
                  strokeWidth={2}
                  dot={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </section>

        <section className="rounded-xl border border-slate-800 bg-slate-900/40 p-4 shadow-lg shadow-black/20 lg:col-span-2">
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-400">
            Request log
          </h2>
          <div className="max-h-72 overflow-auto rounded-lg border border-slate-800">
            <table className="w-full text-left text-sm">
              <thead className="sticky top-0 bg-slate-900">
                <tr className="text-slate-500">
                  <th className="px-3 py-2 font-medium">Time</th>
                  <th className="px-3 py-2 font-medium">Method</th>
                  <th className="px-3 py-2 font-medium">Path</th>
                  <th className="px-3 py-2 font-medium">Status</th>
                  <th className="px-3 py-2 font-medium">Latency</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800">
                {requests.length === 0 && (
                  <tr>
                    <td colSpan={5} className="px-3 py-6 text-slate-500">
                      No requests yet
                    </td>
                  </tr>
                )}
                {requests.map((r) => (
                  <tr
                    key={r.id}
                    onClick={() => setSelectedId(r.id)}
                    className={`cursor-pointer hover:bg-slate-800/60 ${
                      selectedId === r.id ? "bg-slate-800/80" : ""
                    }`}
                  >
                    <td className="whitespace-nowrap px-3 py-2 text-slate-400">
                      {new Date(r.request_at).toLocaleTimeString()}
                    </td>
                    <td className="px-3 py-2 font-mono text-slate-200">
                      {r.method}
                    </td>
                    <td className="max-w-md truncate px-3 py-2 font-mono text-slate-300">
                      {r.path}
                    </td>
                    <td
                      className={`px-3 py-2 font-mono ${statusColor(r.status_code)}`}
                    >
                      {r.status_code ?? "—"}
                    </td>
                    <td className="px-3 py-2 text-slate-400">
                      {r.latency_ms != null ? `${r.latency_ms} ms` : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded-xl border border-slate-800 bg-slate-900/40 p-4 shadow-lg shadow-black/20 lg:col-span-2">
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-400">
            Request inspector
          </h2>
          {!selectedId && (
            <p className="text-sm text-slate-500">
              Select a request from the log to inspect headers and body.
            </p>
          )}
          {selectedId && detailLoading && (
            <p className="text-sm text-slate-400">Loading…</p>
          )}
          {selectedId && !detailLoading && detail && (
            <div className="grid gap-4 md:grid-cols-2">
              <div>
                <h3 className="mb-2 text-xs font-semibold uppercase text-slate-500">
                  Request headers
                </h3>
                <pre className="max-h-48 overflow-auto rounded-lg bg-slate-950 p-3 text-xs text-slate-300">
                  {JSON.stringify(detail.request_headers ?? {}, null, 2)}
                </pre>
                <h3 className="mb-2 mt-4 text-xs font-semibold uppercase text-slate-500">
                  Request body
                </h3>
                <pre className="max-h-48 overflow-auto rounded-lg bg-slate-950 p-3 text-xs text-slate-300">
                  {detail.request_body ?? "—"}
                </pre>
              </div>
              <div>
                <h3 className="mb-2 text-xs font-semibold uppercase text-slate-500">
                  Response headers
                </h3>
                <pre className="max-h-48 overflow-auto rounded-lg bg-slate-950 p-3 text-xs text-slate-300">
                  {JSON.stringify(detail.response_headers ?? {}, null, 2)}
                </pre>
                <h3 className="mb-2 mt-4 text-xs font-semibold uppercase text-slate-500">
                  Response body
                </h3>
                <pre className="max-h-48 overflow-auto rounded-lg bg-slate-950 p-3 text-xs text-slate-300">
                  {detail.response_body ?? "—"}
                </pre>
              </div>
            </div>
          )}
        </section>
      </main>
    </div>
  );
}

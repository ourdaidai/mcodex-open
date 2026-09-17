import { useEffect, useMemo, useRef, useState } from "react";

import {
  appendMetricSample,
  parsePrometheusText,
  selectCoordinationPerformance,
  selectHealthPerformance,
  selectHttpPerformance,
  selectMaintenancePerformance,
  selectSqlitePerformance,
  type MetricSample,
} from "./metrics";

type PerformanceViewProps = {
  fetchMetrics: () => Promise<string>;
  rawMetricsUrl?: string;
  visible: boolean;
};

type ScrapeState = "loading" | "healthy" | "error";
type InFlightScrape = {
  fetcher: () => Promise<string>;
  promise: Promise<string>;
};

function formatNumber(value: number | null, digits = 1): string {
  if (value === null) return "N/A";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: digits }).format(value);
}

function formatRate(value: number | null): string {
  return value === null ? "N/A" : `${formatNumber(value, 2)}/s`;
}

function formatPercent(value: number | null): string {
  return value === null ? "N/A" : `${formatNumber(value * 100, 1)}%`;
}

function formatDuration(value: number | null): string {
  return value === null ? "N/A" : `${formatNumber(value, 1)} ms`;
}

function formatBytes(value: number | null): string {
  if (value === null) return "N/A";
  if (value < 1024) return `${formatNumber(value, 0)} B`;
  if (value < 1024 ** 2) return `${formatNumber(value / 1024)} KiB`;
  if (value < 1024 ** 3) return `${formatNumber(value / 1024 ** 2)} MiB`;
  return `${formatNumber(value / 1024 ** 3)} GiB`;
}

function formatBytesRate(value: number | null): string {
  return value === null ? "N/A" : `${formatBytes(value)}/s`;
}

function formatUptime(value: number | null): string {
  if (value === null) return "N/A";
  const seconds = Math.max(0, Math.floor(value));
  const days = Math.floor(seconds / 86_400);
  const hours = Math.floor((seconds % 86_400) / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  const remainder = seconds % 60;
  return [days ? `${days}d` : null, hours ? `${hours}h` : null, minutes ? `${minutes}m` : null, `${remainder}s`]
    .filter(Boolean)
    .join(" ");
}

function formatTimestamp(value: number | null): string {
  if (value === null) return "N/A";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value * 1_000));
}

function formatCapturedAt(value: number | null): string {
  if (value === null) return "No successful scrape yet";
  const time = new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
  return `Last successful scrape ${time}`;
}

function MetricList({ entries }: { entries: Array<{ label: string; testId?: string; value: string }> }) {
  return (
    <dl className="performance-summary-grid">
      {entries.map((entry) => (
        <div className="performance-stat" key={entry.label}>
          <dt>{entry.label}</dt>
          <dd data-testid={entry.testId}>{entry.value}</dd>
        </div>
      ))}
    </dl>
  );
}

function EmptyTableRow({ columns }: { columns: number }) {
  return <tr><td className="performance-table-empty" colSpan={columns}>N/A</td></tr>;
}

export function PerformanceView({ fetchMetrics, rawMetricsUrl = "/metrics", visible }: PerformanceViewProps) {
  const [samples, setSamples] = useState<MetricSample[]>([]);
  const [scrapeState, setScrapeState] = useState<ScrapeState>("loading");
  const [lastSuccessfulScrape, setLastSuccessfulScrape] = useState<number | null>(null);
  const inFlightScrape = useRef<InFlightScrape | null>(null);

  useEffect(() => {
    if (!visible) return;
    let cancelled = false;
    let nextScrape: number | undefined;

    const startScrape = (): InFlightScrape => {
      let request: Promise<string>;
      try {
        request = fetchMetrics();
      } catch (error) {
        request = Promise.reject(error);
      }
      const entry = { fetcher: fetchMetrics, promise: request };
      inFlightScrape.current = entry;
      const clearRequest = () => {
        if (inFlightScrape.current === entry) inFlightScrape.current = null;
      };
      void request.then(clearRequest, clearRequest);
      return entry;
    };

    const currentScrape = async (): Promise<string | null> => {
      while (!cancelled) {
        const entry = inFlightScrape.current;
        if (entry === null) return startScrape().promise;
        if (entry.fetcher === fetchMetrics) return entry.promise;
        try {
          await entry.promise;
        } catch {
          // A changed fetcher discards the previous generation's result or error.
        }
      }
      return null;
    };

    const scrape = async () => {
      try {
        const text = await currentScrape();
        if (cancelled || text === null) return;
        const capturedAt = Date.now();
        setSamples((current) => appendMetricSample(current, {
          capturedAt,
          series: parsePrometheusText(text),
        }));
        setLastSuccessfulScrape(capturedAt);
        setScrapeState("healthy");
      } catch {
        if (!cancelled) setScrapeState("error");
      } finally {
        if (!cancelled) nextScrape = window.setTimeout(scrape, 5_000);
      }
    };

    void scrape();
    return () => {
      cancelled = true;
      if (nextScrape !== undefined) window.clearTimeout(nextScrape);
    };
  }, [fetchMetrics, visible]);

  const health = useMemo(() => selectHealthPerformance(samples), [samples]);
  const http = useMemo(() => selectHttpPerformance(samples), [samples]);
  const sqlite = useMemo(() => selectSqlitePerformance(samples), [samples]);
  const coordination = useMemo(() => selectCoordinationPerformance(samples), [samples]);
  const maintenance = useMemo(() => selectMaintenancePerformance(samples), [samples]);
  const scrapeLabel = scrapeState === "healthy"
    ? "Scrape healthy"
    : scrapeState === "error" ? "Scrape failed" : "Waiting for scrape";

  return (
    <main className="performance-shell" hidden={!visible}>
      <header className="performance-header">
        <div>
          <p className="kicker">Local telemetry</p>
          <h2>Performance</h2>
          <p className="header-note">{formatCapturedAt(lastSuccessfulScrape)}</p>
        </div>
      </header>

      <section aria-labelledby="performance-health-heading" className="performance-band">
        <div className="performance-band-heading">
          <h3 id="performance-health-heading">Health</h3>
          <div className="performance-band-actions">
            <span className={`performance-health-state ${scrapeState}`}>{scrapeLabel}</span>
            <a className="performance-raw-link" href={rawMetricsUrl} rel="noreferrer" target="_blank">Raw metrics</a>
          </div>
        </div>
        <MetricList entries={[
          { label: "Process uptime", value: formatUptime(health.uptimeSeconds) },
          { label: "SQLite journal", value: health.journalMode?.toUpperCase() ?? "N/A" },
          { label: "Database size", testId: "db-size", value: formatBytes(health.dbSizeBytes) },
          { label: "WAL size", value: formatBytes(health.walSizeBytes) },
          { label: "Metrics scrapes", value: formatNumber(health.scrapeTotal, 0) },
          { label: "Scrape failures", value: formatNumber(health.scrapeFailures, 0) },
        ]} />
      </section>

      <section aria-labelledby="performance-http-heading" className="performance-band">
        <div className="performance-band-heading"><h3 id="performance-http-heading">HTTP</h3></div>
        <MetricList entries={[
          { label: "Request rate", testId: "http-request-rate", value: formatRate(http.requestRate) },
          { label: "Error rate", value: formatPercent(http.errorRate) },
          { label: "Active requests", value: formatNumber(http.activeRequests, 0) },
          { label: "Response size p95", value: formatBytes(http.responseSizeP95Bytes) },
        ]} />
        <div className="performance-table-wrap">
          <table className="performance-table">
            <caption>Route performance</caption>
            <thead><tr><th>Route</th><th>Requests</th><th>Errors</th><th>p50</th><th>p95</th></tr></thead>
            <tbody>
              {http.routes.map((route) => (
                <tr key={route.route}>
                  <th className="performance-name-cell" scope="row">{route.route}</th>
                  <td>{formatRate(route.requestRate)}</td>
                  <td>{formatPercent(route.errorRate)}</td>
                  <td>{formatDuration(route.p50Ms)}</td>
                  <td>{formatDuration(route.p95Ms)}</td>
                </tr>
              ))}
              {http.routes.length === 0 ? <EmptyTableRow columns={5} /> : null}
            </tbody>
          </table>
        </div>
      </section>

      <section aria-labelledby="performance-sqlite-heading" className="performance-band">
        <div className="performance-band-heading"><h3 id="performance-sqlite-heading">SQLite</h3></div>
        <MetricList entries={[
          { label: "Lock timeout rate", value: formatRate(sqlite.lockTimeoutRate) },
          { label: "Lock wait p95", value: formatDuration(sqlite.lockWaitP95Ms) },
          { label: "Transaction rate", value: formatRate(sqlite.transactionRate) },
        ]} />
        <div className="performance-table-wrap">
          <table className="performance-table">
            <caption>Slow operations</caption>
            <thead><tr><th>Operation</th><th>Operations</th><th>p50</th><th>p95</th></tr></thead>
            <tbody>
              {sqlite.operations.map((operation) => (
                <tr key={operation.operation}>
                  <th className="performance-name-cell" scope="row">{operation.operation}</th>
                  <td>{formatRate(operation.rate)}</td>
                  <td>{formatDuration(operation.p50Ms)}</td>
                  <td>{formatDuration(operation.p95Ms)}</td>
                </tr>
              ))}
              {sqlite.operations.length === 0 ? <EmptyTableRow columns={4} /> : null}
            </tbody>
          </table>
        </div>
      </section>

      <section aria-labelledby="performance-coordination-heading" className="performance-band">
        <div className="performance-band-heading"><h3 id="performance-coordination-heading">Coordination</h3></div>
        <MetricList entries={[
          { label: "Heartbeats received", value: formatRate(coordination.heartbeatReceivedRate) },
          { label: "Events recorded", value: formatRate(coordination.heartbeatRecordedRate) },
          { label: "Events suppressed", value: formatRate(coordination.heartbeatSuppressedRate) },
          { label: "Pending deliveries", value: formatNumber(coordination.pendingDeliveries, 0) },
          { label: "Claimed deliveries", value: formatNumber(coordination.claimedDeliveries, 0) },
          { label: "Delivery p95", value: formatDuration(coordination.deliveryP95Ms) },
          { label: "SSE connections", value: formatNumber(coordination.sseConnections, 0) },
          { label: "SSE queue depth", value: formatNumber(coordination.sseQueueDepth, 0) },
          { label: "SSE drop rate", value: formatRate(coordination.sseDropRate) },
        ]} />
        <div className="performance-meter-row">
          <span>Heartbeat sampling ratio</span>
          <meter aria-label="Heartbeat sampling ratio" max={1} min={0} value={coordination.samplingRatio ?? 0} />
          <strong>{formatPercent(coordination.samplingRatio)}</strong>
        </div>
      </section>

      <section aria-labelledby="performance-maintenance-heading" className="performance-band">
        <div className="performance-band-heading"><h3 id="performance-maintenance-heading">Maintenance</h3></div>
        <div className="performance-table-wrap">
          <table className="performance-table">
            <caption>Maintenance tasks</caption>
            <thead><tr><th>Task</th><th>Last success</th><th>Duration p95</th><th>Records</th><th>Failures</th></tr></thead>
            <tbody>{maintenance.tasks.map((task) => (
              <tr key={task.task}>
                <th className="performance-name-cell" scope="row">{task.task}</th>
                <td>{formatTimestamp(task.lastSuccessSeconds)}</td>
                <td>{formatDuration(task.durationP95Ms)}</td>
                <td>{formatRate(task.recordsRate)}</td>
                <td>{formatRate(task.failureRate)}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
        <div className="performance-table-wrap">
          <table className="performance-table">
            <caption>Archives</caption>
            <thead><tr><th>Kind</th><th>Last success</th><th>Records</th><th>Raw bytes</th><th>Compressed</th><th>Failures</th></tr></thead>
            <tbody>{maintenance.archives.map((archive) => (
              <tr key={archive.kind}>
                <th className="performance-name-cell" scope="row">{archive.kind}</th>
                <td>{formatTimestamp(archive.lastSuccessSeconds)}</td>
                <td>{formatRate(archive.recordsRate)}</td>
                <td>{formatBytesRate(archive.uncompressedBytesRate)}</td>
                <td>{formatBytesRate(archive.compressedBytesRate)}</td>
                <td>{formatRate(archive.failureRate)}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      </section>
    </main>
  );
}

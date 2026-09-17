export type MetricLabels = Record<string, string>;

export type MetricPoint = {
  labels: MetricLabels;
  name: string;
  value: number;
};

export type MetricSample = {
  capturedAt: number;
  series: MetricPoint[];
};

const SAMPLE_LIMIT = 720;
const METRIC_NAME_RE = /^[a-zA-Z_:][a-zA-Z0-9_:]*/;
const LABEL_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_]*/;
const VALUE_RE = /^[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?$|^[-+]?Inf$|^NaN$/;
const FINITE_BOUNDARY_RE = /^[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?$/;
const MONOTONIC_NAME_RE = /(?:_total|_count|_bucket)$/;

function parseLabels(source: string): MetricLabels | null {
  const labels: MetricLabels = {};
  let offset = 0;

  const skipWhitespace = () => {
    while (/\s/.test(source[offset] ?? "")) offset += 1;
  };

  skipWhitespace();
  if (offset === source.length) return labels;

  while (offset < source.length) {
    const nameMatch = LABEL_NAME_RE.exec(source.slice(offset));
    if (!nameMatch) return null;
    const name = nameMatch[0];
    if (Object.prototype.hasOwnProperty.call(labels, name)) return null;
    offset += name.length;
    skipWhitespace();
    if (source[offset] !== "=") return null;
    offset += 1;
    skipWhitespace();
    if (source[offset] !== '"') return null;
    offset += 1;

    let value = "";
    let closed = false;
    while (offset < source.length) {
      const character = source[offset];
      offset += 1;
      if (character === '"') {
        closed = true;
        break;
      }
      if (character === "\n" || character === "\r") return null;
      if (character !== "\\") {
        value += character;
        continue;
      }
      const escaped = source[offset];
      offset += 1;
      if (escaped === "n") value += "\n";
      else if (escaped === '"') value += '"';
      else if (escaped === "\\") value += "\\";
      else return null;
    }
    if (!closed) return null;
    Object.defineProperty(labels, name, {
      configurable: true,
      enumerable: true,
      value,
      writable: true,
    });

    skipWhitespace();
    if (offset === source.length) return labels;
    if (source[offset] !== ",") return null;
    offset += 1;
    skipWhitespace();
    if (offset === source.length) return null;
  }

  return labels;
}

function parseSampleLine(line: string): MetricPoint | null {
  const nameMatch = METRIC_NAME_RE.exec(line);
  if (!nameMatch) return null;
  const name = nameMatch[0];
  let offset = name.length;
  let labels: MetricLabels = {};

  if (line[offset] === "{") {
    const labelsStart = offset + 1;
    offset = labelsStart;
    let escaped = false;
    let quoted = false;
    while (offset < line.length) {
      const character = line[offset];
      if (escaped) escaped = false;
      else if (character === "\\" && quoted) escaped = true;
      else if (character === '"') quoted = !quoted;
      else if (character === "}" && !quoted) break;
      offset += 1;
    }
    if (offset >= line.length || line[offset] !== "}") return null;
    const parsedLabels = parseLabels(line.slice(labelsStart, offset));
    if (parsedLabels === null) return null;
    labels = parsedLabels;
    offset += 1;
  }

  const separator = line.slice(offset).match(/^\s+/);
  if (!separator) return null;
  offset += separator[0].length;
  const valueText = line.slice(offset);
  if (!VALUE_RE.test(valueText)) return null;
  const value = Number(valueText);
  if (!Number.isFinite(value)) return null;
  return { name, labels, value };
}

export function parsePrometheusText(text: string): MetricPoint[] {
  const points: MetricPoint[] = [];
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const point = parseSampleLine(line);
    if (point !== null) points.push(point);
  }
  return points;
}

function labelsMatch(actual: MetricLabels, expected: MetricLabels): boolean {
  return Object.entries(expected).every(([name, value]) => actual[name] === value);
}

export function metricValue(
  points: MetricPoint[],
  name: string,
  labels: MetricLabels = {},
): number | null {
  let matched = false;
  let total = 0;
  for (const point of points) {
    if (point.name !== name || !labelsMatch(point.labels, labels)) continue;
    matched = true;
    total += point.value;
  }
  return matched ? total : null;
}

function seriesKey(point: MetricPoint): string {
  return JSON.stringify([point.name, Object.entries(point.labels).sort(([left], [right]) => left.localeCompare(right))]);
}

function monotonicValues(points: MetricPoint[]): Map<string, number> {
  const values = new Map<string, number>();
  for (const point of points) {
    if (!MONOTONIC_NAME_RE.test(point.name)) continue;
    const key = seriesKey(point);
    values.set(key, (values.get(key) ?? 0) + point.value);
  }
  return values;
}

function monotonicReset(samples: MetricSample[], current: MetricPoint[]): boolean {
  const currentValues = monotonicValues(current);
  const unresolved = new Set(currentValues.keys());
  for (let index = samples.length - 1; index >= 0 && unresolved.size > 0; index -= 1) {
    const previousValues = monotonicValues(samples[index].series);
    for (const key of unresolved) {
      const previousValue = previousValues.get(key);
      if (previousValue === undefined) continue;
      if (currentValues.get(key)! < previousValue) return true;
      unresolved.delete(key);
    }
  }
  return false;
}

export function appendMetricSample(
  samples: MetricSample[],
  next: MetricSample,
): MetricSample[] {
  const previous = samples[samples.length - 1];
  if (previous && next.capturedAt <= previous.capturedAt) return [next];
  if (monotonicReset(samples, next.series)) return [next];
  return [...samples, next].slice(-SAMPLE_LIMIT);
}

export function rateBetween(
  previous: MetricSample,
  current: MetricSample,
  name: string,
  labels: MetricLabels = {},
): number | null {
  const elapsedSeconds = (current.capturedAt - previous.capturedAt) / 1_000;
  if (elapsedSeconds <= 0) return null;
  const previousValue = metricValue(previous.series, name, labels);
  const currentValue = metricValue(current.series, name, labels);
  if (previousValue === null || currentValue === null) return null;
  return Math.max(0, currentValue - previousValue) / elapsedSeconds;
}

type HistogramBuckets = Map<number, number>;

function histogramBuckets(
  points: MetricPoint[],
  name: string,
  labels: MetricLabels,
): HistogramBuckets | null {
  const buckets: HistogramBuckets = new Map();
  let matched = false;
  for (const point of points) {
    if (point.name !== name || !labelsMatch(point.labels, labels)) continue;
    matched = true;
    const boundaryText = point.labels.le;
    if (boundaryText !== "+Inf" && !FINITE_BOUNDARY_RE.test(boundaryText ?? "")) return null;
    const boundary = boundaryText === "+Inf" ? Number.POSITIVE_INFINITY : Number(boundaryText);
    if (boundaryText === undefined || Number.isNaN(boundary) || boundary === Number.NEGATIVE_INFINITY) {
      return null;
    }
    buckets.set(boundary, (buckets.get(boundary) ?? 0) + point.value);
  }
  return matched ? buckets : null;
}

function sortedBoundaries(buckets: HistogramBuckets): number[] {
  return [...buckets.keys()].sort((left, right) => left - right);
}

function isCumulative(buckets: HistogramBuckets, boundaries: number[]): boolean {
  let previous = 0;
  for (const boundary of boundaries) {
    const value = buckets.get(boundary);
    if (value === undefined || value < previous) return false;
    previous = value;
  }
  return true;
}

export function histogramQuantileDelta(
  previous: MetricSample,
  current: MetricSample,
  baseName: string,
  labels: MetricLabels,
  quantile: number,
): number | null {
  if (current.capturedAt <= previous.capturedAt) return null;
  if (!Number.isFinite(quantile) || quantile < 0 || quantile > 1) return null;
  const bucketName = `${baseName}_bucket`;
  const previousBuckets = histogramBuckets(previous.series, bucketName, labels);
  const currentBuckets = histogramBuckets(current.series, bucketName, labels);
  if (previousBuckets === null || currentBuckets === null) return null;

  const boundaries = sortedBoundaries(previousBuckets);
  if (
    boundaries.length !== currentBuckets.size ||
    !boundaries.includes(Number.POSITIVE_INFINITY) ||
    !boundaries.some(Number.isFinite) ||
    boundaries.some((boundary) => !currentBuckets.has(boundary)) ||
    !isCumulative(previousBuckets, boundaries) ||
    !isCumulative(currentBuckets, boundaries)
  ) {
    return null;
  }

  const deltas = new Map<number, number>();
  let previousDelta = 0;
  for (const boundary of boundaries) {
    const delta = currentBuckets.get(boundary)! - previousBuckets.get(boundary)!;
    if (delta < 0 || delta < previousDelta) return null;
    deltas.set(boundary, delta);
    previousDelta = delta;
  }

  const observations = deltas.get(Number.POSITIVE_INFINITY);
  if (observations === undefined || observations <= 0) return null;
  const target = quantile * observations;
  const finiteBoundaries = boundaries.filter(Number.isFinite);
  const largestFiniteBoundary = finiteBoundaries[finiteBoundaries.length - 1];
  if (largestFiniteBoundary === undefined) return null;
  for (const boundary of boundaries) {
    if (deltas.get(boundary)! < target) continue;
    return Number.isFinite(boundary) ? boundary : largestFiniteBoundary;
  }
  return null;
}

export type RoutePerformance = {
  errorRate: number;
  p50Ms: number | null;
  p95Ms: number | null;
  requestRate: number;
  route: string;
};

export type OperationPerformance = {
  operation: string;
  p50Ms: number | null;
  p95Ms: number | null;
  rate: number;
};

export type HealthPerformance = {
  scrapeFailures: number | null;
  dbSizeBytes: number | null;
  journalMode: string | null;
  scrapeTotal: number | null;
  uptimeSeconds: number | null;
  walSizeBytes: number | null;
};

export type HttpPerformance = {
  activeRequests: number | null;
  errorRate: number | null;
  requestRate: number | null;
  responseSizeP95Bytes: number | null;
  routes: RoutePerformance[];
};

export type SqlitePerformance = {
  lockTimeoutRate: number | null;
  lockWaitP95Ms: number | null;
  operations: OperationPerformance[];
  transactionRate: number | null;
};

export type CoordinationPerformance = {
  claimedDeliveries: number | null;
  deliveryP95Ms: number | null;
  heartbeatReceivedRate: number | null;
  heartbeatRecordedRate: number | null;
  heartbeatSuppressedRate: number | null;
  pendingDeliveries: number | null;
  samplingRatio: number | null;
  sseConnections: number | null;
  sseDropRate: number | null;
  sseQueueDepth: number | null;
};

export type MaintenanceTaskPerformance = {
  durationP95Ms: number | null;
  failureRate: number | null;
  lastSuccessSeconds: number | null;
  recordsRate: number | null;
  task: string;
};

export type ArchivePerformance = {
  compressedBytesRate: number | null;
  failureRate: number | null;
  kind: string;
  lastSuccessSeconds: number | null;
  recordsRate: number | null;
  uncompressedBytesRate: number | null;
};

export type MaintenancePerformance = {
  archives: ArchivePerformance[];
  tasks: MaintenanceTaskPerformance[];
};

const MAINTENANCE_TASKS = [
  "presence_expiry",
  "claim_expiry",
  "heartbeat_retention",
  "message_archive",
] as const;
const ARCHIVE_KINDS = ["messages", "pane_summaries"] as const;

function lastSample(samples: MetricSample[]): MetricSample | null {
  return samples[samples.length - 1] ?? null;
}

function lastTwoSamples(samples: MetricSample[]): [MetricSample, MetricSample] | null {
  if (samples.length < 2) return null;
  return [samples[samples.length - 2], samples[samples.length - 1]];
}

function withoutMetricsRoute(sample: MetricSample): MetricSample {
  return {
    capturedAt: sample.capturedAt,
    series: sample.series.filter((point) => point.labels.route !== "/metrics"),
  };
}

function rateFromLastTwo(
  samples: MetricSample[],
  name: string,
  labels: MetricLabels = {},
): number | null {
  const pair = lastTwoSamples(samples);
  return pair ? rateBetween(pair[0], pair[1], name, labels) : null;
}

function quantileFromLastTwo(
  samples: MetricSample[],
  name: string,
  labels: MetricLabels,
  quantile: number,
): number | null {
  const pair = lastTwoSamples(samples);
  return pair ? histogramQuantileDelta(pair[0], pair[1], name, labels, quantile) : null;
}

function distinctLabels(points: MetricPoint[], name: string, label: string): string[] {
  return [...new Set(
    points
      .filter((point) => point.name === name)
      .map((point) => point.labels[label])
      .filter((value): value is string => value !== undefined),
  )].sort((left, right) => left.localeCompare(right));
}

export function selectHealthPerformance(samples: MetricSample[]): HealthPerformance {
  const latest = lastSample(samples);
  const series = latest?.series ?? [];
  const journalMode = series.find(
    (point) => point.name === "mcodex_db_journal_mode" && point.value > 0 && point.labels.mode,
  )?.labels.mode ?? null;
  return {
    scrapeFailures: metricValue(series, "mcodex_metrics_scrape_failures_total"),
    dbSizeBytes: metricValue(series, "mcodex_db_file_size_bytes"),
    journalMode,
    scrapeTotal: metricValue(series, "mcodex_metrics_scrapes_total"),
    uptimeSeconds: metricValue(series, "mcodex_process_uptime_seconds"),
    walSizeBytes: metricValue(series, "mcodex_db_wal_size_bytes"),
  };
}

export function selectHttpPerformance(samples: MetricSample[]): HttpPerformance {
  const filteredSamples = samples.map(withoutMetricsRoute);
  const latest = lastSample(filteredSamples);
  const pair = lastTwoSamples(filteredSamples);
  const requestRate = rateFromLastTwo(filteredSamples, "mcodex_http_server_requests_total");
  const errorRequestRate = rateFromLastTwo(filteredSamples, "mcodex_http_server_errors_total");
  const routes = pair && latest
    ? distinctLabels(latest.series, "mcodex_http_server_requests_total", "route")
      .map((route): RoutePerformance | null => {
        const routeRequestRate = rateBetween(pair[0], pair[1], "mcodex_http_server_requests_total", { route });
        if (routeRequestRate === null) return null;
        const routeErrorRate = rateBetween(pair[0], pair[1], "mcodex_http_server_errors_total", { route });
        return {
          errorRate: routeRequestRate > 0 ? (routeErrorRate ?? 0) / routeRequestRate : 0,
          p50Ms: histogramQuantileDelta(
            pair[0], pair[1], "mcodex_http_server_duration_milliseconds", { route }, 0.5,
          ),
          p95Ms: histogramQuantileDelta(
            pair[0], pair[1], "mcodex_http_server_duration_milliseconds", { route }, 0.95,
          ),
          requestRate: routeRequestRate,
          route,
        };
      })
      .filter((route): route is RoutePerformance => route !== null)
    : [];
  return {
    activeRequests: latest ? metricValue(latest.series, "mcodex_http_server_active_requests") : null,
    errorRate: requestRate !== null && requestRate > 0 && errorRequestRate !== null
      ? errorRequestRate / requestRate
      : requestRate === 0 && errorRequestRate === 0 ? 0 : null,
    requestRate,
    responseSizeP95Bytes: quantileFromLastTwo(
      filteredSamples,
      "mcodex_http_server_response_size_bytes",
      {},
      0.95,
    ),
    routes,
  };
}

export function selectSqlitePerformance(samples: MetricSample[]): SqlitePerformance {
  const latest = lastSample(samples);
  const pair = lastTwoSamples(samples);
  const operations = pair && latest
    ? distinctLabels(latest.series, "mcodex_db_operation_duration_milliseconds_count", "operation")
      .map((operation): OperationPerformance | null => {
        const rate = rateBetween(
          pair[0], pair[1], "mcodex_db_operation_duration_milliseconds_count", { operation },
        );
        if (rate === null) return null;
        return {
          operation,
          p50Ms: histogramQuantileDelta(
            pair[0], pair[1], "mcodex_db_operation_duration_milliseconds", { operation }, 0.5,
          ),
          p95Ms: histogramQuantileDelta(
            pair[0], pair[1], "mcodex_db_operation_duration_milliseconds", { operation }, 0.95,
          ),
          rate,
        };
      })
      .filter((operation): operation is OperationPerformance => operation !== null)
      .sort((left, right) => (right.p95Ms ?? -1) - (left.p95Ms ?? -1))
    : [];
  return {
    lockTimeoutRate: rateFromLastTwo(samples, "mcodex_db_lock_timeouts_total"),
    lockWaitP95Ms: quantileFromLastTwo(
      samples,
      "mcodex_db_lock_wait_duration_milliseconds",
      {},
      0.95,
    ),
    operations,
    transactionRate: rateFromLastTwo(samples, "mcodex_db_transactions_total"),
  };
}

export function selectCoordinationPerformance(samples: MetricSample[]): CoordinationPerformance {
  const latest = lastSample(samples);
  const series = latest?.series ?? [];
  const heartbeatReceivedRate = rateFromLastTwo(samples, "mcodex_heartbeat_received_total");
  const heartbeatRecordedRate = rateFromLastTwo(samples, "mcodex_heartbeat_events_recorded_total");
  const heartbeatSuppressedRate = rateFromLastTwo(samples, "mcodex_heartbeat_events_suppressed_total");
  return {
    claimedDeliveries: latest ? metricValue(series, "mcodex_messages_claimed") : null,
    deliveryP95Ms: quantileFromLastTwo(
      samples,
      "mcodex_message_delivery_duration_milliseconds",
      {},
      0.95,
    ),
    heartbeatReceivedRate,
    heartbeatRecordedRate,
    heartbeatSuppressedRate,
    pendingDeliveries: latest ? metricValue(series, "mcodex_messages_pending") : null,
    samplingRatio: heartbeatReceivedRate !== null && heartbeatReceivedRate > 0 && heartbeatRecordedRate !== null
      ? heartbeatRecordedRate / heartbeatReceivedRate
      : heartbeatReceivedRate === 0 && heartbeatRecordedRate === 0 ? 0 : null,
    sseConnections: latest ? metricValue(series, "mcodex_sse_connections") : null,
    sseDropRate: rateFromLastTwo(samples, "mcodex_sse_events_dropped_total"),
    sseQueueDepth: latest ? metricValue(series, "mcodex_sse_queue_depth") : null,
  };
}

export function selectMaintenancePerformance(samples: MetricSample[]): MaintenancePerformance {
  const latest = lastSample(samples);
  const series = latest?.series ?? [];
  return {
    tasks: MAINTENANCE_TASKS.map((task) => ({
      durationP95Ms: quantileFromLastTwo(
        samples,
        "mcodex_maintenance_duration_milliseconds",
        { task },
        0.95,
      ),
      failureRate: rateFromLastTwo(samples, "mcodex_maintenance_failures_total", { task }),
      lastSuccessSeconds: latest
        ? metricValue(series, "mcodex_maintenance_last_success_seconds", { task })
        : null,
      recordsRate: rateFromLastTwo(samples, "mcodex_maintenance_records_processed_total", { task }),
      task,
    })),
    archives: ARCHIVE_KINDS.map((kind) => ({
      compressedBytesRate: rateFromLastTwo(samples, "mcodex_archive_compressed_bytes_total", { kind }),
      failureRate: rateFromLastTwo(samples, "mcodex_archive_failures_total", { kind }),
      kind,
      lastSuccessSeconds: latest
        ? metricValue(series, "mcodex_archive_last_success_seconds", { kind })
        : null,
      recordsRate: rateFromLastTwo(samples, "mcodex_archive_records_total", { kind }),
      uncompressedBytesRate: rateFromLastTwo(samples, "mcodex_archive_uncompressed_bytes_total", { kind }),
    })),
  };
}

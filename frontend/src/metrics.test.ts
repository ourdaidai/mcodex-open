import { describe, expect, it } from "vitest";
import {
  appendMetricSample,
  histogramQuantileDelta,
  metricValue,
  parsePrometheusText,
  rateBetween,
  selectCoordinationPerformance,
  selectHttpPerformance,
  selectMaintenancePerformance,
  selectSqlitePerformance,
  type MetricSample,
} from "./metrics";

const scrape = `
# TYPE mcodex_http_server_requests_total counter
mcodex_http_server_requests_total{method="GET",route="/api/groups",status_class="2xx",outcome="ok"} 42
mcodex_http_server_requests_total{method="POST",route="/api/groups",status_class="2xx",outcome="ok"} 8
mcodex_http_server_duration_milliseconds_bucket{method="GET",route="/api/groups",le="5"} 4
mcodex_http_server_duration_milliseconds_bucket{method="GET",route="/api/groups",le="10"} 8
mcodex_http_server_duration_milliseconds_bucket{method="GET",route="/api/groups",le="+Inf"} 10
mcodex_http_server_active_requests{method="GET",route="/api/groups"} 1
`;

function sample(capturedAt: number, text: string): MetricSample {
  return { capturedAt, series: parsePrometheusText(text) };
}

describe("Prometheus text parsing", () => {
  it("parses counters, gauges, labels, and histogram buckets", () => {
    const parsed = parsePrometheusText(scrape);
    expect(
      metricValue(parsed, "mcodex_http_server_requests_total", {
        method: "GET",
        route: "/api/groups",
      }),
    ).toBe(42);
    expect(
      metricValue(parsed, "mcodex_http_server_active_requests", { method: "GET" }),
    ).toBe(1);
    expect(
      metricValue(parsed, "mcodex_http_server_duration_milliseconds_bucket", {
        route: "/api/groups",
        le: "10",
      }),
    ).toBe(8);
  });

  it("aggregates every series matching a label subset", () => {
    expect(
      metricValue(parsePrometheusText(scrape), "mcodex_http_server_requests_total", {
        route: "/api/groups",
      }),
    ).toBe(50);
  });

  it("unescapes the three Prometheus label escape sequences", () => {
    const parsed = parsePrometheusText(
      String.raw`escaped_total{message="line\nquote\"slash\\",route="/api/groups"} 1`,
    );
    expect(parsed).toEqual([
      {
        name: "escaped_total",
        labels: { message: "line\nquote\"slash\\", route: "/api/groups" },
        value: 1,
      },
    ]);
  });

  it("parses __proto__ as an ordinary label and rejects duplicates", () => {
    const parsed = parsePrometheusText(`
proto_total{__proto__="first"} 1
proto_total{__proto__="first",__proto__="second"} 2
`);
    expect(parsed).toEqual([
      {
        name: "proto_total",
        labels: { ["__proto__"]: "first" },
        value: 1,
      },
    ]);
  });

  it("ignores malformed labels, unsupported escapes, and non-finite values", () => {
    const parsed = parsePrometheusText(String.raw`
valid_total{route="/ok"} 7
valid_total{route="/ok",broken} 8
valid_total{route="bad\tvalue"} 9
valid_total{route="duplicate",route="again"} 10
valid_total{route="/ok"} NaN
valid_total{route="/ok"} +Inf
not valid
`);
    expect(parsed).toEqual([{ name: "valid_total", labels: { route: "/ok" }, value: 7 }]);
  });

  it("returns null when no series matches", () => {
    expect(metricValue(parsePrometheusText(scrape), "missing_total", {})).toBeNull();
    expect(
      metricValue(parsePrometheusText(scrape), "mcodex_http_server_requests_total", {
        route: "/missing",
      }),
    ).toBeNull();
  });
});

describe("performance dashboard selectors", () => {
  const previous = sample(0, `
mcodex_http_server_requests_total{method="GET",route="/api/groups",status_class="2xx",outcome="ok"} 100
mcodex_http_server_requests_total{method="GET",route="/metrics",status_class="2xx",outcome="ok"} 200
mcodex_http_server_errors_total{method="GET",route="/api/groups",status_class="5xx",outcome="server_error"} 2
mcodex_http_server_duration_milliseconds_bucket{method="GET",route="/api/groups",status_class="2xx",outcome="ok",le="5"} 50
mcodex_http_server_duration_milliseconds_bucket{method="GET",route="/api/groups",status_class="2xx",outcome="ok",le="10"} 100
mcodex_http_server_duration_milliseconds_bucket{method="GET",route="/api/groups",status_class="2xx",outcome="ok",le="+Inf"} 100
mcodex_http_server_duration_milliseconds_count{method="GET",route="/api/groups",status_class="2xx",outcome="ok"} 100
mcodex_http_server_response_size_bytes_bucket{method="GET",route="/api/groups",status_class="2xx",outcome="ok",le="100"} 100
mcodex_http_server_response_size_bytes_bucket{method="GET",route="/api/groups",status_class="2xx",outcome="ok",le="+Inf"} 100
mcodex_db_operation_duration_milliseconds_bucket{operation="groups.list",outcome="ok",le="5"} 10
mcodex_db_operation_duration_milliseconds_bucket{operation="groups.list",outcome="ok",le="+Inf"} 10
mcodex_db_operation_duration_milliseconds_count{operation="groups.list",outcome="ok"} 10
mcodex_db_transactions_total{operation="messages.create",outcome="ok"} 10
mcodex_db_lock_timeouts_total{operation="messages.create"} 1
mcodex_db_lock_wait_duration_milliseconds_bucket{operation="messages.create",le="5"} 10
mcodex_db_lock_wait_duration_milliseconds_bucket{operation="messages.create",le="+Inf"} 10
mcodex_heartbeat_received_total 100
mcodex_heartbeat_events_recorded_total{record_reason="sample"} 20
mcodex_heartbeat_events_suppressed_total 80
mcodex_sse_events_dropped_total{event_type="agent_updated"} 1
mcodex_message_delivery_duration_milliseconds_bucket{state="acked",le="10"} 10
mcodex_message_delivery_duration_milliseconds_bucket{state="acked",le="+Inf"} 10
mcodex_maintenance_records_processed_total{task="presence_expiry"} 10
mcodex_maintenance_failures_total{task="presence_expiry"} 1
mcodex_maintenance_duration_milliseconds_bucket{task="presence_expiry",outcome="ok",le="5"} 1
mcodex_maintenance_duration_milliseconds_bucket{task="presence_expiry",outcome="ok",le="+Inf"} 1
mcodex_archive_records_total{kind="messages"} 10
mcodex_archive_uncompressed_bytes_total{kind="messages"} 1000
mcodex_archive_compressed_bytes_total{kind="messages"} 500
mcodex_archive_failures_total{kind="messages"} 1
`);
  const current = sample(5_000, `
mcodex_http_server_requests_total{method="GET",route="/api/groups",status_class="2xx",outcome="ok"} 110
mcodex_http_server_requests_total{method="GET",route="/metrics",status_class="2xx",outcome="ok"} 300
mcodex_http_server_errors_total{method="GET",route="/api/groups",status_class="5xx",outcome="server_error"} 3
mcodex_http_server_active_requests{method="GET",route="/api/groups"} 0
mcodex_http_server_duration_milliseconds_bucket{method="GET",route="/api/groups",status_class="2xx",outcome="ok",le="5"} 54
mcodex_http_server_duration_milliseconds_bucket{method="GET",route="/api/groups",status_class="2xx",outcome="ok",le="10"} 110
mcodex_http_server_duration_milliseconds_bucket{method="GET",route="/api/groups",status_class="2xx",outcome="ok",le="+Inf"} 110
mcodex_http_server_duration_milliseconds_count{method="GET",route="/api/groups",status_class="2xx",outcome="ok"} 110
mcodex_http_server_response_size_bytes_bucket{method="GET",route="/api/groups",status_class="2xx",outcome="ok",le="100"} 110
mcodex_http_server_response_size_bytes_bucket{method="GET",route="/api/groups",status_class="2xx",outcome="ok",le="+Inf"} 110
mcodex_db_operation_duration_milliseconds_bucket{operation="groups.list",outcome="ok",le="5"} 12
mcodex_db_operation_duration_milliseconds_bucket{operation="groups.list",outcome="ok",le="+Inf"} 12
mcodex_db_operation_duration_milliseconds_count{operation="groups.list",outcome="ok"} 12
mcodex_db_transactions_total{operation="messages.create",outcome="ok"} 15
mcodex_db_lock_timeouts_total{operation="messages.create"} 1
mcodex_db_lock_wait_duration_milliseconds_bucket{operation="messages.create",le="5"} 15
mcodex_db_lock_wait_duration_milliseconds_bucket{operation="messages.create",le="+Inf"} 15
mcodex_heartbeat_received_total 110
mcodex_heartbeat_events_recorded_total{record_reason="sample"} 22
mcodex_heartbeat_events_suppressed_total 88
mcodex_messages_pending 0
mcodex_messages_claimed 2
mcodex_sse_connections 1
mcodex_sse_queue_depth 0
mcodex_sse_events_dropped_total{event_type="agent_updated"} 1
mcodex_message_delivery_duration_milliseconds_bucket{state="acked",le="10"} 15
mcodex_message_delivery_duration_milliseconds_bucket{state="acked",le="+Inf"} 15
mcodex_maintenance_last_success_seconds{task="presence_expiry"} 1700000000
mcodex_maintenance_records_processed_total{task="presence_expiry"} 15
mcodex_maintenance_failures_total{task="presence_expiry"} 1
mcodex_maintenance_duration_milliseconds_bucket{task="presence_expiry",outcome="ok",le="5"} 2
mcodex_maintenance_duration_milliseconds_bucket{task="presence_expiry",outcome="ok",le="+Inf"} 2
mcodex_archive_last_success_seconds{kind="messages"} 1700000000
mcodex_archive_records_total{kind="messages"} 15
mcodex_archive_uncompressed_bytes_total{kind="messages"} 1500
mcodex_archive_compressed_bytes_total{kind="messages"} 700
mcodex_archive_failures_total{kind="messages"} 1
`);

  it("excludes the metrics route and aggregates route latency from the last two samples", () => {
    const summary = selectHttpPerformance([previous, current]);
    expect(summary.requestRate).toBe(2);
    expect(summary.errorRate).toBeCloseTo(0.1);
    expect(summary.activeRequests).toBe(0);
    expect(summary.responseSizeP95Bytes).toBe(100);
    expect(summary.routes).toEqual([{ route: "/api/groups", requestRate: 2, errorRate: 0.1, p50Ms: 10, p95Ms: 10 }]);
  });

  it("selects SQLite operation, lock, and transaction performance", () => {
    const summary = selectSqlitePerformance([previous, current]);
    expect(summary.lockTimeoutRate).toBe(0);
    expect(summary.lockWaitP95Ms).toBe(5);
    expect(summary.transactionRate).toBe(1);
    expect(summary.operations).toEqual([{ operation: "groups.list", rate: 0.4, p50Ms: 5, p95Ms: 5 }]);
  });

  it("keeps zero gauges distinct from missing series in coordination metrics", () => {
    const summary = selectCoordinationPerformance([previous, current]);
    expect(summary.heartbeatReceivedRate).toBe(2);
    expect(summary.samplingRatio).toBe(0.2);
    expect(summary.pendingDeliveries).toBe(0);
    expect(summary.claimedDeliveries).toBe(2);
    expect(summary.sseQueueDepth).toBe(0);
    expect(summary.sseDropRate).toBe(0);
    expect(selectCoordinationPerformance([current]).heartbeatReceivedRate).toBeNull();
  });

  it("selects every fixed maintenance task and archive kind without inventing missing values", () => {
    const summary = selectMaintenancePerformance([previous, current]);
    expect(summary.tasks.find((task) => task.task === "presence_expiry")).toMatchObject({
      durationP95Ms: 5,
      failureRate: 0,
      lastSuccessSeconds: 1700000000,
      recordsRate: 1,
    });
    expect(summary.tasks.find((task) => task.task === "claim_expiry")?.lastSuccessSeconds).toBeNull();
    expect(summary.archives.find((archive) => archive.kind === "messages")).toMatchObject({
      compressedBytesRate: 40,
      failureRate: 0,
      recordsRate: 1,
      uncompressedBytesRate: 100,
    });
  });

  it("returns no rates after appendMetricSample resets the history", () => {
    const reset = sample(
      10_000,
      "mcodex_http_server_requests_total{method=\"GET\",route=\"/api/groups\",status_class=\"2xx\",outcome=\"ok\"} 1",
    );
    const history = appendMetricSample([previous, current], reset);
    expect(history).toEqual([reset]);
    expect(selectHttpPerformance(history).requestRate).toBeNull();
  });
});

describe("rolling metric samples", () => {
  it("retains only the newest 720 five-second samples", () => {
    let samples: MetricSample[] = [];
    for (let index = 0; index < 725; index += 1) {
      samples = appendMetricSample(samples, sample(index * 5_000, `gauge ${index}`));
    }
    expect(samples).toHaveLength(720);
    expect(samples[0].capturedAt).toBe(25_000);
  });

  it("clears all prior history after any matching monotonic series resets", () => {
    const first = sample(
      0,
      `counter_total{method="GET",route="/api/groups"} 10\ncounter_total{method="POST",route="/api/groups"} 20`,
    );
    const reset = sample(
      5_000,
      `counter_total{route="/api/groups",method="GET"} 11\ncounter_total{route="/api/groups",method="POST"} 2`,
    );
    expect(appendMetricSample([first], reset)).toEqual([reset]);
  });

  it("does not treat gauges or absent monotonic series as resets", () => {
    const first = sample(0, "queue_depth 10\nrequests_total 20");
    const next = sample(5_000, "queue_depth 2");
    expect(appendMetricSample([first], next)).toEqual([first, next]);
  });

  it("detects a reset against the most recent matching series across a scrape gap", () => {
    const first = sample(0, "requests_total 100");
    const missing = sample(5_000, "queue_depth 2");
    const reset = sample(10_000, "requests_total 2");
    expect(appendMetricSample([first, missing], reset)).toEqual([reset]);
  });

  it("keeps history when a monotonic series returns at a higher value", () => {
    const first = sample(0, "requests_total 100");
    const missing = sample(5_000, "queue_depth 2");
    const current = sample(10_000, "requests_total 102");
    expect(appendMetricSample([first, missing], current)).toEqual([first, missing, current]);
  });

  it("restarts history when sample timestamps do not increase", () => {
    const first = sample(5_000, "queue_depth 1");
    const sameTime = sample(5_000, "queue_depth 2");
    const older = sample(4_000, "queue_depth 3");
    expect(appendMetricSample([first], sameTime)).toEqual([sameTime]);
    expect(appendMetricSample([first], older)).toEqual([older]);
  });
});

describe("counter rates", () => {
  it("computes a per-second aggregate rate", () => {
    const previous = sample(
      0,
      `requests_total{method="GET",route="/api/groups"} 100\nrequests_total{method="POST",route="/api/groups"} 20`,
    );
    const current = sample(
      5_000,
      `requests_total{method="GET",route="/api/groups"} 125\nrequests_total{method="POST",route="/api/groups"} 25`,
    );
    expect(rateBetween(previous, current, "requests_total", { route: "/api/groups" })).toBe(6);
  });

  it("returns zero after a counter reset", () => {
    expect(
      rateBetween(sample(0, "requests_total 10"), sample(5_000, "requests_total 2"), "requests_total"),
    ).toBe(0);
  });

  it("returns null for a missing snapshot series or non-positive time interval", () => {
    expect(
      rateBetween(sample(0, "requests_total 1"), sample(5_000, "other_total 2"), "requests_total"),
    ).toBeNull();
    expect(
      rateBetween(sample(5_000, "requests_total 1"), sample(5_000, "requests_total 2"), "requests_total"),
    ).toBeNull();
    expect(
      rateBetween(sample(10_000, "requests_total 1"), sample(5_000, "requests_total 2"), "requests_total"),
    ).toBeNull();
  });
});

describe("histogram interval quantiles", () => {
  const previous = sample(
    0,
    `
duration_bucket{route="/api/groups",le="5"} 10
duration_bucket{route="/api/groups",le="10"} 20
duration_bucket{route="/api/groups",le="+Inf"} 20
`,
  );
  const current = sample(
    5_000,
    `
duration_bucket{route="/api/groups",le="5"} 12
duration_bucket{route="/api/groups",le="10"} 28
duration_bucket{route="/api/groups",le="+Inf"} 30
`,
  );

  it("computes quantiles from cumulative bucket deltas", () => {
    expect(histogramQuantileDelta(previous, current, "duration", { route: "/api/groups" }, 0.5)).toBe(10);
    expect(histogramQuantileDelta(previous, current, "duration", { route: "/api/groups" }, 0.95)).toBe(10);
  });

  it("aggregates matching bucket series before computing a quantile", () => {
    const before = sample(
      0,
      `duration_bucket{method="GET",le="5"} 1\nduration_bucket{method="GET",le="+Inf"} 1\nduration_bucket{method="POST",le="5"} 2\nduration_bucket{method="POST",le="+Inf"} 2`,
    );
    const after = sample(
      5_000,
      `duration_bucket{method="GET",le="5"} 2\nduration_bucket{method="GET",le="+Inf"} 3\nduration_bucket{method="POST",le="5"} 4\nduration_bucket{method="POST",le="+Inf"} 5`,
    );
    expect(histogramQuantileDelta(before, after, "duration", {}, 0.5)).toBe(5);
  });

  it("returns null for missing, reset, empty, or invalid bucket snapshots", () => {
    expect(histogramQuantileDelta(previous, sample(5_000, "other_bucket{le=\"+Inf\"} 1"), "duration", {}, 0.5)).toBeNull();
    expect(histogramQuantileDelta(current, previous, "duration", {}, 0.5)).toBeNull();
    expect(histogramQuantileDelta(previous, previous, "duration", {}, 0.5)).toBeNull();

    const missingInfinity = sample(5_000, "duration_bucket{le=\"5\"} 12\nduration_bucket{le=\"10\"} 28");
    expect(histogramQuantileDelta(previous, missingInfinity, "duration", {}, 0.5)).toBeNull();

    const mismatched = sample(5_000, "duration_bucket{le=\"5\"} 12\nduration_bucket{le=\"20\"} 28\nduration_bucket{le=\"+Inf\"} 30");
    expect(histogramQuantileDelta(previous, mismatched, "duration", {}, 0.5)).toBeNull();

    const nonCumulativeDelta = sample(5_000, "duration_bucket{le=\"5\"} 15\nduration_bucket{le=\"10\"} 23\nduration_bucket{le=\"+Inf\"} 30");
    expect(histogramQuantileDelta(previous, nonCumulativeDelta, "duration", {}, 0.5)).toBeNull();

    expect(histogramQuantileDelta(previous, current, "duration", {}, Number.NaN)).toBeNull();
    expect(histogramQuantileDelta(previous, current, "duration", {}, 1.1)).toBeNull();
  });

  it("rejects empty and non-numeric bucket boundaries", () => {
    const before = sample(
      0,
      `duration_bucket{le=""} 1\nduration_bucket{le="+Inf"} 1`,
    );
    const after = sample(
      5_000,
      `duration_bucket{le=""} 2\nduration_bucket{le="+Inf"} 2`,
    );
    expect(histogramQuantileDelta(before, after, "duration", {}, 0.5)).toBeNull();
  });

  it("returns null when histogram snapshot timestamps do not increase", () => {
    expect(
      histogramQuantileDelta(previous, { ...current, capturedAt: 0 }, "duration", {}, 0.5),
    ).toBeNull();
    expect(
      histogramQuantileDelta(previous, { ...current, capturedAt: -5_000 }, "duration", {}, 0.5),
    ).toBeNull();
  });
});

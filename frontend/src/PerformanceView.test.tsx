import { StrictMode } from "react";
import { act, cleanup, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { PerformanceView } from "./PerformanceView";

const healthyScrape = `
mcodex_process_uptime_seconds 60
mcodex_metrics_scrapes_total 2
mcodex_db_journal_mode{mode="wal"} 1
mcodex_db_file_size_bytes 0
mcodex_db_wal_size_bytes 0
mcodex_http_server_requests_total{method="GET",route="/api/groups",status_class="2xx",outcome="ok"} 10
mcodex_http_server_active_requests{method="GET",route="/api/groups"} 0
mcodex_messages_pending 0
mcodex_messages_claimed 0
mcodex_sse_connections 0
mcodex_sse_queue_depth 0
`;

function deferred<T>() {
  let resolve: ((value: T) => void) | undefined;
  let reject: ((reason: Error) => void) | undefined;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return {
    promise,
    reject: (reason: Error) => reject?.(reason),
    resolve: (value: T) => resolve?.(value),
  };
}

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("PerformanceView", () => {
  it("does not scrape when initially hidden", async () => {
    const fetchMetrics = vi.fn().mockResolvedValue(healthyScrape);
    render(<PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible={false} />);
    await act(async () => Promise.resolve());
    expect(fetchMetrics).not.toHaveBeenCalled();
  });

  it("scrapes immediately and every five seconds only while visible", async () => {
    vi.useFakeTimers();
    const fetchMetrics = vi.fn().mockResolvedValue(healthyScrape);
    const { rerender } = render(
      <PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible />,
    );
    await act(async () => Promise.resolve());
    expect(fetchMetrics).toHaveBeenCalledTimes(1);

    await act(async () => vi.advanceTimersByTimeAsync(10_000));
    expect(fetchMetrics).toHaveBeenCalledTimes(3);

    rerender(
      <PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible={false} />,
    );
    await act(async () => vi.advanceTimersByTimeAsync(10_000));
    expect(fetchMetrics).toHaveBeenCalledTimes(3);

    rerender(
      <PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible />,
    );
    await act(async () => Promise.resolve());
    expect(fetchMetrics).toHaveBeenCalledTimes(4);
  });

  it("never overlaps scrapes and ignores a completion after becoming hidden", async () => {
    vi.useFakeTimers();
    let resolveFirst: ((value: string) => void) | undefined;
    const fetchMetrics = vi.fn(() => new Promise<string>((resolve) => {
      resolveFirst = resolve;
    }));
    const { rerender } = render(
      <PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible />,
    );

    await act(async () => vi.advanceTimersByTimeAsync(15_000));
    expect(fetchMetrics).toHaveBeenCalledTimes(1);
    rerender(
      <PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible={false} />,
    );
    await act(async () => {
      resolveFirst?.(healthyScrape);
      await Promise.resolve();
    });
    expect(screen.queryByText("Scrape healthy")).toBeNull();
  });

  it("shares a pending scrape across a rapid hide and show", async () => {
    let resolveScrape: ((value: string) => void) | undefined;
    const fetchMetrics = vi.fn(() => new Promise<string>((resolve) => {
      resolveScrape = resolve;
    }));
    const { rerender } = render(
      <PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible />,
    );

    rerender(<PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible={false} />);
    rerender(<PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible />);
    expect(fetchMetrics).toHaveBeenCalledTimes(1);

    await act(async () => {
      resolveScrape?.(healthyScrape);
      await Promise.resolve();
    });
    expect(screen.getByText("Scrape healthy")).toBeTruthy();
  });

  it("shares the initial pending scrape across React StrictMode effect probes", async () => {
    let resolveScrape: ((value: string) => void) | undefined;
    const fetchMetrics = vi.fn(() => new Promise<string>((resolve) => {
      resolveScrape = resolve;
    }));
    render(
      <StrictMode>
        <PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible />
      </StrictMode>,
    );
    expect(fetchMetrics).toHaveBeenCalledTimes(1);

    await act(async () => {
      resolveScrape?.(healthyScrape);
      await Promise.resolve();
    });
    expect(screen.getByText("Scrape healthy")).toBeTruthy();
  });

  for (const previousOutcome of ["resolve", "reject"] as const) {
    it(`serializes a changed fetcher after the previous one ${previousOutcome}s`, async () => {
      const first = deferred<string>();
      const second = deferred<string>();
      let active = 0;
      let maxActive = 0;
      const track = (request: Promise<string>) => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        return request.finally(() => {
          active -= 1;
        });
      };
      const fetchFirst = vi.fn(() => track(first.promise));
      const fetchSecond = vi.fn(() => track(second.promise));
      const { rerender } = render(
        <PerformanceView fetchMetrics={fetchFirst} rawMetricsUrl="/metrics" visible />,
      );

      rerender(<PerformanceView fetchMetrics={fetchSecond} rawMetricsUrl="/metrics" visible />);
      expect(fetchFirst).toHaveBeenCalledTimes(1);
      expect(fetchSecond).not.toHaveBeenCalled();
      expect(maxActive).toBe(1);

      await act(async () => {
        if (previousOutcome === "resolve") first.resolve(healthyScrape);
        else first.reject(new Error("old fetcher failed"));
        await Promise.resolve();
        await Promise.resolve();
        await Promise.resolve();
      });
      expect(fetchSecond).toHaveBeenCalledTimes(1);
      expect(maxActive).toBe(1);
      expect(screen.queryByText("Scrape healthy")).toBeNull();
      expect(screen.queryByText("Scrape failed")).toBeNull();
      expect(screen.queryByText("1m 0s")).toBeNull();

      await act(async () => {
        second.resolve(healthyScrape.replace("uptime_seconds 60", "uptime_seconds 90"));
        await Promise.resolve();
      });
      expect(screen.getByText("Scrape healthy")).toBeTruthy();
      expect(screen.getByText("1m 30s")).toBeTruthy();
      expect(maxActive).toBe(1);
    });
  }

  it("preserves the last sample across a failure and recovers on the next tick", async () => {
    vi.useFakeTimers();
    const fetchMetrics = vi.fn()
      .mockResolvedValueOnce(healthyScrape)
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce(healthyScrape.replace("uptime_seconds 60", "uptime_seconds 70"));
    render(<PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible />);

    await act(async () => Promise.resolve());
    expect(screen.getByText("Scrape healthy")).toBeTruthy();
    expect(screen.getByText("1m 0s")).toBeTruthy();
    await act(async () => vi.advanceTimersByTimeAsync(5_000));
    expect(screen.getByText("Scrape failed")).toBeTruthy();
    expect(screen.getByText("1m 0s")).toBeTruthy();
    await act(async () => vi.advanceTimersByTimeAsync(5_000));
    expect(screen.getByText("Scrape healthy")).toBeTruthy();
    expect(screen.getByText("1m 10s")).toBeTruthy();
  });

  it("keeps a failed state while a hide-show retry is pending or rejected", async () => {
    vi.useFakeTimers();
    let rejectRetry: ((reason: Error) => void) | undefined;
    const fetchMetrics = vi.fn()
      .mockResolvedValueOnce(healthyScrape)
      .mockRejectedValueOnce(new Error("offline"))
      .mockImplementationOnce(() => new Promise<string>((_resolve, reject) => {
        rejectRetry = reject;
      }))
      .mockResolvedValueOnce(healthyScrape.replace("uptime_seconds 60", "uptime_seconds 80"));
    const { rerender } = render(
      <PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible />,
    );
    await act(async () => Promise.resolve());
    await act(async () => vi.advanceTimersByTimeAsync(5_000));
    expect(screen.getByText("Scrape failed")).toBeTruthy();

    rerender(<PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible={false} />);
    rerender(<PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible />);
    expect(fetchMetrics).toHaveBeenCalledTimes(3);
    expect(screen.getByText("Scrape failed")).toBeTruthy();

    await act(async () => {
      rejectRetry?.(new Error("still offline"));
      await Promise.resolve();
    });
    expect(screen.getByText("Scrape failed")).toBeTruthy();
    await act(async () => vi.advanceTimersByTimeAsync(5_000));
    expect(screen.getByText("Scrape healthy")).toBeTruthy();
    expect(screen.getByText("1m 20s")).toBeTruthy();
  });

  it("renders five semantic bands, a raw link, and distinct zero and missing values", async () => {
    const fetchMetrics = vi.fn().mockResolvedValue(healthyScrape);
    render(<PerformanceView fetchMetrics={fetchMetrics} rawMetricsUrl="/metrics" visible />);

    expect(await screen.findByText("Scrape healthy")).toBeTruthy();
    for (const name of ["Health", "HTTP", "SQLite", "Coordination", "Maintenance"]) {
      expect(screen.getByRole("region", { name })).toBeTruthy();
    }
    const healthBand = screen.getByRole("region", { name: "Health" });
    expect(within(healthBand).getByRole("link", { name: "Raw metrics" }).getAttribute("href")).toBe("/metrics");
    expect(screen.getAllByRole("link", { name: "Raw metrics" })).toHaveLength(1);
    expect(screen.getByTestId("db-size").textContent).toBe("0 B");
    expect(screen.getByTestId("http-request-rate").textContent).toBe("N/A");
  });
});

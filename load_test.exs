defmodule LoadTest.DummyWorker do
  @moduledoc """
  The worker implementation for the load test.
  """
  def handle_work({:work, duration_ms}, _from, state) do
    Process.sleep(duration_ms)
    {:reply, {:ok, duration_ms}, state}
  end
end

defmodule LoadTest do
  def run(qps, duration, work_duration_ms \\ 100, jitter \\ 0.0) do
    # 1. Instantiate the pool locally with our specific handler
    IO.puts "--- Initializing ElasticPool for Load Test ---"
    {:ok, _pid} = ElasticPool.start_link(
      name: LTPool,
      worker_handler: LoadTest.DummyWorker,
      max_workers: 8,
      baseline_workers: 2,
      scale_threshold: 10
    )

    IO.puts "\n--- Starting Load Test: #{qps} QPS for #{duration}s (Work: #{work_duration_ms}ms) ---"
    total = qps * duration
    interval = div(1_000_000, qps)
    parent = self()

    spawn_link(fn ->
      start = System.monotonic_time(:microsecond)
      Enum.each(1..total, fn i ->
        scheduled_time = start + (i * interval)
        max_jitter = round(interval * jitter)
        jitter_val = if max_jitter > 0, do: Enum.random(-max_jitter..max_jitter), else: 0
        target = scheduled_time + jitter_val
        now = System.monotonic_time(:microsecond)
        if target > now, do: Process.sleep(div(target - now, 1000))

        spawn(fn ->
          s = System.monotonic_time(:microsecond)
          res = ElasticPool.call(LTPool, {:work, work_duration_ms})
          send(parent, {:res, res, System.monotonic_time(:microsecond) - s})
        end)
      end)
    end)

    collect(total, [], 0)
  end

  defp collect(total, results, count) do
    if rem(count, max(1, div(total, 10))) == 0 do
      IO.write("\rProgress: #{count}/#{total}")
    end

    if count < total do
      receive do
        {:res, r, l} -> collect(total, [{r, l} | results], count + 1)
      after 60_000 -> finish(results, total)
      end
    else
      finish(results, total)
    end
  end

  defp finish(results, total) do
    status = ElasticPool.status(LTPool)
    process(results, total, status)
  end

  defp process(results, total, status) do
    actual = length(results)
    lats = Enum.map(results, fn {_, l} -> l end)
    if actual > 0 do
      avg = Enum.sum(lats) / actual / 1000
      p95 = Enum.sort(lats) |> Enum.at(max(0, round(actual * 0.95) - 1)) |> Kernel./(1000)
      IO.puts "\n\nSuccess: #{actual}/#{total}"
      IO.puts "Total Workers: #{status.total_workers}"
      IO.puts "Peak Workers: #{status.peak_workers}"
      IO.puts "Available Workers: #{status.available_workers}"
      IO.puts "Waiting Clients: #{status.waiting_clients}"
      IO.puts "Avg: #{Float.round(avg, 2)}ms"
      IO.puts "P95: #{Float.round(p95, 2)}ms"
    end
  end
end

# To run: mix run -r load_test.exs -e "LoadTest.run(200, 5, 100)"

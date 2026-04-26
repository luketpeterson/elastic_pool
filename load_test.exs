defmodule LoadTest do
  def run(qps, duration, jitter \\ 0.0) do
    IO.puts "\n--- Starting Load Test: #{qps} QPS for #{duration}s (Jitter: #{jitter}) ---"
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
          res = PrologBridge.query("fact(#{Enum.random(1..5_000_000)}, X)")
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
    status = PrologBridge.status()
    process(results, total, status.peak_workers)
  end

  defp process(results, total, peak) do
    actual = length(results)
    lats = Enum.map(results, fn {_, l} -> l end)
    if actual > 0 do
      avg = Enum.sum(lats) / actual / 1000
      p95 = Enum.at(Enum.sort(lats), round(actual * 0.95) - 1) / 1000
      IO.puts "\n\nSuccess: #{actual}/#{total}"
      IO.puts "Peak Processes: #{peak}"
      IO.puts "Avg: #{Float.round(avg, 2)}ms"
      IO.puts "P95: #{Float.round(p95, 2)}ms"
    end
  end
end

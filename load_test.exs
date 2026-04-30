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
  @moduledoc """
  A stochastic load generator for ElasticPool.

  The generator uses a Gamma distribution to determine inter-arrival times,
  allowing for realistic traffic patterns ranging from bursty to regular.

  ## Running the test
  From the project root:
      mix run -r load_test.exs -e "LoadTest.run(qps, duration, work_ms, shape)"

  ## Arguments
    * `qps` - Average Queries Per Second.
    * `duration` - Total test duration in seconds.
    * `work_duration_ms` - (default 100) The time each worker simulates work.
    * `shape` - (default 1.0) The regularity of traffic.
      * `1.0`: Natural (Exponential) arrivals. Bursty with gaps.
      * `> 1.0`: More regular/steady traffic.
      * `< 1.0`: Highly bursty/clustered traffic.
  """

  def run(qps, duration, work_duration_ms \\ 100, shape \\ 1.0) do
    IO.puts "--- Initializing ElasticPool for Load Test ---"
    # Ensure previous pool is cleaned up if running multiple times in same session
    if Process.whereis(LTPool), do: Supervisor.stop(LTPool)

    {:ok, _pid} = ElasticPool.start_link(
      name: LTPool,
      worker_handler: LoadTest.DummyWorker,
      max_workers: 8,
      baseline_workers: 2,
      scale_threshold: 10
    )

    total = qps * duration
    avg_interval_us = 1_000_000 / qps
    # Mean of Gamma = shape * scale. We want Mean = avg_interval_us.
    scale = avg_interval_us / shape

    IO.puts "\n--- Starting Load Test: #{qps} QPS for #{duration}s (Work: #{work_duration_ms}ms, Shape: #{shape}) ---"
    parent = self()

    spawn_link(fn ->
      dispatch_loop(total, System.monotonic_time(:microsecond), shape, scale, parent, work_duration_ms)
    end)

    collect(total, [], 0)
  end

  defp dispatch_loop(0, _last_target, _shape, _scale, _parent, _work_ms), do: :ok
  defp dispatch_loop(remaining, last_target, shape, scale, parent, work_ms) do
    # Calculate time until the next arrival
    delay = next_gamma(shape, scale)
    target = last_target + delay

    now = System.monotonic_time(:microsecond)
    if target > now, do: Process.sleep(trunc((target - now) / 1000))

    spawn(fn ->
      s = System.monotonic_time(:microsecond)
      res = ElasticPool.call(LTPool, {:work, work_ms})
      send(parent, {:res, res, System.monotonic_time(:microsecond) - s})
    end)

    dispatch_loop(remaining - 1, target, shape, scale, parent, work_ms)
  end

  # Gamma distribution generator (Marsaglia and Tsang method)
  defp next_gamma(a, b) when a < 1.0 do
    next_gamma(a + 1.0, b) * :math.pow(:rand.uniform(), 1.0 / a)
  end
  defp next_gamma(a, b) do
    d = a - 1.0 / 3.0
    c = 1.0 / :math.sqrt(9.0 * d)
    generate_gamma(d, c) * b
  end

  defp generate_gamma(d, c) do
    x = :rand.normal()
    v = 1.0 + c * x
    if v <= 0 do
      generate_gamma(d, c)
    else
      v = v * v * v
      u = :rand.uniform()
      if u < 1.0 - 0.0331 * x * x * x * x or :math.log(u) < 0.5 * x * x + d * (1.0 - v + :math.log(v)) do
        d * v
      else
        generate_gamma(d, c)
      end
    end
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
    result_count = length(results)

    #Adjust latency for expected sleep.  We are only interested in the time lost
    # in the dispatch machinery
    latencies = Enum.map(results, fn {{:ok, s}, l} -> l / 1000 - s end)
    if result_count > 0 do
      avg = Enum.sum(latencies) / result_count
      p95 = Enum.sort(latencies) |> Enum.at(max(0, round(result_count * 0.95) - 1))

      IO.puts "\n\nSuccess: #{result_count}/#{total}"
      IO.puts "Total Workers: #{status.total_workers}"
      IO.puts "Peak Workers: #{status.peak_workers}"
      IO.puts "Available Workers: #{status.available_workers}"
      IO.puts "Waiting Clients: #{status.waiting_clients}"
      IO.puts "Avg Latency: #{Float.round(avg, 2)}ms"
      IO.puts "P95 Latency: #{Float.round(p95, 2)}ms"
    end
  end
end

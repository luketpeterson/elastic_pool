# ==============================================================================
# SECTION 1: ElasticPool Usage Example
# ==============================================================================
# This section demonstrates the idiomatic way to integrate and use ElasticPool.

defmodule LoadTest.DummyWorker do
  @moduledoc """
  A simple worker implementation using the ElasticPool.Worker behavior.
  """
  use ElasticPool.Worker

  @impl true
  def init(args) do
    # IO.puts("  [Worker] Spinning up...")
    Process.sleep(500)
    args
  end

  @impl true
  def handle_work({:work, duration_ms}, _from, state) do
    Process.sleep(duration_ms)
    {:reply, {:ok, duration_ms}, state}
  end

  @impl true
  def terminate(_reason, _state) do
    # IO.puts("  [Worker] Shutting down...")
    :ok
  end
end

defmodule LTPool do
  use ElasticPool,
    worker_handler: LoadTest.DummyWorker
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
    # 1. Start the pool as a supervised process.
    # In a real app, this would likely be in your Application supervision tree.
    if Process.whereis(LTPool), do: Supervisor.stop(LTPool)

    {:ok, _pid} =
      LTPool.start_link(
        max_workers: 8,
        initial_workers: 2,
        scaling_policy_opts: [scale_up_threshold: 10]
      )

    # 2. Perform work using the pool module's call API
    LoadTest.Harness.start(LTPool, qps, duration, work_duration_ms, shape)
  end
end

# ==============================================================================
# SECTION 2: Load Testing Harness
# ==============================================================================
# Internal machinery for simulating high concurrency and stochastic arrivals.

defmodule LoadTest.Harness do
  @moduledoc false

  def start(pool_name, qps, duration, work_ms, shape) do
    total = qps * duration
    avg_interval_us = 1_000_000 / qps
    scale = avg_interval_us / shape

    IO.puts(
      "\n--- Starting Load Test: #{qps} QPS for #{duration}s (Work: #{work_ms}ms, Shape: #{shape}) ---"
    )

    parent = self()

    spawn_link(fn ->
      dispatch_loop(
        pool_name,
        total,
        System.monotonic_time(:microsecond),
        shape,
        scale,
        parent,
        work_ms
      )
    end)

    collect_results(pool_name, total, [], 0)
  end

  defp dispatch_loop(_pool, 0, _last, _sh, _sc, _p, _w), do: :ok

  defp dispatch_loop(pool, remaining, last_target, shape, scale, parent, work_ms) do
    delay = next_gamma(shape, scale)
    target = last_target + delay

    now = System.monotonic_time(:microsecond)
    if target > now, do: Process.sleep(trunc((target - now) / 1000))

    spawn(fn ->
      s = System.monotonic_time(:microsecond)
      res = pool.call({:work, work_ms})
      send(parent, {:res, res, System.monotonic_time(:microsecond) - s})
    end)

    dispatch_loop(pool, remaining - 1, target, shape, scale, parent, work_ms)
  end

  defp collect_results(pool, total, results, count) do
    if rem(count, max(1, div(total, 10))) == 0 do
      IO.write("\rProgress: #{count}/#{total}")
    end

    if count < total do
      receive do
        {:res, r, l} -> collect_results(pool, total, [{r, l} | results], count + 1)
      after
        60_000 ->
          IO.puts("\nTimed out waiting for results.")
          finish(pool, results, total)
      end
    else
      finish(pool, results, total)
    end
  end

  defp finish(pool, results, total) do
    process_results(pool, results, total)
  end

  defp process_results(pool, results, total) do
    result_count = length(results)
    # Adjust latency for expected sleep to see system overhead/queue time
    latencies = Enum.map(results, fn {{:ok, s}, l} -> l / 1000 - s end)

    if result_count > 0 do
      avg = Enum.sum(latencies) / result_count
      p95 = Enum.sort(latencies) |> Enum.at(max(0, round(result_count * 0.95) - 1))

      IO.puts("\n\nSuccess: #{result_count}/#{total}")
      IO.puts("Peak Workers: #{pool.peak_workers()}")
      IO.puts("Avg Excess Latency: #{Float.round(avg, 2)}ms")
      IO.puts("P95 Excess Latency: #{Float.round(p95, 2)}ms")
    end
  end

  # --- Stochastic Generator (Gamma Distribution) ---

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

      if u < 1.0 - 0.0331 * x * x * x * x or
           :math.log(u) < 0.5 * x * x + d * (1.0 - v + :math.log(v)) do
        d * v
      else
        generate_gamma(d, c)
      end
    end
  end
end

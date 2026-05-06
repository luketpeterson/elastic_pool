defmodule FastWorker do
  use ElasticPool.Worker
  @impl true
  def handle_work(:ping, _from, state), do: {:reply, :pong, state}
end

defmodule FastPool do
  use ElasticPool, worker_handler: FastWorker
end

defmodule PoolBench do
  def run(iterations \\ 100_000) do
    IO.puts "Starting FastPool..."
    # The worker isn't doing anything, so the number of workers doesn't seem to matter
    {:ok, _pid} = FastPool.start_link(initial_workers: 1, stats_interval: :never)

    IO.puts "Warming up..."
    for _ <- 1..1000, do: FastPool.call(:ping)

    IO.puts "Running #{iterations} iterations..."
    {time, _} = :timer.tc(fn ->
      for _ <- 1..iterations do
        FastPool.call(:ping)
      end
    end)

    avg_us = time / iterations
    qps = iterations / (time / 1_000_000)

    IO.puts "\n--- ElasticPool Micro-Benchmark ---"
    IO.puts "Total Iterations: #{iterations}"
    IO.puts "Total Time:       #{Float.round(time / 1000, 2)}ms"
    IO.puts "Avg Latency:      #{Float.round(avg_us, 3)}µs"
    IO.puts "Throughput:       #{Float.round(qps, 0)} calls/sec"
    IO.puts "Threads Created:  #{FastPool.peak_workers()}"
  end
end

PoolBench.run()

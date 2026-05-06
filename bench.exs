defmodule FastWorker do
  use ElasticPool.Worker

  @impl true
  def handle_work(:ping, _from, state), do: {:reply, :pong, state}
end

defmodule FastPool do
  use ElasticPool, worker_handler: FastWorker
end

defmodule PoolboyWorker do
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, [])
  end

  @impl true
  def init(:ok), do: {:ok, %{}}

  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :pong, state}
end

defmodule PoolBench do
  @default_iterations 1_000_000
  @default_workers 1
  @poolboy_pool :poolboy_bench_pool

  def run(argv \\ System.argv()) do
    ensure_bench_env!()
    {benches, iterations} = parse_args(argv)

    for bench <- benches do
      case bench do
        :elastic_pool -> run_elastic_pool(iterations)
        :poolboy -> run_poolboy(iterations)
      end
    end
  end

  defp run_elastic_pool(iterations) do
    IO.puts("\n--- ElasticPool Micro-Benchmark ---")
    IO.puts("Starting FastPool...")

    {:ok, pool_pid} =
      FastPool.start_link(initial_workers: @default_workers, stats_interval: :never)

    try do
      benchmark(
        iterations,
        fn -> FastPool.call(:ping) end,
        fn -> FastPool.peak_workers() end
      )
    after
      shutdown_pool(pool_pid)
    end
  end

  defp run_poolboy(iterations) do
    ensure_poolboy_loaded!()

    IO.puts("\n--- poolboy Micro-Benchmark ---")
    IO.puts("Starting Poolboy...")

    {:ok, pool_pid} =
      :poolboy.start_link(
        name: {:local, @poolboy_pool},
        worker_module: PoolboyWorker,
        size: @default_workers,
        max_overflow: 0
      )

    try do
      benchmark(
        iterations,
        fn ->
          :poolboy.transaction(@poolboy_pool, fn pid ->
            GenServer.call(pid, :ping)
          end)
        end,
        fn -> @default_workers end
      )
    after
      shutdown_pool(pool_pid)
    end
  end

  defp benchmark(iterations, call_fun, worker_count_fun) do
    IO.puts("Warming up...")
    for _ <- 1..1000, do: call_fun.()

    IO.puts("Running #{iterations} iterations...")

    {time, _} =
      :timer.tc(fn ->
        for _ <- 1..iterations do
          call_fun.()
        end
      end)

    avg_us = time / iterations
    qps = iterations / (time / 1_000_000)

    IO.puts("Total Iterations: #{iterations}")
    IO.puts("Total Time:       #{Float.round(time / 1000, 2)}ms")
    IO.puts("Avg Latency:      #{Float.round(avg_us, 3)}µs")
    IO.puts("Throughput:       #{Float.round(qps, 0)} calls/sec")
    IO.puts("Workers:          #{worker_count_fun.()}")
  end

  defp parse_args(argv) do
    case argv do
      [] ->
        {[:elastic_pool, :poolboy], @default_iterations}

      [bench] ->
        {[parse_bench!(bench)], @default_iterations}

      [bench, iterations] ->
        {[parse_bench!(bench)], parse_iterations!(iterations)}

      _ ->
        usage!()
    end
  end

  defp parse_bench!("elastic_pool"), do: :elastic_pool
  defp parse_bench!("poolboy"), do: :poolboy
  defp parse_bench!(_), do: usage!()

  defp parse_iterations!(value) do
    case Integer.parse(value) do
      {iterations, ""} when iterations > 0 -> iterations
      _ -> usage!()
    end
  end

  defp ensure_poolboy_loaded! do
    case Code.ensure_loaded(:poolboy) do
      {:module, :poolboy} ->
        :ok

      _ ->
        Mix.raise(
          "poolboy is not available. Run `mix deps.get` before using the `poolboy` benchmark."
        )
    end
  end

  defp ensure_bench_env! do
    if Mix.env() != :bench do
      Mix.raise("""
      bench.exs must be run with MIX_ENV=bench.

      Examples:
        MIX_ENV=bench mix deps.get
        MIX_ENV=bench mix run bench.exs
        MIX_ENV=bench mix run bench.exs poolboy 100000
      """)
    end
  end

  defp shutdown_pool(pool_pid) do
    Process.unlink(pool_pid)
    GenServer.stop(pool_pid, :normal, 5_000)
  catch
    :exit, {:noproc, _} -> :ok
    :exit, :noproc -> :ok
  end

  defp usage! do
    Mix.raise("""
    Usage: MIX_ENV=bench mix run bench.exs [elastic_pool|poolboy] [iterations]

    Examples:
      MIX_ENV=bench mix run bench.exs
      MIX_ENV=bench mix run bench.exs elastic_pool 100000
      MIX_ENV=bench mix run bench.exs poolboy 100000
    """)
  end
end

PoolBench.run()

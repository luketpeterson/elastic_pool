
defmodule FastWorker do
  use ElasticPool.Worker
  @impl true
  def handle_work(:ping, _from, state), do: {:reply, :pong, state}
end

defmodule FastPool do
  use ElasticPool,
    worker_handler: FastWorker,
    scaling_policy: ElasticPool.Policies.Null
end

defmodule PoolboyWorker do
  use GenServer
  def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok, [])
  @impl true
  def init(:ok), do: {:ok, %{}}
  @impl true
  def handle_call(:ping, _from, state), do: {:reply, :pong, state}
end

defmodule ConcurrencyBench do
  @iterations_per_client 10_000
  @clients 100
  @workers 100
  @poolboy_pool :poolboy_concurrency_pool

  def run do
    run_elastic_pool()
    run_poolboy()
  end

  defp run_elastic_pool do
    IO.puts("\n--- ElasticPool Concurrency Benchmark ---")
    IO.puts("Clients: #{@clients}, Iterations/Client: #{@iterations_per_client}, Workers: #{@workers}")

    {:ok, _} = FastPool.start_link(initial_workers: @workers, stats_interval: :never)

    benchmark(fn -> FastPool.call(:ping) end)

    Supervisor.stop(FastPool)
  end

  defp run_poolboy do
    if Code.ensure_loaded?(:poolboy) do
      IO.puts("\n--- poolboy Concurrency Benchmark ---")
      IO.puts("Clients: #{@clients}, Iterations/Client: #{@iterations_per_client}, Workers: #{@workers}")

      {:ok, _} = :poolboy.start_link(
        name: {:local, @poolboy_pool},
        worker_module: PoolboyWorker,
        size: @workers,
        max_overflow: 0
      )

      benchmark(fn ->
        :poolboy.transaction(@poolboy_pool, fn pid ->
          GenServer.call(pid, :ping)
        end)
      end)

      Supervisor.stop(@poolboy_pool)
    else
      IO.puts("\n--- poolboy skipped (not loaded) ---")
    end
  end

  defp benchmark(call_fun) do
    # Warmup
    tasks = for _ <- 1..@clients do
      Task.async(fn -> for _ <- 1..100, do: call_fun.() end)
    end
    Task.await_many(tasks)

    start_time = System.monotonic_time(:microsecond)

    tasks = for _ <- 1..@clients do
      Task.async(fn ->
        for _ <- 1..@iterations_per_client do
          call_fun.()
        end
      end)
    end
    Task.await_many(tasks, :infinity)

    end_time = System.monotonic_time(:microsecond)
    total_time_ms = (end_time - start_time) / 1000
    total_calls = @clients * @iterations_per_client
    qps = total_calls / (total_time_ms / 1000)

    IO.puts("Total Time:  #{Float.round(total_time_ms, 2)}ms")
    IO.puts("Throughput:  #{Float.round(qps, 0)} calls/sec")
  end
end

ConcurrencyBench.run()

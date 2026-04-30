defmodule ElasticPool.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    # Create ETS table for lock-free status reads
    :ets.new(:elastic_pool_stats, [:public, :set, :named_table, read_concurrency: true])
    # Initialize with default values
    :ets.insert(:elastic_pool_stats, {:total_ready, 0})
    :ets.insert(:elastic_pool_stats, {:waiting_clients, 0})

    # Centralized configuration
    config = %{
      max_workers: Application.get_env(:elastic_pool, :pool_max_workers, 8),
      baseline_workers: Application.get_env(:elastic_pool, :pool_baseline_workers, 2),
      cooldown_ms: Application.get_env(:elastic_pool, :pool_cooldown_ms, 500),
      scale_threshold: Application.get_env(:elastic_pool, :pool_scale_threshold, 100)
    }


    children = [
      {ElasticPool.WorkerSupervisor, []},
      {ElasticPool.ScalingManager, config},
      {ElasticPool.Pool, [scale_threshold: config.scale_threshold]}
    ]

    opts = [strategy: :one_for_one, name: ElasticPool.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        # Start baseline workers
        1..config.baseline_workers
        |> Enum.each(fn _ ->
          {:ok, _worker_pid} = ElasticPool.WorkerSupervisor.start_worker()
        end)

        wait_for_workers(config.baseline_workers)

        {:ok, pid}

      error -> error
    end
  end

  defp wait_for_workers(target) do
    status = ElasticPool.status()
    if status.total_workers < target do
      Process.sleep(10)
      wait_for_workers(target)
    else
      :ok
    end
  end
end

defmodule ElasticPool do
  @moduledoc """
  Main Supervisor for an ElasticPool instance.
  """
  use Supervisor

  def start_link(opts) do
    name = opts[:name] || __MODULE__
    case Supervisor.start_link(__MODULE__, opts, name: name) do
      {:ok, pid} ->
        # Retrieve config to start baseline workers
        # The init/1 function already started a Task, but it might have been too fast.
        # Let's ensure it's done correctly here or via a coordinated start.
        {:ok, pid}
      error -> error
    end
  end

  def call(pool_name \\ __MODULE__, request, timeout \\ 30_000) do
    pool_proc = Module.concat(pool_name, Pool)
    case ElasticPool.Pool.checkout(pool_proc, timeout) do
      {:ok, _ref, worker_pid} ->
        try do
          GenServer.call(worker_pid, request, timeout)
        after
          ElasticPool.Pool.checkin(pool_proc, worker_pid)
        end
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def init(opts) do
    name = opts[:name] || __MODULE__
    worker_handler = Keyword.fetch!(opts, :worker_handler)
    worker_args = opts[:worker_args] || []

    pool_proc = Module.concat(name, Pool)
    manager_proc = Module.concat(name, ScalingManager)
    sup_proc = Module.concat(name, WorkerSupervisor)
    stats_table = Module.concat(name, Stats)

    if :ets.whereis(stats_table) == :undefined do
      :ets.new(stats_table, [:public, :set, :named_table, read_concurrency: true])
      :ets.insert(stats_table, {:total_ready, 0})
      :ets.insert(stats_table, {:waiting_clients, 0})
    end

    config = %{
      name: name,
      pool: pool_proc,
      manager: manager_proc,
      supervisor: sup_proc,
      stats_table: stats_table,
      max_workers: opts[:max_workers] || 8,
      baseline_workers: opts[:baseline_workers] || 2,
      cooldown_ms: opts[:cooldown_ms] || 500,
      scale_threshold: opts[:scale_threshold] || 10,
      worker_handler: worker_handler,
      worker_args: worker_args
    }

    children = [
      {ElasticPool.WorkerSupervisor, config},
      {ElasticPool.ScalingManager, config},
      {ElasticPool.Pool, config}
    ]

    # Use a post-start process to avoid race conditions during init
    spawn(fn ->
      wait_for_alive(sup_proc)
      1..config.baseline_workers
      |> Enum.each(fn _ ->
        ElasticPool.WorkerSupervisor.start_worker(sup_proc, config)
      end)
    end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp wait_for_alive(name) do
    if Process.whereis(name) do
      :ok
    else
      Process.sleep(10)
      wait_for_alive(name)
    end
  end

  def status(name \\ __MODULE__) do
    ElasticPool.Pool.status(Module.concat(name, Pool))
  end
end

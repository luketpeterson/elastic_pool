defmodule ElasticPool do
  @moduledoc """
  Main Supervisor for an ElasticPool instance.
  """
  use Supervisor

  def start_link(opts) do
    name = opts[:name] || __MODULE__
    baseline = opts[:baseline_workers] || 2
    timeout = opts[:start_timeout] || 5000

    case Supervisor.start_link(__MODULE__, opts, name: name) do
      {:ok, pid} ->
        case wait_until_ready(name, baseline, timeout) do
          :ok -> {:ok, pid}
          {:error, :timeout} -> 
            Supervisor.stop(pid)
            {:error, :timeout}
        end
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

    # Trigger parallel startup of baseline workers
    spawn(fn ->
      wait_for_alive(sup_proc)
      1..config.baseline_workers
      |> Enum.each(fn _ ->
        spawn(fn ->
          ElasticPool.WorkerSupervisor.start_worker(sup_proc, config)
        end)
      end)
    end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  def status(name \\ __MODULE__) do
    ElasticPool.Pool.status(Module.concat(name, Pool))
  end

  defp wait_until_ready(name, baseline, timeout) do
    start = System.monotonic_time(:millisecond)
    do_wait_until_ready(name, baseline, timeout, start)
  end

  defp do_wait_until_ready(name, baseline, timeout, start) do
    if status(name).total_workers >= baseline do
      :ok
    else
      now = System.monotonic_time(:millisecond)
      if (now - start) > timeout do
        {:error, :timeout}
      else
        Process.sleep(10)
        do_wait_until_ready(name, baseline, timeout, start)
      end
    end
  end

  defp wait_for_alive(name) do
    if Process.whereis(name) do
      :ok
    else
      Process.sleep(10)
      wait_for_alive(name)
    end
  end
end

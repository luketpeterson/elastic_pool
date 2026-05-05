defmodule ElasticPool do
  @moduledoc """
  Main Supervisor for an ElasticPool instance.
  """
  use Supervisor

  @doc """
  Starts an elastic pool.

  ## Options

  - `:worker_handler` - worker module implementing `ElasticPool.Worker`
  - `:worker_args` - arguments passed to each worker
  - `:initial_workers` - pool size at initialization, defaults to `2`
  - `:max_workers` - absolute ceiling on the number of workers that may be
    started, regardless of scaling policy. Defaults to `:infinity`.
    Use `max_workers` when each worker represents a specific and finite
    resource that should not be over-committed, such as a physical CPU core,
    a fixed-size license pool, or some other hard capacity limit that should
    never be exceeded.
  - `:scaling_policy` - scaling policy module, defaults to
    `ElasticPool.Policies.Threshold`
  - `:scaling_policy_opts` - options passed to the scaling policy
  - `:start_timeout` - time in ms to wait for initial workers to come up, defaults
    to `5000`
  - `:max_restarts` - maximum number of worker crashes allowed in `:max_period`,
    defaults to `3`
  - `:max_period` - time window for `:max_restarts` in seconds, defaults to `5`
  """
  def start_link(opts) do
    name = opts[:name] || __MODULE__
    initial = opts[:initial_workers] || 2
    timeout = opts[:start_timeout] || 5000

    case Supervisor.start_link(__MODULE__, opts, name: name) do
      {:ok, pid} ->
        case wait_until_ready(name, initial, timeout) do
          :ok ->
            {:ok, pid}

          {:error, :timeout} ->
            Supervisor.stop(pid)
            {:error, :timeout}
        end

      error ->
        error
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

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def init(opts) do
    name = opts[:name] || __MODULE__
    worker_handler = Keyword.fetch!(opts, :worker_handler)
    worker_args = opts[:worker_args] || []

    pool_proc = Module.concat(name, Pool)
    manager_proc = Module.concat(name, WorkerManager)
    stats_table = Module.concat(name, Stats)

    if :ets.whereis(stats_table) == :undefined do
      :ets.new(stats_table, [:public, :set, :named_table, read_concurrency: true])

      :ets.insert(stats_table,
        target_workers: opts[:initial_workers] || 2,
        active_workers: 0,
        available_workers: 0,
        peak_workers: 0,
        waiting_clients: 0,
        request_count: 0
      )
    end

    # Group the configuration
    config = %{
      name: name,
      pool: pool_proc,
      manager: manager_proc,
      stats_table: stats_table,
      max_workers: Keyword.get(opts, :max_workers, :infinity),
      initial_workers: opts[:initial_workers] || 2,
      max_restarts: opts[:max_restarts] || 3,
      max_period: opts[:max_period] || 5,

      # Scaling Policy Configuration
      scaling_policy: opts[:scaling_policy] || ElasticPool.Policies.Threshold,
      scaling_policy_opts: opts[:scaling_policy_opts] || [],

      # Worker Configuration
      worker_handler: worker_handler,
      worker_args: worker_args
    }

    children = [
      {ElasticPool.Pool, config},
      {ElasticPool.WorkerManager, config}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  # --- High Performance Accessors ---

  @doc """
  Returns the 'Target' number of workers the pool intends to have.
  Identity: `starting_workers = target_workers - active_workers` (may be negative if the pool is about to scale down).
  """
  def target_workers(name), do: get_stat(name, :target_workers)

  @doc """
  Returns the number of workers that are currently alive and monitored by the pool.
  """
  def active_workers(name), do: get_stat(name, :active_workers)

  @doc """
  Returns the number of workers that are currently idle and ready to take work.
  Identity: `busy_workers = active_workers - available_workers`
  """
  def available_workers(name), do: get_stat(name, :available_workers)

  @doc """
  Returns the highest number of concurrent active workers that have existed since the pool started.
  """
  def peak_workers(name), do: get_stat(name, :peak_workers)

  @doc """
  Returns the number of clients currently waiting in the checkout queue.
  """
  def waiting_clients(name), do: get_stat(name, :waiting_clients)

  @doc """
  Returns the cumulative number of checkout requests made to the pool since it started.
  """
  def request_count(name), do: get_stat(name, :request_count)

  defp get_stat(name, key) do
    stats_table = Module.concat(name, Stats)
    :ets.lookup_element(stats_table, key, 2)
  rescue
    ArgumentError -> 0
  end

  defp wait_until_ready(name, baseline, timeout) do
    start = System.monotonic_time(:millisecond)
    do_wait_until_ready(name, baseline, timeout, start)
  end

  defp do_wait_until_ready(name, baseline, timeout, start) do
    if active_workers(name) >= baseline do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now - start > timeout do
        {:error, :timeout}
      else
        Process.sleep(10)
        do_wait_until_ready(name, baseline, timeout, start)
      end
    end
  end
end

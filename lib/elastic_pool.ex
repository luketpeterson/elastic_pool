defmodule ElasticPool do
  @moduledoc """
  A high-performance, reactive worker pool for Elixir.

  ElasticPool uses a macro-based approach to provide compile-time validation,
  monomorphized data access, and a clean, module-based API.

  ## Options

  The following options can be provided either at compile-time (in the `use` macro)
  or overridden at runtime (in `start_link/1`).

  - `:worker_handler` - (Required) Worker module implementing `ElasticPool.Worker`.
  - `:worker_args` - Arguments passed to each worker. Defaults to `[]`.
  - `:initial_workers` - Pool size at initialization. Defaults to `2`.
  - `:max_workers` - Absolute ceiling on the number of workers that may be
    started, regardless of scaling policy. Defaults to `:infinity`.
    Use `max_workers` when each worker represents a specific and finite
    resource that should not be over-committed, such as a physical CPU core,
    a fixed-size license pool, or some other hard capacity limit.
  - `:scaling_policy` - Scaling policy module. Defaults to
    `ElasticPool.Policies.Threshold`.
  - `:scaling_policy_opts` - Options passed to the scaling policy.
  - `:start_timeout` - Time in ms to wait for initial workers to come up.
    Defaults to `5000`.
  - `:max_restarts` - Maximum number of worker crashes allowed in `:max_period`.
    Defaults to `3`.
  - `:max_period` - Time window for `:max_restarts` in seconds. Defaults to `5`.
  - `:stats_interval` - Time in ms for periodic status telemetry heartbeats.
    Set to `:never` to disable. Defaults to `5000`.

  ## Example

      defmodule MyPool do
        use ElasticPool,
          worker_handler: MyWorker,
          initial_workers: 5,
          max_workers: 10,
          scaling_policy_opts: [available_reserve: 2]
      end

      # Start it in your supervision tree:
      {MyPool, []}

      # Perform work:
      MyPool.call(:do_something)
  """

  @doc """
  Defines a specialized pool module.

  Using this macro performs compile-time validation of the worker and policy
  and generates a specialized supervisor and API for the pool.

  See the module documentation for the full list of available options.
  """
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use Supervisor
      require ElasticPool

      # Perform compile-time validation of the provided modules.
      ElasticPool.validate_config!(__MODULE__, opts)

      @doc """
      Starts the pool supervisor.

      Inherits all options from the `use ElasticPool` definition, which can
      be overridden here. By default, the pool instance is named after the module.
      """
      def start_link(runtime_opts \\ []) do
        full_opts = Keyword.merge(unquote(opts), runtime_opts)
        name = full_opts[:name] || __MODULE__
        initial = full_opts[:initial_workers] || 2
        timeout = full_opts[:start_timeout] || 5000

        case Supervisor.start_link(__MODULE__, full_opts, name: name) do
          {:ok, pid} ->
            manager_proc = Module.concat(name, WorkerManager)

            try do
              case ElasticPool.WorkerManager.wait_for_ready(manager_proc, initial, timeout) do
                :ok ->
                  {:ok, pid}

                {:error, reason} ->
                  if Process.alive?(pid), do: Supervisor.stop(pid)
                  {:error, reason}
              end
            catch
              :exit, _ ->
                {:error, :supervisor_died}
            end

          error ->
            error
        end
      end

      @impl true
      def init(opts) do
        ElasticPool.init_pool(opts[:name] || __MODULE__, opts)
      end

      @doc """
      Performs a synchronous call to a worker in this pool.
      Defaults to the instance named after the module.
      """
      def call(request, timeout \\ 30_000) do
        ElasticPool.call(__MODULE__, request, timeout)
      end

      @doc """
      Performs a synchronous call to a specific named instance of this pool.
      """
      def call(name, request, timeout) do
        ElasticPool.call(name, request, timeout)
      end

      @doc """
      Returns the 'Target' number of workers the pool intends to have.
      Identity: `starting_workers = target_workers - active_workers` (may be negative if the pool is about to scale down).
      """
      def target_workers(name \\ __MODULE__), do: ElasticPool.target_workers(name)

      @doc """
      Returns the number of workers that are currently alive and monitored by the pool.
      """
      def active_workers(name \\ __MODULE__), do: ElasticPool.active_workers(name)

      @doc """
      Returns the number of workers that are currently idle and ready to take work.
      Identity: `busy_workers = active_workers - available_workers`
      """
      def available_workers(name \\ __MODULE__), do: ElasticPool.available_workers(name)

      @doc """
      Returns the highest number of concurrent active workers that have existed since the pool started.
      """
      def peak_workers(name \\ __MODULE__), do: ElasticPool.peak_workers(name)

      @doc """
      Returns the number of clients currently waiting in the checkout queue.
      """
      def waiting_clients(name \\ __MODULE__), do: ElasticPool.waiting_clients(name)

      @doc """
      Returns the cumulative number of checkout requests made to the pool since it started.
      """
      def request_count(name \\ __MODULE__), do: ElasticPool.request_count(name)

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end
    end
  end

  # --- Internal Helpers ---

  @doc false
  def validate_config!(module, opts) do
    worker = opts[:worker_handler] || raise "Missing :worker_handler in #{module}"
    policy = opts[:scaling_policy] || ElasticPool.Policies.Threshold

    cond do
      !Code.ensure_loaded?(worker) ->
        raise ArgumentError, "Worker module #{inspect(worker)} could not be loaded in #{module}"

      !Code.ensure_loaded?(policy) ->
        raise ArgumentError, "Scaling policy module #{inspect(policy)} could not be loaded in #{module}"

      true ->
        :ok
    end
  end

  @doc false
  def init_pool(name, opts) do
    worker_handler = Keyword.fetch!(opts, :worker_handler)
    worker_args = opts[:worker_args] || []

    pool_proc = Module.concat(name, Pool)
    manager_proc = Module.concat(name, WorkerManager)
    stats_table = Module.concat(name, Stats)

    if :ets.whereis(stats_table) == :undefined do
      :ets.new(stats_table, [:public, :set, :named_table, read_concurrency: true])
    end

    :ets.insert(stats_table, [
      {:target_workers, opts[:initial_workers] || 2},
      {:active_workers, 0},
      {:available_workers, 0},
      {:peak_workers, 0},
      {:waiting_clients, 0},
      {:request_count, 0}
    ])

    config = %{
      name: name,
      pool: pool_proc,
      manager: manager_proc,
      stats_table: stats_table,
      max_workers: Keyword.get(opts, :max_workers, :infinity),
      initial_workers: opts[:initial_workers] || 2,
      max_restarts: opts[:max_restarts] || 3,
      max_period: opts[:max_period] || 5,
      scaling_policy: opts[:scaling_policy] || ElasticPool.Policies.Threshold,
      scaling_policy_opts: opts[:scaling_policy_opts] || [],
      worker_handler: worker_handler,
      worker_args: worker_args
    }

    stats_interval = Keyword.get(opts, :stats_interval, 5000)

    children = [
      {ElasticPool.Pool, config},
      {ElasticPool.WorkerManager, config}
    ]

    children =
      if stats_interval == :never do
        children
      else
        children ++ [{ElasticPool.StatsPoller, pool: name, interval: stats_interval}]
      end

    Supervisor.init(children, strategy: :one_for_all, max_restarts: 0)
  end

  @doc """
  Performs a synchronous call to a worker in a named pool.
  """
  def call(pool_name, request, timeout \\ 30_000) do
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

  @doc false
  def get_stat_from_table(table, key) do
    :ets.lookup_element(table, key, 2)
  rescue
    ArgumentError -> 0
  end

  defp get_stat(name, key) do
    stats_table = Module.concat(name, Stats)
    get_stat_from_table(stats_table, key)
  end
end

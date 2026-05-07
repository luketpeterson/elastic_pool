defmodule ElasticPool do
  @moduledoc """
  A high-performance, reactive worker pool for Elixir.

  ElasticPool uses a macro-based approach to provide compile-time validation,
  monomorphized data access, and a clean, module-based API.

  ## Configuration

  ElasticPool separates configuration into two phases:

  ### 1. Macro Options (Compile-time)
  These are passed to `use ElasticPool` and are used to generate specialized
  code. They are required for monomorphization.

  - `:worker_handler` - (Required) Worker module implementing `ElasticPool.Worker`.
  - `:scaling_policy` - Scaling policy module. Defaults to
    `ElasticPool.Policies.Threshold`.

  ### 2. Runtime Options
  These are passed to your pool's `start_link/1` function.

  - `:name` - name of the pool instance. Defaults to the module name.
  - `:worker_args` - Arguments passed to the handler module's `init/1` callback. Defaults to `[]`.
  - `:initial_workers` - Pool size at initialization. Defaults to `2`.
  - `:max_workers` - Absolute ceiling on the number of workers that may be
    started, regardless of scaling policy. Defaults to `:infinity`.
    Use `max_workers` when each worker represents a specific and finite
    resource that should not be over-committed, such as a physical CPU core,
    a fixed-size license pool, or some other hard capacity limit.
  - `:scaling_policy_opts` - Options passed to the scaling policy.
  - `:start_timeout` - Time in ms to wait for initial workers to come up.
    Defaults to `5000`.
  - `:max_restarts` - Maximum number of worker crashes allowed in `:max_period`.
    Defaults to `3`.
  - `:max_period` - Time window for `:max_restarts` in seconds. Defaults to `5`.
  - `:stats_interval` - Time in ms for periodic status telemetry heartbeats.
    Set to `:never` to disable periodic stats. Defaults to `5000`.

  ## Example

      defmodule MyPool do
        use ElasticPool,
          worker_handler: MyWorker,
          scaling_policy: MyCustomPolicy
      end

      # Start it in your supervision tree with runtime options:
      {MyPool, [initial_workers: 5, max_workers: 10]}

      # Perform work:
      MyPool.call(:do_something)
  """

  @doc """
  Defines a specialized pool module.

  Using this macro performs compile-time validation of the worker and policy
  and generates a specialized supervisor and API for the pool.

  ## Macro Options (Compile-time only)

  - `:worker_handler` - (Required) Worker module implementing `ElasticPool.Worker`.
  - `:scaling_policy` - Scaling policy module. Defaults to
    `ElasticPool.Policies.Threshold`.

  All other options should be passed to `start_link/1` at runtime.
  """
  defmacro __using__(opts) do
    # Pre-compute absolute module names to avoid scoping issues during expansion
    worker_mod = Module.concat(__CALLER__.module, Worker)
    manager_mod = Module.concat(__CALLER__.module, WorkerManager)
    pool_mod = Module.concat(__CALLER__.module, Pool)

    quote do
      use Supervisor
      require ElasticPool

      # Perform compile-time validation and separation of options.
      {worker, policy} = ElasticPool.validate_macro_config!(__MODULE__, unquote(opts))

      @worker_handler worker
      @scaling_policy policy

      # Default handle for the monomorphized instance
      @default_name __MODULE__

      @doc """
      Starts the pool supervisor.

      Accepts runtime configuration:
      - `:name` - name of the pool instance. Defaults to the module name.
      - `:initial_workers` - Pool size at initialization. Defaults to `2`.
      - `:max_workers` - Absolute ceiling on the number of workers.
      - `:scaling_policy_opts` - Options passed to the scaling policy.
      - `:start_timeout` - Time in ms to wait for initial workers.
      - `:max_restarts` / `:max_period` - Worker crash intensity limits.
      - `:stats_interval` - Telemetry heartbeat interval (ms).
      - `:worker_args` - Arguments passed to the handler module's `init/1` callback.
      """
      def start_link(runtime_opts \\ []) do
        name = runtime_opts[:name] || __MODULE__
        initial = runtime_opts[:initial_workers] || 2
        timeout = runtime_opts[:start_timeout] || 5000

        # All referents are calculated once and passed down.
        # Design: name (Pool/Stats), Module.concat(name, WorkerManager) (Manager).
        manager_handle = Module.concat(name, WorkerManager)
        stats_handle = name

        # We start the supervisor unnamed to allow the Pool GenServer to take
        # the provided 'name' atom.
        case Supervisor.start_link(__MODULE__, runtime_opts) do
          {:ok, pid} ->
            try do
              case ElasticPool.WorkerManager.wait_for_ready(
                     manager_handle,
                     stats_handle,
                     initial,
                     timeout
                   ) do
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
      def init(runtime_opts) do
        ElasticPool.init_pool(
          runtime_opts[:name] || __MODULE__,
          unquote(pool_mod),
          unquote(manager_mod),
          runtime_opts
        )
      end

      # Specialized Worker Module for this Pool
      defmodule Worker do
        require ElasticPool.Worker.Runtime
        ElasticPool.Worker.Runtime.__monomorphize__(worker)
      end

      # Specialized WorkerManager Module for this Pool
      defmodule WorkerManager do
        require ElasticPool.WorkerManager
        # Link to the specialized sibling Worker module using its absolute name
        ElasticPool.WorkerManager.__monomorphize__(unquote(worker_mod))
      end

      # Specialized Pool Module for this Pool
      defmodule Pool do
        require ElasticPool.Pool
        ElasticPool.Pool.__monomorphize__(policy)
      end

      @doc """
      Performs a synchronous call to a worker in the default pool.
      Defaults to the instance named after the module.
      """
      def call(request, timeout \\ 30_000) do
        ElasticPool.execute_call(@default_name, request, timeout)
      end

      @doc """
      Performs a synchronous call to a specific named instance of the pool.
      """
      def call(name, request, timeout) do
        ElasticPool.execute_call(name, request, timeout)
      end

      @doc """
      Returns the 'Target' number of workers the pool intends to have.
      Identity: `starting_workers = target_workers - active_workers` (may be negative if the pool is about to scale down).
      """
      def target_workers(name \\ @default_name), do: ElasticPool.target_workers(name)

      @doc """
      Returns the number of workers that are currently alive and monitored by the pool.
      """
      def active_workers(name \\ @default_name), do: ElasticPool.active_workers(name)

      @doc """
      Returns the number of workers that are currently idle and ready to take work.
      Identity: `busy_workers = active_workers - available_workers`
      """
      def available_workers(name \\ @default_name), do: ElasticPool.available_workers(name)

      @doc """
      Returns the highest number of concurrent active workers that have existed since the pool started.
      """
      def peak_workers(name \\ @default_name), do: ElasticPool.peak_workers(name)

      @doc """
      Returns the number of clients currently waiting in the checkout queue.
      """
      def waiting_clients(name \\ @default_name), do: ElasticPool.waiting_clients(name)

      @doc """
      Returns the cumulative number of checkout requests made to the pool since it started.
      """
      def request_count(name \\ @default_name), do: ElasticPool.request_count(name)

      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end
    end
  end

  # --- High Performance Public Accessor Macros ---

  @doc """
  Returns the 'Target' number of workers the pool intends to have.
  Fails if the pool is not running.
  """
  defmacro target_workers(pool) do
    quote do: :ets.lookup_element(unquote(pool), :target_workers, 2)
  end

  @doc """
  Returns the number of workers that are currently alive and monitored by the pool.
  Fails if the pool is not running.
  """
  defmacro active_workers(pool) do
    quote do: :ets.lookup_element(unquote(pool), :active_workers, 2)
  end

  @doc """
  Returns the number of workers that are currently idle and ready to take work.
  Fails if the pool is not running.
  """
  defmacro available_workers(pool) do
    quote do: :ets.lookup_element(unquote(pool), :available_workers, 2)
  end

  @doc """
  Returns the highest number of concurrent active workers that have existed since the pool started.
  Fails if the pool is not running.
  """
  defmacro peak_workers(pool) do
    quote do: :ets.lookup_element(unquote(pool), :peak_workers, 2)
  end

  @doc """
  Returns the number of clients currently waiting in the checkout queue.
  Fails if the pool is not running.
  """
  defmacro waiting_clients(pool) do
    quote do: :ets.lookup_element(unquote(pool), :waiting_clients, 2)
  end

  @doc """
  Returns the cumulative number of checkout requests made to the pool since it started.
  Fails if the pool is not running.
  """
  defmacro request_count(pool) do
    quote do: :ets.lookup_element(unquote(pool), :request_count, 2)
  end

  # --- Internal Helpers ---

  @doc false
  def validate_macro_config!(module, opts) do
    worker = opts[:worker_handler] || raise "Missing :worker_handler in #{module}"
    policy = opts[:scaling_policy] || ElasticPool.Policies.Threshold

    # Ensure ONLY macro options are present
    allowed_keys = [:worker_handler, :scaling_policy]
    provided_keys = Keyword.keys(opts)
    extra_keys = provided_keys -- allowed_keys

    if extra_keys != [] do
      raise ArgumentError, """
      Invalid macro options in #{module}: #{inspect(extra_keys)}.
      Only :worker_handler and :scaling_policy should be passed to 'use ElasticPool'.
      All other options should be passed to start_link/1 at runtime.
      """
    end

    # Check for presence and behavior at compile time if possible
    cond do
      !Code.ensure_loaded?(worker) ->
        raise ArgumentError, "Worker module #{inspect(worker)} could not be loaded in #{module}"

      !function_exported?(worker, :handle_work, 3) ->
        raise ArgumentError,
              "Worker module #{inspect(worker)} does not implement ElasticPool.Worker behavior (missing handle_work/3) in #{module}"

      !Code.ensure_loaded?(policy) ->
        raise ArgumentError,
              "Scaling policy module #{inspect(policy)} could not be loaded in #{module}"

      true ->
        {worker, policy}
    end
  end

  @doc false
  def validate_config!(_module, _opts), do: :ok

  @doc false
  def init_pool(name, pool_mod, manager_mod, opts) do
    worker_args = opts[:worker_args] || []

    # ZERO-CONCAT DESIGN:
    # Handles are computed once at initialization and stored in process state.
    # Stats table shares the 'name' atom.
    # Manager process is named once using Module.concat.
    stats_table = name
    manager_handle = Module.concat(name, WorkerManager)

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

    # Group the configuration
    config = %{
      name: name,
      # Pool process uses the name directly
      pool: name,
      # Stored atom handle
      manager: manager_handle,
      stats_table: stats_table,
      max_workers: Keyword.get(opts, :max_workers, :infinity),
      initial_workers: opts[:initial_workers] || 2,
      max_restarts: opts[:max_restarts] || 3,
      max_period: opts[:max_period] || 5,

      # Scaling Policy Configuration
      scaling_policy_opts: opts[:scaling_policy_opts] || [],

      # Worker Configuration
      worker_args: worker_args
    }

    stats_interval = Keyword.get(opts, :stats_interval, 5000)

    children = [
      {pool_mod, config},
      {manager_mod, config}
    ]

    children =
      if stats_interval == :never do
        children
      else
        children ++ [{ElasticPool.StatsPoller, config: config, interval: stats_interval}]
      end

    Supervisor.init(children, strategy: :one_for_all, max_restarts: 0)
  end

  @doc false
  def execute_call(pool_proc, request, timeout) do
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
end

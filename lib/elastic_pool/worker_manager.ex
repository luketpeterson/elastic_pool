defmodule ElasticPool.WorkerManager do
  @moduledoc false

  # Internal: The supervisor and lifecycle manager for all worker processes in the pool.
  #
  # WorkerManager is responsible for:
  # - Starting new worker processes to meet scaling targets.
  # - Stopping worker processes when requested by the Pool.
  # - Monitoring workers and performing instant recovery if they crash.
  # - Acting as the direct supervisor for all workers by trapping exits.

  use GenServer
  require Logger

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: config.manager)
  end

  def set_target(manager, target) do
    GenServer.cast(manager, {:set_target, target})
  end

  def wait_for_ready(manager, count, timeout) do
    GenServer.call(manager, {:wait_for_ready, count}, timeout)
  end

  def worker_ready(manager, pid) do
    GenServer.cast(manager, {:worker_ready, pid})
  end

  @doc """
  Safely stops a worker that has been dismissed by the Pool.
  """
  def stop_worker(manager, pid) do
    GenServer.cast(manager, {:stop_worker, pid})
  end

  # --- Callbacks ---

  @impl true
  def init(config) do
    Process.flag(:trap_exit, true)

    target = min(config.initial_workers, config.max_workers)

    state = %{
      # The full pool configuration (max_workers, handler, etc.)
      config: config,
      # Atom name of the pool for identification in logs and telemetry
      pool_name: config.name,
      # Current capacity target requested by the Scaling Policy
      target: target,
      # Set of all physical worker PIDs currently linked to this manager
      workers: MapSet.new(),
      # List of monotonic timestamps of recent worker crashes for intensity tracking
      restarts: [],
      # List of {from, target_count} clients waiting for initial boot-up
      waiting_readiness: []
    }

    # Initial scale-up to baseline
    {:ok, state, {:continue, :init_workers}}
  end

  @impl true
  def handle_continue(:init_workers, state) do
    case reconcile(state.target, state) do
      {:ok, new_state} -> {:noreply, new_state}
      {:error, :too_many_crashes} -> {:stop, :reached_max_restart_intensity, state}
    end
  end

  @impl true
  def handle_call({:wait_for_ready, count}, from, state) do
    current_count = ElasticPool.active_workers(state.pool_name)

    if current_count >= count do
      {:reply, :ok, state}
    else
      {:noreply, %{state | waiting_readiness: [{from, count} | state.waiting_readiness]}}
    end
  end

  @impl true
  def handle_cast({:set_target, target}, state) do
    target = min(target, state.config.max_workers)

    case reconcile(target, %{state | target: target}) do
      {:ok, new_state} -> {:noreply, new_state}
      {:error, :too_many_crashes} -> {:stop, :reached_max_restart_intensity, state}
    end
  end

  @impl true
  def handle_cast({:stop_worker, pid}, state) do
    if MapSet.member?(state.workers, pid) do
      # Normal exit - won't trigger "crash" recovery
      Process.exit(pid, :normal)
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:worker_ready, pid}, state) do
    # 1. Register the worker with the Pool synchronously
    case ElasticPool.Pool.add_worker(state.config.pool, pid) do
      :ok ->
        current_count = ElasticPool.active_workers(state.pool_name)

        # 2. Check if we can satisfy any clients waiting for pool readiness
        remaining_waiting =
          Enum.reduce(state.waiting_readiness, [], fn {from, count}, acc ->
            if current_count >= count do
              GenServer.reply(from, :ok)
              acc
            else
              [{from, count} | acc]
            end
          end)

        {:noreply, %{state | waiting_readiness: remaining_waiting}}

      :error ->
        # Worker died during handover
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, state) do
    new_workers = MapSet.delete(state.workers, pid)
    state = %{state | workers: new_workers}

    case reason do
      :normal ->
        # Planned scale-down or clean exit, do nothing.
        {:noreply, state}

      _other ->
        case check_intensity(state) do
          {:ok, new_state} ->
            # Instant Recovery: Reconcile immediately to hit target
            case reconcile(state.target, new_state, :recovery) do
              {:ok, final_state} -> {:noreply, final_state}
              {:error, :too_many_crashes} -> {:stop, :reached_max_restart_intensity, state}
            end

          {:error, :too_many_crashes} ->
            {:stop, :reached_max_restart_intensity, state}
        end
    end
  end

  # --- Private ---

  defp check_intensity(state) do
    now = System.monotonic_time(:millisecond)
    # max_period is in seconds, so convert to milliseconds
    cutoff = now - (state.config.max_period * 1_000)

    # Filter out old restarts
    recent_restarts = [now | Enum.filter(state.restarts, &(&1 > cutoff))]

    if length(recent_restarts) > state.config.max_restarts do
      {:error, :too_many_crashes}
    else
      {:ok, %{state | restarts: recent_restarts}}
    end
  end

  defp reconcile(target, state, reason \\ nil) do
    active_count = MapSet.size(state.workers)
    needed = target - active_count

    if needed > 0 do
      # Calculate the start reason for this batch once
      start_reason =
        reason || (if active_count == 0, do: :initial, else: :scale_up)

      case start_worker(state.config, start_reason) do
        {:ok, pid} ->
          reconcile(target, %{state | workers: MapSet.put(state.workers, pid)}, start_reason)

        _ ->
          case check_intensity(state) do
            {:ok, new_state} -> reconcile(target, new_state, start_reason)
            {:error, :too_many_crashes} -> {:error, :too_many_crashes}
          end
      end
    else
      {:ok, state}
    end
  end

  defp start_worker(config, reason) do
    worker_args =
      [
        handler: config.worker_handler,
        pool: config.pool,
        manager: config.manager,
        start_reason: reason
      ] ++ config.worker_args

    # Link directly to the manager so we can trap exits
    ElasticPool.Worker.start_link(worker_args)
  end
end

defmodule ElasticPool.WorkerManager do
  @moduledoc """
  The supervisor and lifecycle manager for all worker processes in the pool.

  WorkerManager is responsible for:
  - Starting new worker processes to meet scaling targets.
  - Stopping worker processes when requested by the Pool.
  - Monitoring workers and performing instant recovery if they crash.
  - Acting as the direct supervisor for all workers by trapping exits.
  """
  use GenServer

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: config.manager)
  end

  def set_target(manager, target) do
    GenServer.cast(manager, {:set_target, target})
  end

  def worker_ready(manager) do
    GenServer.cast(manager, :worker_ready)
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
      config: config,
      pool_name: config.name,
      target: target,
      workers: MapSet.new(),
      pending_count: 0,
      restarts: []
    }

    # Initial scale-up to baseline
    {:ok, state, {:continue, :init_workers}}
  end

  @impl true
  def handle_continue(:init_workers, state) do
    new_state = reconcile(state.target, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:set_target, target}, state) do
    target = min(target, state.config.max_workers)
    {:noreply, reconcile(target, %{state | target: target})}
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
  def handle_cast(:worker_ready, state) do
    new_pending = max(0, state.pending_count - 1)
    {:noreply, %{state | pending_count: new_pending}}
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
            {:noreply, reconcile(state.target, new_state, :recovery)}

          {:error, :too_many_crashes} ->
            {:stop, :reached_max_restart_intensity, state}
        end
    end
  end

  # --- Private ---

  defp check_intensity(state) do
    now = System.monotonic_time(:second)
    cutoff = now - state.config.max_period

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
    needed = target - (active_count + state.pending_count)

    if needed > 0 do
      # If no reason was explicitly provided, infer it from current pool state
      start_reason =
        reason || (if active_count == 0 and state.pending_count == 0, do: :initial, else: :scale_up)

      new_workers =
        Enum.reduce(1..needed, state.workers, fn _, acc ->
          case start_worker(state.config, start_reason) do
            {:ok, pid} -> MapSet.put(acc, pid)
            _ -> acc
          end
        end)

      %{state | workers: new_workers, pending_count: state.pending_count + needed}
    else
      # Scale-down is handled by the Pool calling stop_worker/2 when
      # workers check in, or we could proactively kill idle workers here.
      # For now, we follow the "drain" strategy where Pool dismisses them.
      state
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

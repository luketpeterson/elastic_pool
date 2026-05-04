defmodule ElasticPool.ScalingManager do
  @moduledoc """
  The brain of the pool. Manages the worker lifecycle and scaling decisions.
  Acts as the direct supervisor for all workers by trapping exits.
  """
  use GenServer
  require Logger

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

    target = min(config.baseline_workers, config.max_workers)

    state = %{
      config: config,
      pool_name: config.name,
      target: target,
      workers: MapSet.new(),
      pending_count: 0
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
        Logger.error("[ScalingManager] Worker #{inspect(pid)} crashed: #{inspect(reason)}. Recovering...")
        # Instant Recovery: Reconcile immediately to hit target
        {:noreply, reconcile(state.target, state)}
    end
  end

  # --- Private ---

  defp reconcile(target, state) do
    active_count = MapSet.size(state.workers)
    needed = target - (active_count + state.pending_count)

    if needed > 0 do
      Logger.info("[ScalingManager] Scaling up: target=#{target}, active=#{active_count}, starting=#{needed}")

      new_workers = Enum.reduce(1..needed, state.workers, fn _, acc ->
        case start_worker(state.config) do
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

  defp start_worker(config) do
    worker_args = [
      handler: config.worker_handler,
      pool: config.pool,
      manager: config.manager
    ] ++ config.worker_args

    # Link directly to the manager so we can trap exits
    ElasticPool.Worker.start_link(worker_args)
  end
end

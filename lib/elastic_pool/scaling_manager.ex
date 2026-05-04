defmodule ElasticPool.ScalingManager do
  @moduledoc """
  Generic coordinator for scaling. Orchestrates worker creation/destruction
  based on target counts from the Pool.
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

  # --- Callbacks ---

  @impl true
  def init(config) do
    {:ok, %{
      pending_count: 0,
      pool_name: config.name,
      supervisor: config.supervisor,
      config: config
    }}
  end

  @impl true
  def handle_cast({:set_target, target}, state) do
    current_total = ElasticPool.target_workers(state.pool_name)
    # Reconcile: How many do we need to start to hit the target, 
    # accounting for those already in the process of starting?
    needed = target - (current_total + state.pending_count)

    if needed > 0 do
      Logger.info("[ScalingManager] Target is #{target}. Starting #{needed} worker(s) (Pending: #{state.pending_count})")
      perform_scale_up(needed, state)
      {:noreply, %{state | pending_count: state.pending_count + needed}}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_cast(:worker_ready, state) do
    new_pending = max(0, state.pending_count - 1)
    {:noreply, %{state | pending_count: new_pending}}
  end

  defp perform_scale_up(count, state) do
    manager_pid = self()

    for _ <- 1..count do
      Task.start(fn ->
        case ElasticPool.WorkerSupervisor.start_worker(state.supervisor, state.config) do
          {:ok, _pid} -> :ok
          {:error, reason} ->
            Logger.error("[ScalingManager] Failed to start worker: #{inspect(reason)}")
            GenServer.cast(manager_pid, :worker_ready)
        end
      end)
    end
  end
end

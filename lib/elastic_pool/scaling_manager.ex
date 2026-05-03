defmodule ElasticPool.ScalingManager do
  @moduledoc """
  Generic coordinator for scaling. Orchestrates worker creation/destruction
  based on decisions from a pluggable ScalingPolicy.
  """
  use GenServer
  require Logger

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: config.manager)
  end

  def request_scale_up(manager) do
    GenServer.cast(manager, :evaluate)
  end

  def worker_ready(manager) do
    GenServer.cast(manager, :worker_ready)
  end

  # --- Callbacks ---

  @impl true
  def init(config) do
    policy_mod = config.scaling_policy
    # Pass a unified map with clear namespaces
    policy_state = policy_mod.init(%{
      policy_opts: config.scaling_policy_opts,
      pool_config: config
    })

    {:ok, %{
      starting: false,
      pool_name: config.name,
      supervisor: config.supervisor,
      config: config,
      policy_mod: policy_mod,
      policy_state: policy_state
    }}
  end

  @impl true
  def handle_cast(:evaluate, state) do
    if state.starting do
      {:noreply, state}
    else
      stats = %{
        total_workers: ElasticPool.total_workers(state.pool_name),
        available_workers: ElasticPool.available_workers(state.pool_name),
        peak_workers: ElasticPool.peak_workers(state.pool_name),
        waiting_clients: ElasticPool.waiting_clients(state.pool_name)
      }

      case state.policy_mod.handle_stats(stats, state.policy_state) do
        {:scale_up, count, new_policy_state} ->
          perform_scale_up(count, state)
          {:noreply, %{state | starting: true, policy_state: new_policy_state}}

        {:scale_down, count, new_policy_state} ->
          Logger.info("[ScalingManager] Policy requested scale down of #{count} workers (not implemented)")
          {:noreply, %{state | policy_state: new_policy_state}}

        {:none, new_policy_state} ->
          {:noreply, %{state | policy_state: new_policy_state}}
      end
    end
  end

  @impl true
  def handle_cast(:worker_ready, state) do
    {:noreply, %{state | starting: false}}
  end

  defp perform_scale_up(count, state) do
    Logger.info("[ScalingManager] Scaling up #{count} worker(s)")
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

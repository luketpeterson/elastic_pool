defmodule ElasticPool.ScalingManager do
  @moduledoc """
  Global gatekeeper for scaling. Ensures only one worker is spinning up
  at a time and respects cooldowns.
  """
  use GenServer
  require Logger

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: config.manager)
  end

  def request_scale_up(manager) do
    GenServer.cast(manager, :request_scale_up)
  end

  def worker_ready(manager) do
    GenServer.cast(manager, :worker_ready)
  end

  # --- Callbacks ---

  @impl true
  def init(config) do
    {:ok, %{
      starting: false,
      last_scale_time: System.monotonic_time(:millisecond) - config.cooldown_ms,
      cooldown_ms: config.cooldown_ms,
      max_workers: config.max_workers,
      scale_threshold: config.scale_threshold,
      pool_name: config.name,
      supervisor: config.supervisor,
      config: config
    }}
  end

  @impl true
  def handle_cast(:request_scale_up, state) do
    now = System.monotonic_time(:millisecond)
    cooldown_passed = (now - state.last_scale_time) > state.cooldown_ms

    cond do
      state.starting ->
        {:noreply, state}

      not cooldown_passed ->
        {:noreply, state}

      true ->
        total_ready = ElasticPool.total_workers(state.pool_name)
        waiting = ElasticPool.waiting_clients(state.pool_name)

        if total_ready < state.max_workers and waiting >= state.scale_threshold do
          Logger.info("[ScalingManager] Scaling up. Ready: #{total_ready}, Waiting: #{waiting}")

          # Emit high-signal scaling event
          :telemetry.execute([:elastic_pool, :pool, :scale_up],
            %{total_workers: total_ready + 1},
            %{pool_name: state.pool_name}
          )

          manager_pid = self()
          Task.start(fn ->
            case ElasticPool.WorkerSupervisor.start_worker(state.supervisor, state.config) do
              {:ok, _pid} -> :ok
              {:error, reason} ->
                Logger.error("[ScalingManager] Failed to start worker: #{inspect(reason)}")
                GenServer.cast(manager_pid, :worker_ready)
            end
          end)
          {:noreply, %{state | starting: true, last_scale_time: now}}
        else
          {:noreply, state}
        end
    end
  end

  @impl true
  def handle_cast(:worker_ready, state) do
    {:noreply, %{state | starting: false}}
  end
end

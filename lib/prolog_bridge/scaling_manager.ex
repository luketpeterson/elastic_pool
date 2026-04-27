defmodule PrologBridge.ScalingManager do
  @moduledoc """
  Global gatekeeper for scaling. Ensures only one worker is spinning up
  at a time and respects cooldowns.
  """
  use GenServer
  require Logger

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  def request_scale_up do
    GenServer.cast(__MODULE__, :request_scale_up)
  end

  def worker_ready do
    GenServer.cast(__MODULE__, :worker_ready)
  end

  # --- Callbacks ---

  @impl true
  def init(config) do
    {:ok, %{
      starting: false,
      last_scale_time: System.monotonic_time(:millisecond) - config.cooldown_ms,
      cooldown_ms: config.cooldown_ms,
      max_workers: config.max_workers,
      scale_threshold: config.scale_threshold
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
        # Read from ETS - fast and non-blocking
        [{:total_ready, total_ready}] = :ets.lookup(:prolog_pool_stats, :total_ready)
        [{:waiting_clients, waiting}] = :ets.lookup(:prolog_pool_stats, :waiting_clients)

        if total_ready < state.max_workers and waiting >= state.scale_threshold do
          Logger.info("[ScalingManager] Scaling up. Ready: #{total_ready}, Waiting: #{waiting} (Threshold: #{state.scale_threshold})")
          Task.start(fn ->
            case PrologBridge.WorkerSupervisor.start_worker() do
              {:ok, _pid} -> :ok
              {:error, reason} -> 
                Logger.error("[ScalingManager] Failed to start worker: #{inspect(reason)}")
                GenServer.cast(__MODULE__, :worker_ready)
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

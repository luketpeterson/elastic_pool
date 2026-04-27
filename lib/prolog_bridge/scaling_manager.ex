defmodule PrologBridge.ScalingManager do
  @moduledoc """
  Global gatekeeper for scaling. Ensures only one worker is spinning up
  at a time and respects cooldowns.
  """
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def request_scale_up do
    GenServer.cast(__MODULE__, :request_scale_up)
  end

  def worker_ready do
    GenServer.cast(__MODULE__, :worker_ready)
  end

  @doc """
  Returns the total number of ready workers in the Pool.
  """
  def total_ready_count do
    # Pool.status returns %{size: size}
    %{size: size} = PrologBridge.Pool.status()
    size
  end

  # --- Callbacks ---

  @impl true
  def init(_opts) do
    {:ok, %{
      starting: false,
      last_scale_time: 0,
      cooldown_ms: 500
    }}
  end

  @impl true
  def handle_cast(:request_scale_up, state) do
    now = System.monotonic_time(:millisecond)
    cooldown_passed = (now - state.last_scale_time) > state.cooldown_ms

    cond do
      state.starting ->
        Logger.debug("[ScalingManager] Already starting a worker, skipping")
        {:noreply, state}

      not cooldown_passed ->
        Logger.debug("[ScalingManager] Cooldown not passed, skipping")
        {:noreply, state}

      true ->
        total_ready = total_ready_count()
        max_workers = Application.get_env(:prolog_bridge, :pool_max_workers, 16)

        if total_ready < max_workers do
          Logger.info("[ScalingManager] Scaling up. Total ready: #{total_ready}")
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
          Logger.debug("[ScalingManager] Max workers reached (#{max_workers}), skipping")
          {:noreply, state}
        end
    end
  end

  @impl true
  def handle_cast(:worker_ready, state) do
    {:noreply, %{state | starting: false}}
  end
end

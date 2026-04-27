defmodule PrologBridge.ScalingManager do
  @moduledoc """
  Global gatekeeper for scaling. Ensures only one worker is spinning up 
  across all partitions.
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

  # --- Callbacks ---

  @impl true
  def init(opts) do
    {:ok, %{
      starting: false,
      last_scale_time: 0,
      cooldown_ms: 500 # Wait at least 500ms between scale-ups
    }}
  end

  @impl true
  def handle_cast(:request_scale_up, state) do
    now = System.monotonic_time(:millisecond)
    if not state.starting and (now - state.last_scale_time) > state.cooldown_ms do
      # Double check global worker count vs max
      total_ready = PrologBridge.WorkerPool.total_ready_count()
      max_workers = Application.get_env(:prolog_bridge, :pool_max_workers, 16)

      if total_ready < max_workers do
        Logger.info("[ScalingManager] Scaling up. Total ready: #{total_ready}")
        Task.start(fn ->
          case PrologBridge.WorkerSupervisor.start_worker() do
            {:ok, _pid} -> :ok
            {:error, _} -> GenServer.cast(__MODULE__, :worker_ready)
          end
        end)
        {:noreply, %{state | starting: true, last_scale_time: now}}
      else
        {:noreply, state}
      end
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_cast(:worker_ready, state) do
    {:noreply, %{state | starting: false}}
  end
end

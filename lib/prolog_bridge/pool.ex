defmodule PrologBridge.Pool do
  @moduledoc """
  A simple queue-based pool for Prolog workers.
  """
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def checkout(timeout \\ :infinity) do
    GenServer.call(__MODULE__, :checkout, timeout)
  end

  def checkin(worker_pid) do
    GenServer.cast(__MODULE__, {:checkin, worker_pid})
  end

  def worker_ready(worker_pid) do
    GenServer.cast(__MODULE__, {:checkin, worker_pid})
  end

  def status do
    GenServer.call(__MODULE__, :status)
  end

  # --- Callbacks ---

  @impl true
  def init(_opts) do
    {:ok, %{
      available: [],
      waiting: :queue.new(),
      monitors: %{}, # pid -> ref
      peak_workers: 0
    }}
  end

  @impl true
  def handle_call(:checkout, from, state) do
    case state.available do
      [pid | rest] ->
        {:reply, {:ok, nil, pid}, %{state | available: rest}}

      [] ->
        # Trigger scaling and queue the caller
        PrologBridge.ScalingManager.request_scale_up()
        {:noreply, %{state | waiting: :queue.in(from, state.waiting)}}
    end
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, %{
      size: map_size(state.monitors), 
      peak_workers: state.peak_workers,
      waiting_count: :queue.len(state.waiting)
    }, state}
  end

  @impl true
  def handle_cast({:checkin, pid}, state) do
    if Process.alive?(pid) do
      state = ensure_monitored(state, pid)
      new_peak = max(state.peak_workers, map_size(state.monitors))
      state = %{state | peak_workers: new_peak}
      
      case :queue.out(state.waiting) do
        {{:value, from}, rest} ->
          GenServer.reply(from, {:ok, nil, pid})
          {:noreply, %{state | waiting: rest}}

        {:empty, _} ->
          if pid in state.available do
            {:noreply, state}
          else
            {:noreply, %{state | available: [pid | state.available]}}
          end
      end
    else
      {:noreply, handle_down(state, pid)}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, handle_down(state, pid)}
  end

  # --- Private ---

  defp ensure_monitored(state, pid) do
    if Map.has_key?(state.monitors, pid) do
      state
    else
      ref = Process.monitor(pid)
      %{state | monitors: Map.put(state.monitors, pid, ref)}
    end
  end

  defp handle_down(state, pid) do
    new_monitors = Map.delete(state.monitors, pid)
    new_available = Enum.reject(state.available, &(&1 == pid))
    %{state | monitors: new_monitors, available: new_available}
  end
end

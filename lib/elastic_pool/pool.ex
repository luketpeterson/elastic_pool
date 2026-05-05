defmodule ElasticPool.Pool do
  @moduledoc """
  A queue-based pool for workers that takes worker resource cost and startup latency
  into account
  """
  use GenServer
  require Logger

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: config.pool)
  end

  def checkout(pool, timeout \\ :infinity) do
    GenServer.call(pool, :checkout, timeout)
  end

  def checkin(pool, worker_pid) do
    GenServer.cast(pool, {:checkin, worker_pid})
  end

  def worker_ready(pool, worker_pid) do
    GenServer.cast(pool, {:worker_ready, worker_pid})
  end

  # --- Callbacks ---
  @impl true
  def init(config) do
    policy_mod = config.scaling_policy
    policy_state = policy_mod.init(%{
      policy_opts: config.scaling_policy_opts,
      pool_config: config
    })

    state = %{
      available: [],
      waiting: :queue.new(),
      monitors: %{}, # pid -> ref
      peak_workers: 0,
      manager: config.manager,
      stats_table: config.stats_table,
      pool_name: config.name,
      policy_mod: policy_mod,
      policy_state: policy_state,
      target_count: config.initial_workers
    }

    update_ets(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:checkout, from, state) do
    # Efficiently increment request count on every checkout attempt
    :ets.update_counter(state.stats_table, :request_count, {2, 1})

    case state.available do
      [pid | rest] ->
        new_state = %{state | available: rest}
        update_ets(new_state)
        new_state = evaluate_policy(:checkout_success, new_state)
        {:reply, {:ok, nil, pid}, new_state}

      [] ->
        new_waiting = :queue.in(from, state.waiting)
        new_state = %{state | waiting: new_waiting}
        update_ets(new_state)
        new_state = evaluate_policy(:checkout_failed, new_state)
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_cast({:checkin, pid}, state) do
    {:noreply, do_checkin(pid, :checkin, state)}
  end

  @impl true
  def handle_cast({:worker_ready, pid}, state) do
    {:noreply, do_checkin(pid, :worker_ready, state)}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    new_state = handle_down(state, pid)
    update_ets(new_state)
    {:noreply, evaluate_policy(:checkin, new_state)}
  end

  # --- Private ---

  defp do_checkin(pid, event, state) do
    # Evaluate policy to get latest target
    state = evaluate_policy(event, state)

    active_count = map_size(state.monitors)

    if state.target_count < active_count do
      # Drain: We are over target, so don't put this worker back in rotation.
      # Dismiss it locally and tell the manager to stop it.
      new_state = dismiss_worker_locally(pid, state)
      ElasticPool.WorkerManager.stop_worker(state.manager, pid)
      new_state
    else
      # Normal Checkin logic
      if Process.alive?(pid) do
        state = ensure_monitored(state, pid)
        new_peak = max(state.peak_workers, map_size(state.monitors))
        state = %{state | peak_workers: new_peak}

        case :queue.out(state.waiting) do
          {{:value, from}, rest} ->
            GenServer.reply(from, {:ok, nil, pid})
            new_state = %{state | waiting: rest}
            update_ets(new_state)
            new_state

          {:empty, _} ->
            if pid in state.available do
              state
            else
              new_state = %{state | available: [pid | state.available]}
              update_ets(new_state)
              new_state
            end
        end
      else
        new_state = handle_down(state, pid)
        update_ets(new_state)
        new_state
      end
    end
  end

  defp dismiss_worker_locally(pid, state) do
    if ref = Map.get(state.monitors, pid) do
      Process.demonitor(ref)
    end

    new_monitors = Map.delete(state.monitors, pid)
    new_available = Enum.reject(state.available, &(&1 == pid))
    new_state = %{state | monitors: new_monitors, available: new_available}
    update_ets(new_state)
    new_state
  end

  defp evaluate_policy(event, state) do
    {target, new_policy_state} =
      state.policy_mod.handle_event(event, state.pool_name, state.policy_state)

    state = %{state | policy_state: new_policy_state}

    if target != state.target_count do
      ElasticPool.WorkerManager.set_target(state.manager, target)
      # Update the intent (target) stat immediately
      :ets.insert(state.stats_table, {:target_workers, target})

      # Handle immediate scale-down if we have idle workers
      active_count = map_size(state.monitors)

      if target < active_count do
        to_dismiss_count = active_count - target

        # We can only dismiss workers that are currently idle (available)
        to_dismiss_immediate_count = min(to_dismiss_count, length(state.available))
        {to_dismiss, _remaining} = Enum.split(state.available, to_dismiss_immediate_count)

        # Dismiss them locally and notify Manager
        Enum.reduce(to_dismiss, %{state | target_count: target}, fn pid, acc ->
          acc = dismiss_worker_locally(pid, acc)
          ElasticPool.WorkerManager.stop_worker(state.manager, pid)
          acc
        end)
      else
        %{state | target_count: target}
      end
    else
      state
    end
  end

  defp update_ets(state) do
    stats = [
      active_workers: map_size(state.monitors),
      available_workers: length(state.available),
      peak_workers: state.peak_workers,
      waiting_clients: :queue.len(state.waiting)
    ]

    :ets.insert(state.stats_table, stats)
    state
  end

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

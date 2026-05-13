defmodule ElasticPool.Pool.Atomic do
  @moduledoc false
  import ElasticPool.Atomics

  # Max iterations to spin-retry when the atomic counter and ETS are temporarily out of sync
  @max_spin 100

  @spec checkout(atom(), timeout()) :: {:ok, reference() | nil, pid()} | {:error, :timeout | :no_workers}
  def checkout(pool_name, timeout \\ :infinity) do
    case get_config(pool_name) do
      nil -> {:error, :no_workers}
      config ->
        atomics = config.atomics

        # Track request count and sample for policy
        req_idx = :atomics.add_get(atomics, request_idx(), 1)
        if rem(req_idx, config.sampling_rate) == 0 do
          notify_manager(config.manager, {:checkout_sample, weight: config.sampling_rate})
        end

        case :atomics.add_get(atomics, score_idx(), -1) do
          s when s >= 0 ->
            # Transition to Saturation: If we just hit exactly 0 idle workers,
            # but the request is being satisfied, we're at the edge.
            # (Technically saturation starts when we hit -1, handled in wait_for_worker)
            case take_worker_spin(pool_name, @max_spin) do
              {:ok, pid} ->
                {:ok, nil, pid}

              :error ->
                # Atomic counter and ETS are out of sync.
                :atomics.add(atomics, score_idx(), 1)
                wait_for_worker(pool_name, config, timeout)
            end

          -1 ->
            # Saturation Regime: The moment the first client has to wait
            notify_manager(config.manager, :saturation_regime)
            wait_for_worker(pool_name, config, timeout)

          _s ->
            # Slow path: Pool is already empty
            wait_for_worker(pool_name, config, timeout)
        end
    end
  end

  @spec checkin(atom(), pid()) :: :ok
  def checkin(pool_name, worker_pid) do
    case get_config(pool_name) do
      nil -> :ok
      config ->
        atomics = config.atomics

        # Track completion count and sample for policy
        comp_idx = :atomics.add_get(atomics, completion_idx(), 1)
        if rem(comp_idx, config.sampling_rate) == 0 do
          notify_manager(config.manager, {:checkin_sample, weight: config.sampling_rate})
        end

        case :atomics.add_get(atomics, score_idx(), 1) do
          s when s <= 0 ->
            # Handoff path: A client is waiting in ETS
            case take_client_spin(pool_name, @max_spin) do
              {:ok, {ref, client_pid}} ->
                send(client_pid, {:elastic_pool, :worker, ref, worker_pid})
                :ok

              :error ->
                # Atomic counter and ETS are out of sync. Treat worker as idle.
                # Correction: if we were supposed to hand off but failed, we might enter idle regime
                put_worker_idle(pool_name, worker_pid)
                :ok
            end

          1 ->
            # Idle Regime: The moment the last waiting client is satisfied
            notify_manager(config.manager, :idle_regime)
            put_worker_idle(pool_name, worker_pid)
            :ok

          _s ->
            # Idle path: No one is waiting
            put_worker_idle(pool_name, worker_pid)
            :ok
        end
    end
  end

  @spec add_worker(atom(), pid()) :: :ok
  def add_worker(pool_name, worker_pid) do
    # New workers use the same checkin logic to enter rotation
    checkin(pool_name, worker_pid)
  end

  @doc false
  def setup_pool(config) do
    name = config.name
    available_table = Module.concat(name, AvailableWorkers)
    waiting_table = Module.concat(name, WaitingClients)

    for {t, type} <- [{available_table, :set}, {waiting_table, :ordered_set}] do
      if :ets.whereis(t) == :undefined do
        :ets.new(t, [:public, type, :named_table, read_concurrency: true])
      else
        :ets.delete_all_objects(t)
      end
    end

    :ok
  end

  @spec dismiss_workers(atom(), non_neg_integer()) :: :ok
  def dismiss_workers(_pool_name, 0), do: :ok
  def dismiss_workers(pool_name, n) do
    case get_config(pool_name) do
      nil -> :ok
      config ->
        atomics = config.atomics

        # The "Third-Party Thief" logic:
        # Act like a client to safely take a worker for termination
        case :atomics.add_get(atomics, score_idx(), -1) do
          s when s >= 0 ->
            case take_worker_spin(pool_name, @max_spin) do
              {:ok, pid} ->
                Process.exit(pid, :normal)
                dismiss_workers(pool_name, n - 1)
              :error ->
                # Zombie write correction
                :atomics.add(atomics, score_idx(), 1)
                :ok
            end
          _s ->
            # No idle workers to kill, undo reservation
            :atomics.add(atomics, score_idx(), 1)
            :ok
        end
    end
  end

  @spec worker_exit(atom(), pid()) :: :ok
  def worker_exit(pool_name, pid) do
    case get_config(pool_name) do
      nil -> :ok
      config ->
        atomics = config.atomics
        table = Module.concat(pool_name, AvailableWorkers)

        # Atomic cleanup from available list:
        # If the worker was in the available table, it was idle.
        # We must take it atomically to avoid racing with a concurrent checkout.
        case :ets.take(table, pid) do
          [{^pid}] ->
            # It was truly idle, so we decrement the score
            :atomics.add(atomics, score_idx(), -1)
          [] ->
            # It was busy (checked out), so the score was already decremented by checkout
            :ok
        end
    end
  end

  @doc false
  def pool_children(_config), do: []

  # --- Internal Logic ---

  defp get_config(pool_name) do
    # In this architecture, the config is stored in the manager's state,
    # but for high-performance access in the pool, we retrieve it from the stats table.
    # Note: We can optimize this by only looking up the atomics and manager once.
    # For now, we fetch the atomics reference from ETS.
    if :ets.whereis(pool_name) == :undefined do
      nil
    else
      %{
        atomics: :ets.lookup_element(pool_name, :atomics, 2),
        manager: Module.concat(pool_name, WorkerManager),
        sampling_rate: :ets.lookup_element(pool_name, :sampling_rate, 2)
      }
    end
  end

  defp notify_manager(manager, event) do
    GenServer.cast(manager, {:policy_event, event})
  end

  defp get_atomics(pool_name) do
    case :ets.whereis(pool_name) do
      :undefined -> nil
      _ -> :ets.lookup_element(pool_name, :atomics, 2)
    end
  end

  defp take_worker_spin(_pool_name, 0), do: :error

  defp take_worker_spin(pool_name, limit) do
    table = Module.concat(pool_name, AvailableWorkers)

    case :ets.first(table) do
      :"$end_of_table" ->
        take_worker_spin(pool_name, limit - 1)

      pid when is_pid(pid) ->
        case :ets.take(table, pid) do
          [{^pid}] ->
            if Process.alive?(pid) do
              {:ok, pid}
            else
              # Worker died between ETS insertion and take.
              case get_atomics(pool_name) do
                nil -> :error
                atomics ->
                  :atomics.add(atomics, score_idx(), 1)
                  take_worker_spin(pool_name, limit - 1)
              end
            end

          [] ->
            # Race: Someone else took it
            take_worker_spin(pool_name, limit - 1)
        end
    end
  end

  defp take_client_spin(_pool_name, 0), do: :error

  defp take_client_spin(pool_name, limit) do
    table = Module.concat(pool_name, WaitingClients)

    case :ets.first(table) do
      :"$end_of_table" ->
        take_client_spin(pool_name, limit - 1)

      ref ->
        case :ets.take(table, ref) do
          [{^ref, client_pid}] -> {:ok, {ref, client_pid}}
          [] -> take_client_spin(pool_name, limit - 1)
        end
    end
  end

  defp wait_for_worker(pool_name, config, timeout) do
    table = Module.concat(pool_name, WaitingClients)
    ref = make_ref()
    :ets.insert(table, {ref, self()})

    receive do
      {:elastic_pool, :worker, ^ref, worker_pid} ->
        {:ok, nil, worker_pid}
    after
      timeout ->
        # Cleanup on timeout
        :ets.delete(table, ref)

        # Atomic Correction: We "undo" our reservation.
        :atomics.add(config.atomics, score_idx(), 1)
        {:error, :timeout}
    end
  end

  defp put_worker_idle(pool_name, worker_pid) do
    table = Module.concat(pool_name, AvailableWorkers)
    :ets.insert(table, {worker_pid})
  end
end

defmodule ElasticPool.WorkerManager do
  @moduledoc false

  # Internal: The supervisor and lifecycle manager for all worker processes in the pool.
  #
  # WorkerManager is responsible for:
  # - Starting new worker processes to meet scaling targets.
  # - Stopping worker processes when requested by the Pool.
  # - Monitoring workers and performing instant recovery if they crash.
  # - Acting as the direct supervisor for all workers by trapping exits.

  @doc false
  defmacro __monomorphize__(worker_mod, policy_mod) do
    quote bind_quoted: [worker_mod: worker_mod, policy_mod: policy_mod] do
      use GenServer
      require Logger
      require ElasticPool
      import ElasticPool.Atomics

      @worker_mod worker_mod
      @policy_mod policy_mod

      def start_link(config) do
        GenServer.start_link(__MODULE__, config, name: config.manager)
      end

      def wait_for_ready(manager, count, timeout) do
        GenServer.call(manager, {:wait_for_ready, count}, timeout)
      end

      def worker_ready(manager, pid) do
        GenServer.cast(manager, {:worker_ready, pid})
      end

      # --- Callbacks ---

      @impl true
      def init(config) do
        # Trap exits so we are notified when workers crash
        Process.flag(:trap_exit, true)

        target = min(config.initial_workers, config.max_workers)

        # Initialize Policy State
        policy_state =
          @policy_mod.init(%{
            policy_opts: config.scaling_policy_opts,
            pool_config: config
          })

        state = %{
          # The full pool configuration (max_workers, handler, etc.)
          config: config,
          # Atom name of the pool for identification in logs and telemetry
          pool_name: config.name,
          # Cached access to the atomic state
          atomics: config.atomics,
          target: target,
          # Set of all physical worker PIDs currently linked to this manager
          workers: MapSet.new(),
          # List of monotonic timestamps of recent worker crashes for intensity tracking
          restarts: [],
          # List of {from, target_count} clients waiting for initial boot-up
          waiting_readiness: [],
          # State of scaling policy
          policy_state: policy_state
        }

        # Schedule periodic policy evaluation (every 100ms)
        :timer.send_interval(100, :policy_heartbeat)

        # Initial scale-up to baseline
        {:ok, state, {:continue, :init_workers}}
      end

      @impl true
      def handle_continue(:init_workers, state) do
        case reconcile(state.target, state) do
          {:ok, new_state} -> {:noreply, new_state}
          {:error, :too_many_crashes} -> {:stop, :reached_max_restart_intensity, state}
        end
      end

      @impl true
      def handle_call({:wait_for_ready, count}, from, state) do
        current_count = :atomics.get(state.atomics, active_idx())

        if current_count >= count do
          {:reply, :ok, state}
        else
          {:noreply, %{state | waiting_readiness: [{from, count} | state.waiting_readiness]}}
        end
      end

      @impl true
      def handle_cast({:worker_ready, pid}, state) do
        # Update atomics for active/peak
        new_active = :atomics.add_get(state.atomics, active_idx(), 1)
        update_peak(state.atomics, new_active)

        # Evaluate policy
        state = evaluate_policy(:worker_ready, state)

        # Register with Pool rotation
        ElasticPool.Pool.add_worker(state.config.pool, pid)

        # Check readiness waiters
        current_count = :atomics.get(state.atomics, active_idx())
        remaining_waiting =
          Enum.reduce(state.waiting_readiness, [], fn {from, count}, acc ->
            if current_count >= count do
              GenServer.reply(from, :ok)
              acc
            else
              [{from, count} | acc]
            end
          end)

        {:noreply, %{state | waiting_readiness: remaining_waiting}}
      end

      @impl true
      def handle_cast({:policy_event, event}, state) do
        {:noreply, evaluate_policy(event, state)}
      end

      @impl true
      def handle_info(:policy_heartbeat, state) do
        {:noreply, evaluate_policy(:periodic, state)}
      end

      @impl true
      def handle_info({:EXIT, pid, reason}, state) do
        new_workers = MapSet.delete(state.workers, pid)
        state = %{state | workers: new_workers}

        # Update active count
        :atomics.add(state.atomics, active_idx(), -1)

        # Background Scan: Remove dead worker from whichever shard it was in
        tables = :ets.lookup_element(state.pool_name, :tables, 2)
        for i <- 0..(ElasticPool.Atomics.num_shards() - 1) do
          table = elem(tables.available, i)
          if :ets.delete_object(table, {pid}) do
            # Found it! Decrement the shard score and stop scanning
            :atomics.add(state.atomics, score_idx(i + 1), -1)
          end
        end

        state = evaluate_policy(:worker_exit, state)

        case reason do
          :normal ->
            # Even on normal exit, reconcile to ensure we hit target
            case reconcile(state.target, state) do
              {:ok, final_state} -> {:noreply, final_state}
              {:error, :too_many_crashes} ->
                {:stop, :shutdown, state}
            end

          _other ->
            case check_intensity(state) do
              {:ok, new_state} ->
                case reconcile(state.target, new_state, :recovery) do
                  {:ok, final_state} -> {:noreply, final_state}
                  {:error, :too_many_crashes} ->
                    # Stop the entire supervisor tree
                    {:stop, :shutdown, state}
                end

              {:error, :too_many_crashes} ->
                # Stop the entire supervisor tree
                {:stop, :shutdown, state}
            end
        end
      end

      # --- Private ---

      defp apply_target(new_target, state) do
        # Always update target in atomics
        :atomics.put(state.atomics, target_idx(), new_target)

        state = %{state | target: new_target}
        active_count = :atomics.get(state.atomics, active_idx())

        if new_target < active_count do
          # Scale down: acquire workers from the idle pool
          to_kill = active_count - new_target
          kill_idle_workers(to_kill, state)
        end

        case reconcile(new_target, state) do
          {:ok, new_state} -> new_state
          {:error, :too_many_crashes} ->
            exit(:shutdown)
        end
      end

      defp kill_idle_workers(0, _state), do: :ok
      defp kill_idle_workers(n, state) do
        # Iterate through shards to find workers to kill
        kill_from_shard(n, state, 0)
      end

      defp kill_from_shard(0, _state, _shard), do: :ok
      defp kill_from_shard(n, state, shard) when shard < ElasticPool.Atomics.num_shards() do
        case :atomics.add_get(state.atomics, score_idx(shard + 1), -1) do
          s when s >= 0 ->
            table = state.config.available_table
            case :ets.take(table, shard) do
              [{^shard, pid}] ->
                Process.exit(pid, :normal)
                kill_from_shard(n - 1, state, shard)
              [] ->
                :atomics.add(state.atomics, score_idx(shard + 1), 1)
                kill_from_shard(n, state, shard + 1)
            end
          _s ->
            :atomics.add(state.atomics, score_idx(shard + 1), 1)
            kill_from_shard(n, state, shard + 1)
        end
      end
      defp kill_from_shard(_n, _state, _shard), do: :ok

      defp evaluate_policy(event, state) do
        # Aggregate scores across all shards
        total_score = 
          Enum.reduce(1..ElasticPool.Atomics.num_shards(), 0, fn i, acc ->
            acc + :atomics.get(state.atomics, i)
          end)

        # If we are polling and have waiting clients, treat it as a checkout failure
        event =
          if event == :periodic and total_score < 0 do
            :checkout_failed
          else
            event
          end

        {target, new_policy_state} =
          @policy_mod.handle_event(event, state.pool_name, state.policy_state)

        state = %{state | policy_state: new_policy_state}

        case target do
          :no_change -> state
          new_target ->
            new_target = min(new_target, state.config.max_workers)
            if new_target != state.target do
              apply_target(new_target, state)
            else
              state
            end
        end
      end


      defp update_peak(atomics, current) do
        peak = :atomics.get(atomics, peak_idx())
        if current > peak do
          case :atomics.compare_exchange(atomics, peak_idx(), peak, current) do
            :ok -> :ok
            _ -> update_peak(atomics, current)
          end
        end
      end

      defp check_intensity(state) do
        now = System.monotonic_time(:millisecond)
        # max_period is in seconds, so convert to milliseconds
        cutoff = now - state.config.max_period * 1_000

        # Filter out old restarts
        recent_restarts = [now | Enum.filter(state.restarts, &(&1 > cutoff))]

        if length(recent_restarts) > state.config.max_restarts do
          {:error, :too_many_crashes}
        else
          {:ok, %{state | restarts: recent_restarts}}
        end
      end

      defp reconcile(target, state, reason \\ nil) do
        active_count = MapSet.size(state.workers)
        needed = target - active_count

        if needed > 0 do
          # Calculate the start reason for this batch once
          # Ensures :initial doesn't flip to :scale_up mid-reconcile
          start_reason = reason || if active_count == 0, do: :initial, else: :scale_up

          case start_worker(state.config, start_reason) do
            {:ok, pid} ->
              reconcile(target, %{state | workers: MapSet.put(state.workers, pid)}, start_reason)

            _ ->
              case check_intensity(state) do
                {:ok, new_state} -> reconcile(target, new_state, start_reason)
                {:error, :too_many_crashes} -> {:error, :too_many_crashes}
              end
          end
        else
          {:ok, state}
        end
      end

      defp start_worker(config, reason) do
        # Link directly to the manager so we can trap exits
        worker_args = [pool: config.pool, manager: config.manager, start_reason: reason] ++ config.worker_args
        @worker_mod.start_link(worker_args)
      end
    end
  end

  def wait_for_ready(manager, count, timeout) do
    GenServer.call(manager, {:wait_for_ready, count}, timeout)
  end

  def worker_ready(manager, pid) do
    GenServer.cast(manager, {:worker_ready, pid})
  end
end

defmodule ElasticPool.IntegrationTest do
  use ExUnit.Case

  defmodule TrackingWorker do
    use ElasticPool.Worker

    @impl true
    def init(args) do
      send(args[:test_pid], {:worker_init, self()})
      args
    end

    @impl true
    def handle_work(:ping, _from, state) do
      {:reply, :pong, state}
    end

    @impl true
    def terminate(_reason, state) do
      send(state[:test_pid], {:worker_terminated, self()})
      :ok
    end
  end

  defmodule ScheduledPolicy do
    @behaviour ElasticPool.ScalingPolicy

    @impl true
    def init(opts) do
      %{initial: opts.pool_config.initial_workers}
    end

    @impl true
    def handle_event(_event, pool, state) do
      count = ElasticPool.request_count(pool)

      target =
        cond do
          count >= 200 -> 5
          count >= 100 -> 20
          true -> state.initial
        end

      {target, state}
    end
  end

  defmodule OverTargetPolicy do
    @behaviour ElasticPool.ScalingPolicy

    @impl true
    def init(_opts), do: %{}

    @impl true
    def handle_event(_event, _pool, state) do
      {100, state}
    end
  end

  test "scaling up and down based on request count with lifecycle tracking" do
    name = :scheduled_scaling_test
    test_pid = self()

    {:ok, _pid} =
      ElasticPool.start_link(
        name: name,
        worker_handler: TrackingWorker,
        initial_workers: 2,
        scaling_policy: ScheduledPolicy,
        worker_args: [test_pid: test_pid]
      )

    # 1. Initial State: 2 workers
    assert ElasticPool.target_workers(name) == 2
    assert ElasticPool.active_workers(name) == 2
    assert ElasticPool.available_workers(name) == 2

    for _ <- 1..2 do
      assert_receive {:worker_init, _pid}
    end

    refute_receive {:worker_init, _}, 100

    # 2. Trigger Scale-Up: Send 100 requests
    for _ <- 1..100 do
      assert ElasticPool.call(name, :ping) == :pong
    end

    # Wait for policy to hit target
    wait_for_target(name, 20)
    # Wait for all 20 workers to be active AND idle
    wait_for_idle(name)

    assert ElasticPool.active_workers(name) == 20
    assert ElasticPool.available_workers(name) == 20

    # Check inits (exactly 18 new)
    for _ <- 1..18 do
      assert_receive {:worker_init, _pid}, 1000
    end

    refute_receive {:worker_init, _}, 100

    # 3. Trigger Scale-Down: Send 100 more requests (Total 200)
    for _ <- 1..100 do
      assert ElasticPool.call(name, :ping) == :pong
    end

    # Wait for policy to hit target
    wait_for_target(name, 5)
    # Wait for pool to settle at 5 workers and be idle
    wait_for_idle(name)

    assert ElasticPool.active_workers(name) == 5
    assert ElasticPool.available_workers(name) == 5

    # Check for exactly 15 termination messages from scale-down
    for _ <- 1..15 do
      assert_receive {:worker_terminated, _pid}, 1000
    end

    refute_receive {:worker_terminated, _}, 100

    # 4. Global Termination: Stop the whole pool
    Supervisor.stop(name)

    # The remaining 5 workers should all terminate
    for _ <- 1..5 do
      assert_receive {:worker_terminated, _pid}, 1000
    end

    refute_receive {:worker_terminated, _}, 100
  end

  test "WorkerManager survives scale-down" do
    name = :manager_survival_test
    test_pid = self()

    {:ok, _pid} =
      ElasticPool.start_link(
        name: name,
        worker_handler: TrackingWorker,
        initial_workers: 10,
        max_workers: 10,
        scaling_policy: ScheduledPolicy,
        worker_args: [test_pid: test_pid]
      )

    manager_name = Module.concat(name, WorkerManager)
    manager_pid = Process.whereis(manager_name)
    assert is_pid(manager_pid)

    # Monitor the manager
    ref = Process.monitor(manager_pid)

    # Trigger Scale-Down: Send 200 requests to hit the 5 worker target
    for _ <- 1..200 do
      ElasticPool.call(name, :ping)
    end

    # Wait for target and idle
    wait_for_target(name, 5)
    wait_for_idle(name)

    # If the manager crashed, we would receive a :DOWN message
    refute_receive {:DOWN, ^ref, :process, ^manager_pid, _reason},
                   1000,
                   "WorkerManager crashed during scale-down! This would mean our merged supervisor/manager is unstable."

    assert ElasticPool.active_workers(name) == 5

    Supervisor.stop(name)
  end

  test "WorkerManager enforces max_workers as a hard cap" do
    name = :max_worker_cap_test
    test_pid = self()

    {:ok, _pid} =
      ElasticPool.start_link(
        name: name,
        worker_handler: TrackingWorker,
        initial_workers: 1,
        max_workers: 3,
        scaling_policy: OverTargetPolicy,
        worker_args: [test_pid: test_pid]
      )

    assert_receive {:worker_init, _pid}, 1000

    for _ <- 1..10 do
      assert ElasticPool.call(name, :ping) == :pong
    end

    wait_for_idle(name)

    assert ElasticPool.active_workers(name) == 3
    assert ElasticPool.available_workers(name) == 3

    Supervisor.stop(name)
  end

  defmodule TrackingCrashingWorker do
    use ElasticPool.Worker

    @impl true
    def init(args) do
      send(args[:test_pid], {:worker_init, self()})
      args
    end

    @impl true
    def handle_work(:crash, _from, _state) do
      raise "Intentional Crash"
    end

    @impl true
    def handle_work(:ping, _from, state) do
      {:reply, :pong, state}
    end
  end

  test "instant recovery from worker crash" do
    name = :crash_recovery_test
    test_pid = self()

    {:ok, _pid} =
      ElasticPool.start_link(
        name: name,
        worker_handler: TrackingCrashingWorker,
        initial_workers: 1,
        worker_args: [test_pid: test_pid]
      )

    # 1. Capture the initial worker PID
    assert_receive {:worker_init, first_pid}
    assert ElasticPool.active_workers(name) == 1

    # 2. Trigger Crash
    # Use spawn so the test process doesn't crash from the linked worker
    spawn(fn -> ElasticPool.call(name, :crash) end)

    # 3. Prove Instant Recovery via message passing
    # The WorkerManager should start a new one immediately.
    assert_receive {:worker_init, second_pid}, 1000
    assert second_pid != first_pid

    # NEW: Deterministic Sync Barrier.
    # By calling :sys.get_state on the Pool, we guarantee that the
    # 'worker_ready' cast has been fully processed before we check the stats.
    :sys.get_state(Module.concat(name, Pool))

    # 4. Verify the new worker is functional and stats are correct
    assert ElasticPool.active_workers(name) == 1
    assert ElasticPool.call(name, :ping) == :pong

    Supervisor.stop(name)
  end

  defp wait_for_target(name, expected, retries \\ 100) do
    if ElasticPool.target_workers(name) == expected or retries == 0 do
      :ok
    else
      Process.sleep(10)
      wait_for_target(name, expected, retries - 1)
    end
  end

  defp wait_for_idle(name, retries \\ 100) do
    active = ElasticPool.active_workers(name)
    available = ElasticPool.available_workers(name)

    if (active > 0 and active == available) or retries == 0 do
      :ok
    else
      Process.sleep(10)
      wait_for_idle(name, retries - 1)
    end
  end
end

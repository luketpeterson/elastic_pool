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
  require ElasticPool

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

  # --- Test Pool Modules ---

  defmodule ScheduledPool do
    use ElasticPool,
      worker_handler: TrackingWorker,
      scaling_policy: ScheduledPolicy
  end

  defmodule OverTargetPool do
    use ElasticPool,
      worker_handler: TrackingWorker,
      scaling_policy: OverTargetPolicy
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

  defmodule CrashRecoveryPool do
    use ElasticPool,
      worker_handler: TrackingCrashingWorker
  end

  # --------------------------

  test "scaling up and down based on request count with lifecycle tracking" do
    test_pid = self()
    name = ScheduledPool

    # --- Setup Telemetry Tracking ---
    handler_id = "telemetry-integration-test-handler"
    events = [
      [:elastic_pool, :worker, :start],
      [:elastic_pool, :worker, :stop]
    ]
    :telemetry.attach_many(handler_id, events, &__MODULE__.handle_telemetry/4, %{test_pid: test_pid})

    on_exit(fn -> :telemetry.detach(handler_id) end)
    # -------------------------------

    {:ok, pid} = ScheduledPool.start_link(
      initial_workers: 2,
      worker_args: [test_pid: test_pid]
    )

    # 1. Initial State: 2 workers
    assert ScheduledPool.target_workers() == 2
    assert ScheduledPool.active_workers() == 2
    assert ScheduledPool.available_workers() == 2

    for _ <- 1..2 do
      assert_receive {:worker_init, _pid}
      assert_receive {:telemetry_event, [:elastic_pool, :worker, :start], %{count: 1}, %{start_reason: :initial}}
    end

    refute_receive {:worker_init, _}, 100

    # 2. Trigger Scale-Up: Send 100 requests
    for _ <- 1..100 do
      assert ScheduledPool.call(:ping) == :pong
    end

    # Wait for policy to hit target
    wait_for_target(name, 20)
    # Wait for all 20 workers to be active AND idle
    wait_for_idle(name)

    assert ScheduledPool.active_workers() == 20
    assert ScheduledPool.available_workers() == 20

    # Check inits (exactly 18 new)
    for _ <- 1..18 do
      assert_receive {:worker_init, _pid}, 1000
      assert_receive {:telemetry_event, [:elastic_pool, :worker, :start], %{count: 1}, %{start_reason: :scale_up}}
    end

    refute_receive {:worker_init, _}, 100

    # 3. Trigger Scale-Down: Send 100 more requests (Total 200)
    for _ <- 1..100 do
      assert ScheduledPool.call(:ping) == :pong
    end

    # Wait for policy to hit target
    wait_for_target(name, 5)
    # Wait for pool to settle at 5 workers and be idle
    wait_for_idle(name)

    assert ScheduledPool.active_workers() == 5
    assert ScheduledPool.available_workers() == 5

    # Check for exactly 15 termination messages from scale-down
    for _ <- 1..15 do
      assert_receive {:worker_terminated, _pid}, 1000
      assert_receive {:telemetry_event, [:elastic_pool, :worker, :stop], %{count: 1}, %{stop_reason: :scale_down}}
    end

    refute_receive {:worker_terminated, _}, 100

    # 4. Global Termination: Stop the whole pool
    Supervisor.stop(pid)

    # The remaining 5 workers should all terminate with :shutdown reason
    for _ <- 1..5 do
      assert_receive {:worker_terminated, _pid}, 1000
      assert_receive {:telemetry_event, [:elastic_pool, :worker, :stop], %{count: 1}, %{stop_reason: :shutdown}}
    end

    refute_receive {:worker_terminated, _}, 100
  end

  test "WorkerManager survives scale-down" do
    test_pid = self()
    name = ScheduledPool

    {:ok, pid} = ScheduledPool.start_link(
      initial_workers: 10,
      max_workers: 10,
      worker_args: [test_pid: test_pid]
    )

    manager_name = Module.concat(name, WorkerManager)
    manager_pid = Process.whereis(manager_name)
    assert is_pid(manager_pid)

    # Monitor the manager
    ref = Process.monitor(manager_pid)

    # Trigger Scale-Down: Send 200 requests to hit the 5 worker target
    for _ <- 1..200 do
      ScheduledPool.call(:ping)
    end

    # Wait for target and idle
    wait_for_target(name, 5)
    wait_for_idle(name)

    # If the manager crashed, we would receive a :DOWN message
    refute_receive {:DOWN, ^ref, :process, ^manager_pid, _reason},
                   1000,
                   "WorkerManager crashed during scale-down!"

    assert ScheduledPool.active_workers() == 5

    Supervisor.stop(pid)
  end

  test "WorkerManager enforces max_workers as a hard cap" do
    test_pid = self()
    name = OverTargetPool

    {:ok, pid} = OverTargetPool.start_link(
      initial_workers: 1,
      max_workers: 3,
      worker_args: [test_pid: test_pid]
    )

    assert_receive {:worker_init, _pid}, 1000

    for _ <- 1..10 do
      assert OverTargetPool.call(:ping) == :pong
    end

    wait_for_idle(name)

    assert OverTargetPool.active_workers() == 3
    assert OverTargetPool.available_workers() == 3

    Supervisor.stop(pid)
  end

  @tag :capture_log
  test "instant recovery from worker crash" do
    test_pid = self()
    name = CrashRecoveryPool

    # --- Setup Telemetry Tracking ---
    handler_id = "telemetry-crash-test-handler"

    :telemetry.attach_many(
      handler_id,
      [[:elastic_pool, :worker, :start], [:elastic_pool, :worker, :stop]],
      &__MODULE__.handle_telemetry/4,
      %{test_pid: test_pid}
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    # -------------------------------

    {:ok, pid} = CrashRecoveryPool.start_link(
      initial_workers: 1,
      worker_args: [test_pid: test_pid]
    )

    # 1. Capture the initial worker PID and telemetry
    assert_receive {:worker_init, first_pid}
    assert_receive {:telemetry_event, [:elastic_pool, :worker, :start], %{count: 1}, %{start_reason: :initial}}
    assert CrashRecoveryPool.active_workers() == 1

    # 2. Trigger Crash
    spawn(fn -> CrashRecoveryPool.call(:crash) end)

    # 3. Prove Instant Recovery via message passing and telemetry
    assert_receive {:telemetry_event, [:elastic_pool, :worker, :stop], %{count: 1}, %{stop_reason: :crash}}

    # The WorkerManager should start a new one immediately.
    assert_receive {:worker_init, second_pid}, 1000
    assert_receive {:telemetry_event, [:elastic_pool, :worker, :start], %{count: 1}, %{start_reason: :recovery}}
    assert second_pid != first_pid

    # NEW: Deterministic Sync Barrier.
    :sys.get_state(name)

    # 4. Verify the new worker is functional and stats are correct
    assert CrashRecoveryPool.active_workers() == 1
    assert CrashRecoveryPool.call(:ping) == :pong

    Supervisor.stop(pid)
  end

  defp wait_for_target(name, expected, retries \\ 100) do
    if name.target_workers() == expected or retries == 0 do
      :ok
    else
      Process.sleep(10)
      wait_for_target(name, expected, retries - 1)
    end
  end

  defp wait_for_idle(name, retries \\ 100) do
    active = name.active_workers()
    available = name.available_workers()

    if (active > 0 and active == available) or retries == 0 do
      :ok
    else
      Process.sleep(10)
      wait_for_idle(name, retries - 1)
    end
  end

  def handle_telemetry(name, measurements, metadata, config) do
    send(config.test_pid, {:telemetry_event, name, measurements, metadata})
  end
end

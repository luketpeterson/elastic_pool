defmodule ElasticPoolTest.TestWorker do
  use ElasticPool.Worker
  @impl true
  def handle_work(:ping, _from, state), do: {:reply, :pong, state}
end

defmodule ElasticPoolTest.SlowInitWorker do
  use ElasticPool.Worker

  @impl true
  def init(args) do
    Process.sleep(250)
    args
  end

  @impl true
  def handle_work(_, _, state), do: {:reply, :ok, state}
end

defmodule ElasticPoolTest.TerminationWorker do
  use ElasticPool.Worker

  @impl true
  def handle_work(_, _, state), do: {:reply, :ok, state}

  @impl true
  def terminate(_reason, state) do
    send(state[:test_pid], :worker_terminated)
    :ok
  end
end

defmodule ElasticPoolTest.FastCrashingWorker do
  use ElasticPool.Worker
  @impl true
  def init(_), do: raise("Instant Crash")

  @impl true
  def handle_work(_req, _from, state), do: {:reply, :ok, state}
end

defmodule ElasticPoolTest.WorkCrashingWorker do
  use ElasticPool.Worker
  @impl true
  def handle_work(:crash, _from, _state), do: raise("Work Crash")
  @impl true
  def handle_work(:ping, _from, state), do: {:reply, :pong, state}
end

# --- Test Pool Modules ---

defmodule BasicPool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.TestWorker
end

defmodule TerminationPool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.TerminationWorker
end

defmodule SlowStartupPool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.SlowInitWorker
end

defmodule PollerPool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.TestWorker
end

defmodule IntensityPool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.FastCrashingWorker
end

defmodule WorkIntensityPool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.WorkCrashingWorker
end

defmodule ElasticPoolTest.BadInitWorker do
  use ElasticPool.Worker
  @impl true
  def init(_args), do: {:stop, :init_failed}
  @impl true
  def handle_work(_req, _from, state), do: {:reply, :ok, state}
end

defmodule ImmediateFailurePool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.BadInitWorker
end

# --------------------------

defmodule ElasticPoolTest do
  use ExUnit.Case

  test "all workers are terminated when pool stops" do
    test_pid = self()

    {:ok, pid} =
      TerminationPool.start_link(
        initial_workers: 5,
        stats_interval: :never,
        worker_args: [test_pid: test_pid]
      )

    # Shutdown the pool
    Supervisor.stop(pid)

    # Check that we received exactly 5 termination messages
    for _ <- 1..5 do
      assert_receive :worker_terminated, 500
    end
  end

  test "initial workers init in parallel" do
    start_time = System.monotonic_time(:millisecond)

    {:ok, pid} = SlowStartupPool.start_link(initial_workers: 10)

    end_time = System.monotonic_time(:millisecond)
    peak_workers = SlowStartupPool.peak_workers()
    duration = end_time - start_time

    Supervisor.stop(pid)

    assert peak_workers == 10
    assert duration < 500, "Startup took too long: #{duration}ms"
  end

  test "can perform work via generic call" do
    {:ok, pid} = BasicPool.start_link(initial_workers: 1, stats_interval: :never)

    assert BasicPool.call(:ping) == :pong
    assert BasicPool.request_count() == 1
    Supervisor.stop(pid)
  end

  test "named instances of the same pool module are independent" do
    {:ok, pool_a_pid} =
      BasicPool.start_link(name: :pool_a, initial_workers: 1, stats_interval: :never)

    {:ok, pool_b_pid} =
      BasicPool.start_link(name: :pool_b, initial_workers: 3, stats_interval: :never)

    try do
      assert BasicPool.target_workers(:pool_a) == 1
      assert BasicPool.active_workers(:pool_a) == 1
      assert BasicPool.available_workers(:pool_a) == 1
      assert BasicPool.request_count(:pool_a) == 0

      assert BasicPool.target_workers(:pool_b) == 3
      assert BasicPool.active_workers(:pool_b) == 3
      assert BasicPool.available_workers(:pool_b) == 3
      assert BasicPool.request_count(:pool_b) == 0

      assert BasicPool.call(:pool_a, :ping, 5_000) == :pong
      assert BasicPool.call(:pool_a, :ping, 5_000) == :pong
      assert BasicPool.call(:pool_b, :ping, 5_000) == :pong

      :sys.get_state(:pool_a)
      :sys.get_state(:pool_b)

      assert BasicPool.request_count(:pool_a) == 2
      assert BasicPool.request_count(:pool_b) == 1

      assert BasicPool.active_workers(:pool_a) == 1
      assert BasicPool.available_workers(:pool_a) == 1
      assert BasicPool.active_workers(:pool_b) == 3
      assert BasicPool.available_workers(:pool_b) == 3
    after
      Supervisor.stop(pool_a_pid)
      Supervisor.stop(pool_b_pid)
    end
  end

  test "status shows workers" do
    {:ok, pid} = BasicPool.start_link(initial_workers: 1)

    assert BasicPool.target_workers() == 1
    Supervisor.stop(pid)
  end

  test "StatsPoller emits periodic telemetry" do
    test_pid = self()
    name = PollerPool

    # Attach a temporary telemetry handler using a named function to avoid warnings
    handler_id = "test-poller-handler"

    :telemetry.attach(
      handler_id,
      [:elastic_pool, :pool, :status],
      &__MODULE__.handle_telemetry/4,
      test_pid
    )

    {:ok, pool_pid} = PollerPool.start_link(initial_workers: 2, stats_interval: 100)

    # We should receive a heartbeat
    assert_receive {:telemetry_event, measurements, %{pool_name: ^name}}, 500
    assert measurements.target_workers == 2

    # Cleanup
    :telemetry.detach(handler_id)
    Supervisor.stop(pool_pid)
  end

  @tag :capture_log
  test "pool shuts down when crash intensity is reached during worker init" do
    Process.flag(:trap_exit, true)

    # This should crash immediately on start because initial_workers=1
    # but it will keep trying to reconcile until max_restarts (2) is hit.
    result =
      IntensityPool.start_link(
        initial_workers: 1,
        max_restarts: 2,
        max_period: 5,
        stats_interval: :never
      )

    # During init failure, start_link returns the error reason
    assert {:error, :supervisor_died} = result
  end

  @tag :capture_log
  test "pool shuts down when crash intensity is reached during work" do
    test_pid = self()
    Process.flag(:trap_exit, true)

    # --- Setup Telemetry Tracking ---
    handler_id = "telemetry-work-intensity-handler"

    :telemetry.attach_many(
      handler_id,
      [[:elastic_pool, :worker, :start]],
      &__MODULE__.handle_telemetry/4,
      %{test_pid: test_pid}
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    # -------------------------------

    {:ok, pid} =
      WorkIntensityPool.start_link(
        initial_workers: 1,
        max_restarts: 1,
        max_period: 5,
        stats_interval: :never
      )

    # We need to crash it 2 times to hit max_restarts: 1
    # 1. First crash - Call synchronously
    catch_exit(WorkIntensityPool.call(:crash))

    # Wait for the RECOVERY telemetry (Sent by the worker itself!)
    assert_receive {:telemetry_event, [:elastic_pool, :worker, :start], _,
                    %{start_reason: :recovery}},
                   1000

    # 2. Second crash - This should trigger the intensity limit
    catch_exit(WorkIntensityPool.call(:crash))

    # The entire pool supervisor should stop and send an EXIT signal to us.
    assert_receive {:EXIT, ^pid, :shutdown}, 2000
  end

  @tag :capture_log
  test "pool shuts down when workers fail to start (immediate failure)" do
    Process.flag(:trap_exit, true)

    result =
      ImmediateFailurePool.start_link(
        initial_workers: 1,
        max_restarts: 1,
        max_period: 5,
        start_timeout: 1000
      )

    # This should fail instantly with :supervisor_died.
    assert {:error, :supervisor_died} = result
  end

  def handle_telemetry(name, measurements, metadata, config_or_pid) do
    case config_or_pid do
      %{test_pid: pid} -> send(pid, {:telemetry_event, name, measurements, metadata})
      pid when is_pid(pid) -> send(pid, {:telemetry_event, measurements, metadata})
    end
  end
end

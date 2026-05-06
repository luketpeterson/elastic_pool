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
    worker_handler: ElasticPoolTest.TestWorker,
    initial_workers: 1,
    stats_interval: :never
end

defmodule TerminationPool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.TerminationWorker,
    initial_workers: 5,
    stats_interval: :never
end

defmodule SlowStartupPool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.SlowInitWorker,
    initial_workers: 10
end

defmodule PollerPool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.TestWorker,
    initial_workers: 2,
    stats_interval: 100
end

defmodule IntensityPool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.FastCrashingWorker,
    initial_workers: 1,
    max_restarts: 2,
    max_period: 5,
    stats_interval: :never
end

defmodule WorkIntensityPool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.WorkCrashingWorker,
    initial_workers: 1,
    max_restarts: 1,
    max_period: 5,
    stats_interval: :never
end

defmodule ElasticPoolTest.MissingCallbacksWorker do
  # No handle_work callback
end

defmodule ImmediateFailurePool do
  use ElasticPool,
    worker_handler: ElasticPoolTest.MissingCallbacksWorker,
    initial_workers: 1,
    max_restarts: 1,
    max_period: 5,
    start_timeout: 1000
end

# --------------------------

defmodule ElasticPoolTest do
  use ExUnit.Case

  test "all workers are terminated when pool stops" do
    test_pid = self()

    {:ok, pid} = TerminationPool.start_link(worker_args: [test_pid: test_pid])

    # Shutdown the pool
    Supervisor.stop(pid)

    # Check that we received exactly 5 termination messages
    for _ <- 1..5 do
      assert_receive :worker_terminated, 500
    end
  end

  test "initial workers init in parallel" do
    start_time = System.monotonic_time(:millisecond)

    {:ok, pid} = SlowStartupPool.start_link()

    end_time = System.monotonic_time(:millisecond)
    peak_workers = SlowStartupPool.peak_workers()
    duration = end_time - start_time

    Supervisor.stop(pid)

    assert peak_workers == 10
    assert duration < 500, "Startup took too long: #{duration}ms"
  end

  test "can perform work via generic call" do
    {:ok, pid} = BasicPool.start_link()

    assert BasicPool.call(:ping) == :pong
    assert BasicPool.request_count() == 1
    Supervisor.stop(pid)
  end

  test "status shows workers" do
    {:ok, pid} = BasicPool.start_link()

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

    {:ok, pool_pid} = PollerPool.start_link()

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
    result = IntensityPool.start_link()

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

    {:ok, pid} = WorkIntensityPool.start_link()

    # We need to crash it 2 times to hit max_restarts: 1
    # 1. First crash - Call synchronously
    catch_exit(WorkIntensityPool.call(:crash))

    # Wait for the RECOVERY telemetry (Sent by the worker itself!)
    assert_receive {:telemetry_event, [:elastic_pool, :worker, :start], _, %{start_reason: :recovery}}, 1000

    # 2. Second crash - This should trigger the intensity limit
    catch_exit(WorkIntensityPool.call(:crash))

    # The entire pool supervisor should stop and send an EXIT signal to us.
    assert_receive {:EXIT, ^pid, :shutdown}, 2000
  end

  @tag :capture_log
  test "pool shuts down when workers fail to start (immediate failure)" do
    Process.flag(:trap_exit, true)

    result = ImmediateFailurePool.start_link()

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

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

defmodule ElasticPoolTest do
  use ExUnit.Case

  test "all workers are terminated when pool stops" do
    name = :termination_test
    test_pid = self()
    worker_count = 5

    {:ok, _pid} =
      ElasticPool.start_link(
        name: name,
        worker_handler: ElasticPoolTest.TerminationWorker,
        initial_workers: worker_count,
        worker_args: [test_pid: test_pid]
      )

    # Shutdown the pool
    Supervisor.stop(name)

    # Check that we received exactly 5 termination messages
    for _ <- 1..worker_count do
      assert_receive :worker_terminated, 500
    end
  end

  test "initial workers init in parallel" do
    name = :slow_startup_test
    start_time = System.monotonic_time(:millisecond)

    {:ok, _pid} =
      ElasticPool.start_link(
        name: name,
        worker_handler: ElasticPoolTest.SlowInitWorker,
        initial_workers: 10
      )

    end_time = System.monotonic_time(:millisecond)
    peak_workers = ElasticPool.peak_workers(name)
    duration = end_time - start_time

    Supervisor.stop(name)

    assert peak_workers == 10
    assert duration < 500, "Startup took too long: #{duration}ms"
  end

  test "can perform work via generic call" do
    name = String.to_atom("TestPool_#{:erlang.unique_integer([:positive])}")

    {:ok, _pid} =
      ElasticPool.start_link(
        name: name,
        worker_handler: ElasticPoolTest.TestWorker,
        initial_workers: 1
      )

    assert ElasticPool.call(name, :ping) == :pong
    assert ElasticPool.request_count(name) == 1
    Supervisor.stop(name)
  end

  test "status shows workers" do
    name = String.to_atom("StatusPool_#{:erlang.unique_integer([:positive])}")

    {:ok, _pid} =
      ElasticPool.start_link(
        name: name,
        worker_handler: ElasticPoolTest.TestWorker,
        initial_workers: 1
      )

    assert ElasticPool.target_workers(name) == 1
    Supervisor.stop(name)
  end

  test "StatsPoller emits periodic telemetry" do
    name = :poller_test
    test_pid = self()

    # Attach a temporary telemetry handler using a named function to avoid warnings
    handler_id = "test-poller-handler"

    :telemetry.attach(
      handler_id,
      [:elastic_pool, :pool, :status],
      &__MODULE__.handle_telemetry/4,
      test_pid
    )

    {:ok, _pool_pid} =
      ElasticPool.start_link(
        name: name,
        worker_handler: ElasticPoolTest.TestWorker,
        initial_workers: 2
      )

    # Start the poller with a very short interval for the test
    {:ok, _poller_pid} = ElasticPool.StatsPoller.start_link(pool: name, interval: 100)

    # We should receive a heartbeat
    assert_receive {:telemetry_event, measurements, %{pool_name: ^name}}, 500
    assert measurements.target_workers == 2

    # Cleanup
    :telemetry.detach(handler_id)
    Supervisor.stop(name)
  end

  defmodule FastCrashingWorker do
    use ElasticPool.Worker
    @impl true
    def init(_), do: raise("Instant Crash")

    @impl true
    def handle_work(_req, _from, state), do: {:reply, :ok, state}
  end

  @tag :capture_log
  test "pool shuts down when crash intensity is reached during worker init" do
    name = :intensity_test
    Process.flag(:trap_exit, true)

    # This should crash immediately on start because initial_workers=1
    # but it will keep trying to reconcile until max_restarts (2) is hit.
    result =
      ElasticPool.start_link(
        name: name,
        worker_handler: FastCrashingWorker,
        initial_workers: 1,
        max_restarts: 2,
        max_period: 5
      )

    # During init failure, start_link returns the error reason
    assert {:error, :supervisor_died} = result
    assert_receive {:EXIT, _pid, :shutdown}
    assert Process.whereis(name) == nil
  end

  defmodule WorkCrashingWorker do
    use ElasticPool.Worker
    @impl true
    def handle_work(:crash, _from, _state), do: raise("Work Crash")
    @impl true
    def handle_work(:ping, _from, state), do: {:reply, :pong, state}
  end

  @tag :capture_log
  test "pool shuts down when crash intensity is reached during work" do
    name = :work_intensity_test
    Process.flag(:trap_exit, true)

    {:ok, pid} =
      ElasticPool.start_link(
        name: name,
        worker_handler: WorkCrashingWorker,
        initial_workers: 1,
        max_restarts: 1,
        max_period: 5
      )

    # We need to crash it 2 times to hit max_restarts: 1
    # 1. First crash
    spawn(fn -> ElasticPool.call(name, :crash) end)
    # Wait for the manager to see the crash and start a new one
    Process.sleep(100)

    # 2. Second crash - This should trigger the intensity limit
    spawn(fn -> ElasticPool.call(name, :crash) end)

    # The entire pool supervisor should stop and send an EXIT signal to us.
    # Supervisors that stop due to restart intensity exit with :shutdown.
    assert_receive {:EXIT, ^pid, :shutdown}, 2000
    assert Process.whereis(name) == nil
  end

  def handle_telemetry(_name, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry_event, measurements, metadata})
  end
end

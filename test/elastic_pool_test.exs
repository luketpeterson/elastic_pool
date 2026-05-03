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

    {:ok, _pid} = ElasticPool.start_link(
      name: name,
      worker_handler: ElasticPoolTest.TerminationWorker,
      baseline_workers: worker_count,
      worker_args: [test_pid: test_pid]
    )

    # Shutdown the pool
    Supervisor.stop(name)

    # Check that we received exactly 5 termination messages
    for _ <- 1..worker_count do
      assert_receive :worker_terminated, 500
    end
  end

  test "baseline workers init in parallel" do
    name = :slow_startup_test
    start_time = System.monotonic_time(:millisecond)

    {:ok, _pid} = ElasticPool.start_link(
      name: name,
      worker_handler: ElasticPoolTest.SlowInitWorker,
      baseline_workers: 10
    )

    end_time = System.monotonic_time(:millisecond)
    total_workers = ElasticPool.total_workers(name)
    peak_workers = ElasticPool.peak_workers(name)
    duration = end_time - start_time

    Supervisor.stop(name)

    assert peak_workers == 10
    assert duration < 500, "Startup took too long: #{duration}ms"
  end

  test "can perform work via generic call" do
    name = String.to_atom("TestPool_#{:erlang.unique_integer([:positive])}")
    {:ok, _pid} = ElasticPool.start_link(
      name: name,
      worker_handler: ElasticPoolTest.TestWorker,
      baseline_workers: 1
    )

    assert ElasticPool.call(name, :ping) == :pong
    Supervisor.stop(name)
  end

  test "status shows workers" do
    name = String.to_atom("StatusPool_#{:erlang.unique_integer([:positive])}")
    {:ok, _pid} = ElasticPool.start_link(
      name: name,
      worker_handler: ElasticPoolTest.TestWorker,
      baseline_workers: 1
    )

    assert ElasticPool.total_workers(name) == 1
    Supervisor.stop(name)
  end

  test "StatsPoller emits periodic telemetry" do
    name = :poller_test
    test_pid = self()
    
    # Attach a temporary telemetry handler
    handler_id = "test-poller-handler"
    :telemetry.attach(handler_id, [:elastic_pool, :pool, :status], fn _name, measurements, metadata, _config ->
      send(test_pid, {:telemetry_event, measurements, metadata})
    end, nil)

    {:ok, _pool_pid} = ElasticPool.start_link(
      name: name,
      worker_handler: ElasticPoolTest.TestWorker,
      baseline_workers: 2
    )

    # Start the poller with a very short interval for the test
    {:ok, _poller_pid} = ElasticPool.StatsPoller.start_link(pool: name, interval: 100)

    # We should receive a heartbeat
    assert_receive {:telemetry_event, measurements, %{pool_name: ^name}}, 500
    assert measurements.total_workers == 2

    # Cleanup
    :telemetry.detach(handler_id)
    Supervisor.stop(name)
  end
end

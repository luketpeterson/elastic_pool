defmodule ElasticPoolTest.TestWorker do
  def handle_work(:ping, _from, state), do: {:reply, :pong, state}
end

defmodule ElasticPoolTest.SlowInitWorker do
  def init(args) do
    Process.sleep(250)
    args
  end
  def handle_work(_, _, state), do: {:reply, :ok, state}
end

defmodule ElasticPoolTest do
  use ExUnit.Case

  test "baseline workers init in parallel" do
    name = :slow_startup_test
    start_time = System.monotonic_time(:millisecond)

    # This should now block until 10 workers are ready
    {:ok, _pid} = ElasticPool.start_link(
      name: name,
      worker_handler: ElasticPoolTest.SlowInitWorker,
      baseline_workers: 10
    )

    end_time = System.monotonic_time(:millisecond)
    status = ElasticPool.status(name)
    duration = end_time - start_time

    # Cleanup
    Supervisor.stop(name)

    assert status.peak_workers == 10
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

    status = ElasticPool.status(name)
    assert status.total_workers == 1
    Supervisor.stop(name)
  end
end

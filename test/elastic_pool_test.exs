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

  test "baseline workers all start in parallel" do
    name = :slow_startup_test
    start_time = System.monotonic_time(:millisecond)

    {:ok, _pid} = ElasticPool.start_link(
      name: name,
      worker_handler: ElasticPoolTest.SlowInitWorker,
      baseline_workers: 10
    )

    # Wait for all 10 workers to report ready
    wait_for_workers(name, 10)
    end_time = System.monotonic_time(:millisecond)

    status = ElasticPool.status(name)
    duration = end_time - start_time

    # Cleanup
    Supervisor.stop(name)

    assert status.peak_workers == 10
    assert duration < 500, "Startup took too long: #{duration}ms"
  end

  defp wait_for_workers(name, count, attempts \\ 20)
  defp wait_for_workers(_name, count, 0), do: flunk("Workers never reached #{count}")
  defp wait_for_workers(name, count, attempts) do
    if ElasticPool.status(name).total_workers == count do
      :ok
    else
      Process.sleep(50)
      wait_for_workers(name, count, attempts - 1)
    end
  end

  test "can perform work via generic call" do
    name = String.to_atom("TestPool_#{:erlang.unique_integer([:positive])}")
    {:ok, _pid} = ElasticPool.start_link(
      name: name,
      worker_handler: ElasticPoolTest.TestWorker,
      baseline_workers: 1
    )

    # Wait for baseline worker to boot
    wait_for_worker(name)

    assert ElasticPool.call(name, :ping) == :pong
  end

  test "status shows workers" do
    name = String.to_atom("StatusPool_#{:erlang.unique_integer([:positive])}")
    {:ok, _pid} = ElasticPool.start_link(
      name: name,
      worker_handler: ElasticPoolTest.TestWorker,
      baseline_workers: 1
    )

    wait_for_worker(name)
    status = ElasticPool.status(name)
    assert status.total_workers == 1
  end

  defp wait_for_worker(name, attempts \\ 10)
  defp wait_for_worker(_name, 0), do: :timeout
  defp wait_for_worker(name, attempts) do
    if ElasticPool.status(name).total_workers > 0 do
      :ok
    else
      Process.sleep(50)
      wait_for_worker(name, attempts - 1)
    end
  end
end

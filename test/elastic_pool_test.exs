defmodule ElasticPoolTest.TestWorker do
  def handle_work(:ping, _from, state), do: {:reply, :pong, state}
end

defmodule ElasticPoolTest do
  use ExUnit.Case

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

defmodule ElasticPoolTest do
  use ExUnit.Case
  doctest ElasticPool

  test "can perform work" do
    assert ElasticPool.work(10) == :ok
  end

  test "status shows workers" do
    status = ElasticPool.status()
    assert status.total_workers >= 2
    assert status.available_workers >= 0
  end
end

defmodule PrologBridgeTest do
  use ExUnit.Case
  doctest PrologBridge

  test "greets the world" do
    assert PrologBridge.hello() == :world
  end
end

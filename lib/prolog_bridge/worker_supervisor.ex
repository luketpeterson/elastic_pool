defmodule PrologBridge.WorkerSupervisor do
  @moduledoc """
  Manages the lifecycle of Prolog workers.
  """
  use DynamicSupervisor

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def start_worker do
    kb_file = Path.expand(Application.get_env(:prolog_bridge, :kb_file, "kb.pl"))
    # We do NOT increment here anymore; Queue handles it atomically
    DynamicSupervisor.start_child(__MODULE__, {PrologBridge.Worker, [kb_file: kb_file]})
  end

  @impl true
  def init(:ok) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end

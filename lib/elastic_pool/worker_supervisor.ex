defmodule ElasticPool.WorkerSupervisor do
  @moduledoc """
  Manages the lifecycle of Elastic workers.
  """
  use DynamicSupervisor

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def start_worker do
    DynamicSupervisor.start_child(__MODULE__, {ElasticPool.Worker, []})
  end

  @impl true
  def init(:ok) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end

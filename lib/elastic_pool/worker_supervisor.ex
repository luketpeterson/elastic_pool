defmodule ElasticPool.WorkerSupervisor do
  @moduledoc """
  Manages the lifecycle of Elastic workers for a specific pool.
  """
  use DynamicSupervisor

  def start_link(config) do
    DynamicSupervisor.start_link(__MODULE__, config, name: config.supervisor)
  end

  def start_worker(supervisor, config) do
    worker_args = [
      handler: config.worker_handler,
      pool: config.pool,
      manager: config.manager
    ] ++ config.worker_args

    DynamicSupervisor.start_child(supervisor, {ElasticPool.Worker, worker_args})
  end

  @impl true
  def init(_config) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end

defmodule ElasticPool.Worker do
  @moduledoc """
  A generic worker shell that delegates work to a caller-provided module.

  To implement a worker, use this module:

      defmodule MyWorker do
        use ElasticPool.Worker

        @impl true
        def handle_work(request, _from, state) do
          {:reply, :ok, state}
        end
      end
  """

  @callback init(args :: term()) :: state :: term()
  @callback handle_work(request :: term(), from :: term(), state :: term()) ::
              {:reply, reply :: term(), new_state :: term()}
              | {:noreply, new_state :: term()}
  @callback terminate(reason :: term(), state :: term()) :: term()

  @optional_callbacks init: 1, terminate: 2

  defmacro __using__(_opts) do
    quote do
      @behaviour ElasticPool.Worker

      @impl true
      def init(args), do: args

      @impl true
      def terminate(_reason, _state), do: :ok

      defoverridable init: 1, terminate: 2
    end
  end

  use GenServer, restart: :temporary
  require Logger

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(args) do
    # Trap exits so terminate/2 is called even when the supervisor shuts us down
    Process.flag(:trap_exit, true)
    # Return immediately so the supervisor can start more workers in parallel
    {:ok, args, {:continue, :post_init}}
  end

  @impl true
  def handle_continue(:post_init, args) do
    handler = Keyword.fetch!(args, :handler)
    pool = Keyword.fetch!(args, :pool)
    manager = Keyword.fetch!(args, :manager)
    start_reason = Keyword.get(args, :start_reason, :unknown)

    handler_state = if function_exported?(handler, :init, 1), do: handler.init(args), else: args

    # Notify that this worker is ready to take work
    ElasticPool.WorkerManager.worker_ready(manager)
    ElasticPool.Pool.worker_ready(pool, self())

    :telemetry.execute(
      [:elastic_pool, :worker, :start],
      %{count: 1},
      %{
        pool: pool,
        handler: handler,
        start_reason: start_reason
      }
    )

    {:noreply,
     %{
       handler: handler,
       handler_state: handler_state,
       pool: pool,
       start_reason: start_reason
     }}
  end

  @impl true
  def handle_call(request, from, state) do
    case state.handler.handle_work(request, from, state.handler_state) do
      {:reply, reply, new_handler_state} ->
        {:reply, reply, %{state | handler_state: new_handler_state}}

      {:noreply, new_handler_state} ->
        {:noreply, %{state | handler_state: new_handler_state}}
    end
  end

  @impl true
  def handle_info({:EXIT, _from, :normal}, state) do
    {:stop, :normal, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.error("[Worker] Received unexpected message: #{inspect(msg)}")

    :telemetry.execute(
      [:elastic_pool, :worker, :unknown_message],
      %{count: 1},
      %{pool: state.handler, message: msg}
    )

    {:noreply, state}
  end

  @impl true
  def terminate(reason, state) do
    stop_reason =
      case reason do
        :normal -> :scale_down
        :shutdown -> :shutdown
        {:shutdown, _} -> :shutdown
        _ -> :crash
      end

    if is_map(state) and Map.has_key?(state, :pool) do
      :telemetry.execute(
        [:elastic_pool, :worker, :stop],
        %{count: 1},
        %{
          pool: state.pool,
          handler: state.handler,
          stop_reason: stop_reason,
          exit_reason: reason
        }
      )
    end

    if is_map(state) and Map.has_key?(state, :handler) and
         function_exported?(state.handler, :terminate, 2) do
      state.handler.terminate(reason, state.handler_state)
    else
      :ok
    end
  end
end

defmodule ElasticPool.Worker do
  @moduledoc """
  Public behaviour and `use` macro for ElasticPool workers.

  To implement a worker, use this module:

      defmodule MyWorker do
        use ElasticPool.Worker

        @impl true
        def handle_work(request, _from, state) do
          {:reply, :ok, state}
        end
      end

  ## Asynchronous Messages

  Workers can handle asynchronous messages (e.g. from Ports or `send/2`)
  by implementing the `handle_info/2` callback.

      @impl true
      def handle_info({:data, data}, state) do
        # handle data
        {:noreply, state}
      end
  """

  @type worker_state :: term()
  @type worker_reply ::
          {:reply, reply :: term(), new_state :: worker_state()}
          | {:noreply, new_state :: worker_state()}

  @callback init(args :: term()) :: worker_state() | {:stop, reason :: term()}

  @doc """
  Called when a synchronous request is made to the worker via `ElasticPool.call/2`.
  """
  @callback handle_work(request :: term(), from :: GenServer.from(), state :: worker_state()) ::
              worker_reply()

  @doc """
  Called when the worker receives an asynchronous message.
  """
  @callback handle_info(msg :: term(), state :: worker_state()) ::
              {:noreply, new_state :: worker_state()} | {:stop, reason :: term(), new_state :: worker_state()}

  @callback terminate(reason :: term(), state :: worker_state()) :: term()

  @optional_callbacks init: 1, terminate: 2, handle_info: 2

  defmacro __using__(_opts) do
    quote do
      @behaviour ElasticPool.Worker

      @impl true
      def init(args), do: args

      @impl true
      def handle_info(_msg, state), do: {:noreply, state}

      @impl true
      def terminate(_reason, _state), do: :ok

      defoverridable init: 1, terminate: 2, handle_info: 2
    end
  end
end

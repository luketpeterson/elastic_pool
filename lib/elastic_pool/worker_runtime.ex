defmodule ElasticPool.Worker.Runtime do
  @moduledoc false

  @doc false
  defmacro __monomorphize__(handler) do
    quote bind_quoted: [handler: handler] do
      use GenServer, restart: :temporary
      require Logger

      @handler handler

      def start_link(args), do: GenServer.start_link(__MODULE__, args)

      @impl true
      def init(args) do
        # Fail fast if the handler is not a valid module or doesn't implement the worker behavior
        if !Code.ensure_loaded?(@handler) or !function_exported?(@handler, :handle_work, 3) do
          {:stop, {:invalid_worker_handler, @handler}}
        else
          # Trap exits so terminate/2 is called even when the supervisor shuts us down
          Process.flag(:trap_exit, true)
          # Return immediately so the supervisor can start more workers in parallel
          {:ok, args, {:continue, :post_init}}
        end
      end

      @impl true
      def handle_continue(:post_init, args) do
        pool = Keyword.fetch!(args, :pool)
        manager = Keyword.fetch!(args, :manager)
        start_reason = Keyword.get(args, :start_reason, :unknown)

        case init_handler(@handler, args) do
          {:stop, reason} ->
            {:stop, reason, args}

          handler_state ->
            # Notify that this worker is ready to take work
            ElasticPool.WorkerManager.worker_ready(manager, self())

            :telemetry.execute(
              [:elastic_pool, :worker, :start],
              %{count: 1},
              %{
                pool: pool,
                handler: @handler,
                start_reason: start_reason
              }
            )

            {:noreply,
             %{
               handler_state: handler_state,
               pool: pool,
               start_reason: start_reason
             }}
        end
      end

      defp init_handler(handler, args) do
        if function_exported?(handler, :init, 1) do
          handler.init(args)
        else
          args
        end
      end

      @doc false
      def identity(x), do: x

      @impl true
      def handle_call(request, from, state) do
        # MONOMORPHIZED: This is a direct call to the handler module.
        # We pass the result through identity/1 to silence 'never match' warnings
        # caused by the compiler's reachability analysis of specialized workers.
        case identity(@handler.handle_work(request, from, state.handler_state)) do
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
          %{pool: @handler, message: msg}
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
              handler: @handler,
              stop_reason: stop_reason,
              exit_reason: reason
            }
          )
        end

        if is_map(state) and Map.has_key?(state, :handler_state) and
             function_exported?(@handler, :terminate, 2) do
          @handler.terminate(reason, state.handler_state)
        else
          :ok
        end
      end
    end
  end
end

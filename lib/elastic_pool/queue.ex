defmodule ElasticPool.Queue do
  @moduledoc """
  A lock-free concurrent LIFO stack using an atomic "Ticket Dispenser" for coordination.
  """

  @type t :: %{
          table: :ets.tid() | atom(),
          atomics: :erlang.atomics_ref(),
          counter_idx: pos_integer()
        }

  # Number of times to spin-wait for a specific ticket's data to appear in ETS
  @ticket_spin 100

  @doc """
  Initializes the queue state.
  """
  def new(table, atomics, counter_idx) do
    %{
      table: table,
      atomics: atomics,
      counter_idx: counter_idx
    }
  end

  @doc """
  Pushes an item into the queue.
  """
  def push(queue, item) do
    # Get a unique "parking spot"
    index = :atomics.add_get(queue.atomics, queue.counter_idx, 1)
    :ets.insert(queue.table, {index, item})
  end

  @doc """
  Pops an item from the queue. Returns `{:ok, item}` or `:error`.
  """
  def pop(queue, limit \\ 100)
  def pop(_queue, 0), do: :error

  def pop(queue, limit) do
    # Claim a "ticket" to check the current top slot
    index = :atomics.add_get(queue.atomics, queue.counter_idx, -1)

    if index >= 0 do
      # We claimed slot index+1 (since it was the value before decrement)
      target = index + 1
      
      case spin_take(queue.table, target, @ticket_spin) do
        {:ok, item} ->
          {:ok, item}

        :error ->
          # This ticket's slot is empty even after spinning.
          # This could be a race where the pusher died or is very slow.
          # We've already consumed the ticket, so we must try the next one down.
          pop(queue, limit - 1)
      end
    else
      # Stack appears empty. Undo the claim.
      :atomics.add(queue.atomics, queue.counter_idx, 1)
      :error
    end
  end

  defp spin_take(_table, _target, 0), do: :error
  defp spin_take(table, target, limit) do
    case :ets.take(table, target) do
      [{^target, item}] -> {:ok, item}
      [] -> spin_take(table, target, limit - 1)
    end
  end
end

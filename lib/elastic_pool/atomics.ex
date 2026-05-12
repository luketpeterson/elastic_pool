defmodule ElasticPool.Atomics do
  @moduledoc false

  # The ground truth of the pool state.
  # Positive: Idle workers available. Negative: Clients waiting.
  defmacro score_idx, do: 1

  # Current physical count of workers linked to the manager.
  defmacro active_idx, do: 2

  # Historical maximum of concurrent workers.
  defmacro peak_idx, do: 3

  # Total number of checkout requests since start.
  defmacro request_idx, do: 4

  # Total number of checkins (completed jobs) since start.
  defmacro completion_idx, do: 5

  # The current capacity goal of the scaling policy.
  defmacro target_idx, do: 6

  # Total number of slots to allocate in :atomics.new/2
  defmacro count, do: 6
end

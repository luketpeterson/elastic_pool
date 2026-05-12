defmodule ElasticPool.Atomics do
  @moduledoc false

  # Shard Scores: 1..16
  def score_idx(shard), do: shard
  
  # Shard Push Indices: 17..32
  def push_idx(shard), do: 16 + shard
  
  # Shard Pop Indices: 33..48
  def pop_idx(shard), do: 32 + shard

  defmacro active_idx, do: 49
  defmacro peak_idx, do: 50
  defmacro request_idx, do: 51
  defmacro completion_idx, do: 52
  defmacro target_idx, do: 53

  defmacro count, do: 54
  defmacro num_shards, do: 16
end

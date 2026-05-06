defmodule ElasticPool.Policies.Threshold do
  @moduledoc """
  A reactive scaling policy that increases the pool size by 1 worker at a time
  until there are fewer than `scale_up_threshold` requests waiting to be serviced,
  and decreases the pool size by 1 worker at a time until there are
  `available_reserve` free workers or fewer.

  This is a simple policy, but it shouldn't be used unless workers are very fast
  to start and stop.  The reactive nature means it's virtually guaranteed to
  under-provision workers until latency starts getting bad, and then it will
  over-shoot and bring up more workers than needed because of the backlog.
  """
  @behaviour ElasticPool.ScalingPolicy
  require ElasticPool

  @impl true
  def init(%{policy_opts: opts, pool_config: _pool}) do
    %{
      available_reserve: opts[:available_reserve] || 4,
      scale_up_threshold: opts[:scale_up_threshold] || 10
    }
  end

  @impl true
  def handle_event(event, pool, state) do
    old_target = ElasticPool.target_workers(pool)
    active = ElasticPool.active_workers(pool)

    target =
      if active == old_target do
        # We're in a steady state, so we might want to adjust the target
        case event do
          :checkout_failed ->
            waiting = ElasticPool.waiting_clients(pool)

            if waiting > state.scale_up_threshold do
              old_target + 1
            else
              old_target
            end

          :checkin ->
            available = ElasticPool.available_workers(pool)

            if available > state.available_reserve do
              max(old_target - 1, 0)
            else
              old_target
            end

          _ ->
            old_target
        end
      else
        # A worker is starting or stopping, so let's just let it be until that finshes
        old_target
      end

    {target, state}
  end
end

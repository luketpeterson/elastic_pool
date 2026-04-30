import Config

config :elastic_pool,
  pool_max_workers: 8,
  pool_baseline_workers: 2,
  pool_cooldown_ms: 500,
  pool_scale_threshold: 10,
  worker_handler: ElasticPool.DummyWorker,
  worker_args: []

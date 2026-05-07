defmodule ElasticPool.MixProject do
  use Mix.Project

  def project do
    [
      app: :elastic_pool,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      # {:poolboy, "~> 1.5", runtime: false},  # Uncomment to run the benchmark
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      groups_for_modules: [
        "Core API": [
          ElasticPool,
          ElasticPool.Worker,
          ElasticPool.ScalingPolicy
        ],
        "Built-in Policies": [
          ElasticPool.Policies.Null,
          ElasticPool.Policies.Threshold,
          ElasticPool.Policies.ErlangC
        ]
      ]
    ]
  end
end

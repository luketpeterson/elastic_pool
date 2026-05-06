defmodule ElasticPool.MixProject do
  use Mix.Project

  def project do
    [
      app: :elastic_pool,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:telemetry, "~> 1.0"},
      {:poolboy, "~> 1.5", only: :bench},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end
end

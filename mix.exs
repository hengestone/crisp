defmodule Crisp.MixProject do
  use Mix.Project

  def project do
    [
      app: :crisp,
      deps: deps(),
      elixir: "~> 1.10",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      version: "0.5.0",

      # Docs
      name: "Crisp",
      source_url: "https://gitlab.com/conrad-steenberg/crisp",
      docs: [
        # The main page in the docs
        main: "readme",
        extras: ["README.md"]
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support", "test/demo"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :redix, :cowboy],
      mod: {Crisp, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:cowboy, "~>2.9"},
      {:csv, "~> 2.4"},
      {:ex_doc, "~> 0.28", only: :dev, runtime: false},
      {:glob, "~> 1.0"},
      {:hashids, "~> 2.0"},
      {:jason, "~>1.3"},
      {:logger_file_backend, "~> 0.0.13"},
      {:mimerl, "~> 1.2"},
      {:mox, "~> 0.5", only: :test},
      {:redix, "~> 1.1"},
      {:rexbug, "~> 1.0", only: [:test, :dev]},
      {:xxhash, "~> 0.3"}
    ]
  end
end

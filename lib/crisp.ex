defmodule Crisp do
  @moduledoc false

  use Application

  @doc false
  @spec start(any, any) :: {:error, any} | {:ok, pid}
  def start(_type, _args) do
    children = [
      {Redix, Application.get_env(:crisp, :redis)},
      Crisp.WebHandler
    ]

    opts = [strategy: :one_for_one, name: Crisp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

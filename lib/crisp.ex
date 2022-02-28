defmodule Crisp do
  @moduledoc false

  use Application
  import Supervisor.Spec

  @doc false
  @spec start(any, any) :: {:error, any} | {:ok, pid}
  def start(_type, _args) do
    children = [
      supervisor(
        Redix,
        [Application.get_env(:crisp, :redis)]
      )
      # %{
      #   id: Crisp.WebHandler,
      #   start: {Crisp.WebHandler, :start, []}
      # }
    ]

    opts = [strategy: :one_for_one, name: Crisp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

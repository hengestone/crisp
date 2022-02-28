defmodule Crisp.Queue do
  @moduledoc """
  This module represents a queue structure
   - SerDe
  """

  import Crisp.Utilities

  @fields [:struct, :version, :name, :runlimit]
  defstruct @fields
  @types %{runlimit: :int}

  use Crisp.QueueObject

  @infinite 1000_000

  @spec new(binary, keyword) :: map()
  def new(name, args \\ []) when is_binary(name) and is_list(args) do
    %Crisp.Queue{
      struct: module_name(__MODULE__),
      version: version(),
      name: name,
      runlimit: Keyword.get(args, :runlimit, @infinite)
    }
  end
end

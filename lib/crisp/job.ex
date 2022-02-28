defmodule Crisp.Job do
  @moduledoc """
  This module represents a job structure
   - SerDe
  """
  import Crisp.Utilities

  @fields [:struct, :version, :id, :name, :node, :params]
  @types %{}

  defstruct @fields

  use Crisp.QueueObject

  @spec new(binary, keyword) :: map()
  def new(name, args \\ []) when is_binary(name) and is_list(args) do
    %Crisp.Job{
      struct: module_name(__MODULE__),
      version: version(),
      name: name,
      node: Keyword.get(args, :node, "no_node"),
      params: Keyword.get(args, :params, "no_params"),
      id: utc_id_short()
    }
  end
end

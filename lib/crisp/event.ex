defmodule Crisp.Event do
  @moduledoc """
  This module represents an event structure

  Fields: `[:name, :version, :id, :action, :object, :info]`
  """

  @fields [:struct, :version, :id, :action, :object, :info]
  @types %{}
  defstruct @fields

  use Crisp.QueueObject
  import Crisp.Utilities

  @doc """
  Creates a new event signifying a status or action, with an associated
  object name.

  The convention used is to encode the associated object as CSV, using a colon
  as separator.

  Fields: `fields [:name, :version, :id, :action, :object, :info]`
  """
  @spec new(atom, atom, keyword) :: map()
  def new(action, object, args \\ [])
      when is_atom(action) and is_atom(object) and is_list(args) do
    %Crisp.Event{
      struct: module_name(__MODULE__),
      version: version(),
      id: Utilities.utc_id(),
      info: Keyword.get(args, :info, "no_info"),
      action: action,
      object: object
    }
  end

end

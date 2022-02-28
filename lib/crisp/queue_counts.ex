defmodule Crisp.QueueCounts do
  @moduledoc """
  This module represents job counts in different subqueues
  """
  import Crisp.Utilities

  @types %{
    queued: :int,
    runnable: :int,
    running: :int,
    finished: :int,
    failed: :int
  }

  @queues Map.keys(@types)

  @fields [
    :struct,
    :version
    | @queues
  ]

  defstruct @fields
  use Crisp.QueueObject

  @spec queues :: [
          :failed | :finished | :queued | :runnable | :running,
          ...
        ]

  def queues() do
    @queues
  end

  @spec new(list) :: map
  def new(counts) when is_list(counts) do
    {keyvals, []} =
      Enum.reduce(counts, {[], @queues}, fn count, {pairs, names} ->
        [head | rest] = names
        {[{to_atom(head), count} | pairs], rest}
      end)

    new(Map.new(List.flatten(keyvals)))
  end

  @spec new(map) :: map
  def new(counts) when is_map(counts) do
    %Crisp.QueueCounts{
      struct: module_name(__MODULE__),
      version: version()
    }
    |> Map.merge(counts)
  end
end

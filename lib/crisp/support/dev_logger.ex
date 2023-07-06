defmodule Crisp.DevLogger do
  @moduledoc """
  Log formatter for development
  """

  @doc """
  Formatting callback for Logger module
  """
  @spec format(atom, binary, any, map) :: binary
  def format(level, message, _ts, metadata) do
    utc = DateTime.to_iso8601(DateTime.utc_now())

    meta =
      %{module: "nomod", line: "--", function: "nofun"}
      |> Map.merge(Map.new(metadata))

    slevel = "#{level}" |> String.slice(0, 5)

    "[#{slevel}]\t#{utc} #{meta.module}\t#{meta.function}:#{meta.line}\t#{inspect(meta.pid)}\n--\t#{message}\n"
  rescue
    error ->
      " *** could not format:\n#{inspect({level, message, metadata})}\n#{inspect(error)}"
  end
end

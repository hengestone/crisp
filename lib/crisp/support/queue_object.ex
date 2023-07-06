defmodule Crisp.QueueObject do
  @moduledoc false
  defmacro __using__(_arg) do
    quote do
      alias Crisp.Utilities

      @doc """
      Creates a CSV encoded line representing the structure.
      """
      @spec to_csv(map, non_neg_integer) :: binary
      @spec to_csv(map) :: binary
      def to_csv(map, sep \\ ?,) do
        for field <- @fields do
          CSV.Encode.encode(Map.get(map, field, ""))
        end
        |> Enum.join(to_string([sep]))
      end

      @doc """
      Creates a structure based on the given line of CSV-encoded text
      """
      @spec from_csv(binary) :: {:ok, map} | {:error, any}
      @spec from_csv(binary, non_neg_integer) :: {:ok, map} | {:error, any}
      def from_csv(line, sep \\ ?,) when is_binary(line) do
        case [line]
             |> Stream.map(& &1)
             |> CSV.decode(headers: @fields, separator: sep)
             |> Enum.take(1) do
          [ok: obj_map] ->
            new_obj =
              Map.merge(%__MODULE__{}, Utilities.decode(obj_map, @types))

            if new_obj.version == version() do
              {:ok, new_obj}
            else
              {:error,
               "version mismatch: got #{new_obj.version}, current is #{version()}"}
            end

          [error: error] ->
            {:error, error}
        end
      end

      @doc """
      Structure version
      """
      @spec version :: binary
      def version() do
        Utilities.short_id_strings([Enum.join(@fields, ":")])
      end

      @doc """
      A list of atom fields in the structure.
      """
      @spec fields :: [atom]
      def fields() do
        @fields
      end
    end
  end
end

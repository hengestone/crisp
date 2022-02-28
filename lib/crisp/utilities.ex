defmodule Crisp.Utilities do
  @salt :binary.list_to_bin(
          :io_lib.format('~.32B', [
            Keyword.get(:erlang.system_info(:os_monotonic_time_source), :time) +
              :crypto.rand_uniform(0, 65536 * 65536 - 1)
          ])
        )

  @spec mt_id() :: binary
  def mt_id() do
    short_id_numbers([
      :erlang.unique_integer([:positive, :monotonic]),
      :rand.uniform(65536 * 65536) - 1
    ])
  end

  @spec mt_id_short() :: binary
  def mt_id_short() do
    short_id_numbers([
      :erlang.unique_integer([:positive, :monotonic]),
      :rand.uniform(65536) - 1
    ])
  end

  @spec short_id_strings([binary, ...]) :: binary
  def short_id_strings(data) when is_list(data) do
    short_id_numbers(Enum.map(data, &XXHash.xxh32/1))
  end

  @spec short_id_numbers([non_neg_integer, ...]) :: binary
  def short_id_numbers(numbers) when is_list(numbers) do
    Hashids.new(salt: @salt) |> Hashids.encode(numbers)
  end

  @spec utc_id :: binary
  def utc_id() do
    utc_usec = :io_lib.format('~.32B', [time_usecs()])
    "#{utc_usec}-#{mt_id()}"
  end

  @spec utc_id_short :: binary
  def utc_id_short() do
    utc32 = :io_lib.format('~.32B', [time_secs()])
    "#{utc32}-#{mt_id_short()}"
  end

  @spec utc_now :: [char]
  def utc_now() do
    t = DateTime.utc_now()

    :io_lib.format('~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0BUTC', [
      t.year,
      t.month,
      t.day,
      t.hour,
      t.minute,
      t.second
    ])
  end

  @spec time_secs :: non_neg_integer
  def time_secs() do
    :erlang.system_time(:second)
  end

  @spec time_msecs :: non_neg_integer
  def time_msecs() do
    :erlang.system_time(:millisecond)
  end

  @spec time_usecs :: non_neg_integer
  def time_usecs() do
    :erlang.system_time(:microsecond)
  end

  @spec decode(map, map) :: map
  def decode(smap, types) do
    for {key, val} <- Map.to_list(smap) do
      {key,
       case Map.get(types, key) do
         nil -> val
         :int -> String.to_integer(val)
       end}
    end
    |> Map.new()
  end

  @spec to_atom(atom) :: atom
  def to_atom(val) when is_atom(val) do
    val
  end

  @spec to_atom(binary) :: atom
  def to_atom(val) when is_binary(val) do
    String.to_atom(val)
  end

  @spec module_name(atom) :: binary
  def module_name(full_name) when is_atom(full_name) do
    String.trim_leading(Atom.to_string(full_name), "Elixir.")
  end

  @doc """
  Convenience function to return the pubsub updates channel name for the
  specified queue.

  E.g. `pulse("mynamespace", "myqueue")` returns `mynamespace:myqueue:pulse`
  """
  @spec pulse(binary | atom, binary | atom) :: binary
  def pulse(namespace, queue_name) do
    Enum.join([namespace, queue_name, "pulse"], ":")
  end

  @doc """
  Convenience function to return the full name of the specified queue/subqueue
  as stored in Redis.

  E.g. `subqueue("myqueue", :runnable)` returns `mynamespace:myqueue:runnable`
  """
  @spec subqueue(binary | atom, binary | atom, binary | atom) :: binary
  def subqueue(namespace, queue_name, subqueue) do
    Enum.join([namespace, queue_name, subqueue], ":")
  end

  @spec timer_running(any) :: false | nil | true
  def timer_running(nil) do
    nil
  end

  def timer_running(timer_ref) do
    case Process.read_timer(timer_ref) do
      false -> false
      _ -> true
    end
  end
end

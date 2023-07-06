defmodule Crisp.WebsocketHandler do
  @behaviour :cowboy_websocket
  require Logger
  alias Crisp.WebHandler

  # We are using the regular http init callback to perform handshake.
  #     https://ninenines.eu/docs/en/cowboy/2.7/guide/ws_handlers/
  def init(req, state) do
    {:cowboy_websocket, req, state, %{max_frame_size: 65535}}
  end

  def websocket_init(state) do
    WebHandler.subscribe(:counts)
    Process.send_after(self(), :ping, 15_000)
    {:ok, state}
  end

  # Put any essential clean-up here.
  def terminate(_reason, _req, _state) do
    :ok
  end

  # websocket_handle deals with messages coming in over the websocket,
  # including text, binary, ping or pong messages. But you need not
  # handle ping/pong, cowboy takes care of that.
  def websocket_handle({:text, content}, state) do
    Logger.debug(state)
    # Use JSEX to decode the JSON message and extract the word entered
    # by the user into the variable 'message'.
    {:ok, %{"message" => message}} = Jason.decode(content)

    # Reverse the message and use Jason to re-encode a reply containing
    # the reversed message.
    rev = String.reverse(message)
    {:ok, reply} = Jason.encode(%{reply: rev})

    # All websocket callbacks share the same return values.
    # See http://ninenines.eu/docs/en/cowboy/2.0/manual/cowboy_websocket/
    {[{:text, reply}], state}
  end

  def websocket_handle(:pong, state) do
    {:ok, state}
  end

  # Fallback clause for websocket_handle.  If the previous one does not match
  # this one just ignores the frame and returns `{:ok, state}` without
  # taking any action. A proper app should  probably intelligently handle
  # unexpected messages.

  def websocket_handle(frame, state) do
    Logger.debug(inspect({frame, state}))
    {:ok, state}
  end

  # Topic message handler
  @spec websocket_info({atom, binary}, any) ::
          {:ok, any} | {:ok, [{:text, any}, ...], any}
  def websocket_info({_topic, message}, state) do
    {[{:text, message}], state}
  end

  def websocket_info(:ping, state) do
    Process.send_after(self(), :ping, 15_000)
    {[:ping], state}
  end

  # fallback message handler
  def websocket_info(info, state) do
    Logger.debug(inspect(info))
    {:ok, state}
  end

  def time_as_string do
    {hh, mm, ss} = :erlang.time()

    :io_lib.format("~2.10.0B:~2.10.0B:~2.10.0B", [hh, mm, ss])
    |> :erlang.list_to_binary()
  end
end

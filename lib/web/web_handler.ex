defmodule Crisp.WebHandler do
  @moduledoc """
  This module
   - starts a cowboy web server with a websocket handler
   - maintains a list of queues in the namespace
   - subscribes to changes in all queues
   - sends queue job counts to the client
   - sends messages to the client
  """
  require Logger
  use GenServer
  import Crisp.Utilities
  @count_delay 2000

  alias Crisp.{Client, Event, QueueCounts, Server, WebsocketHandler}
  # Genserver Client part ------------------------------------------------------
  @spec start :: :ignore | {:error, any} | {:ok, pid}
  def start() do
    start_link([])
  end

  @spec start_link(list) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(init_arg) do
    GenServer.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @spec init(list) ::
          {:ok, %{pubsub: pid, queues_updated: %{}}} | {:stop, :not_started}
  @impl true
  def init(_args) do
    Application.ensure_started(:cowboy)

    if is_nil(Process.whereis(Client)) do
      Client.start()
      Process.sleep(50)
    end

    if is_nil(Process.whereis(Server)) do
      Server.start()
      Process.sleep(50)
    end

    client_state = Client.state()
    Server.update_local()

    with {:ok, _listener_pid} <-
           :cowboy.start_clear(:http, [{:port, 3141}], %{
             env: %{dispatch: routes()}
           }),
         {:ok, pubsub} <- Redix.PubSub.start_link(),
         {:ok, _ref} <-
           Redix.PubSub.psubscribe(
             pubsub,
             "#{client_state.namespace}:*",
             self()
           ) do
      schedule_event(
        %{
          pubsub: pubsub,
          queues_updated: %{},
          subs: %{},
          count_timer: nil,
          count_delay: @count_delay
        },
        :queue_counts,
        :count_timer,
        @count_delay
      )
    else
      error ->
        Logger.error(inspect(error))
        {:stop, :not_started}
    end
  end

  @spec routes :: :cowboy_router.dispatch_rules()
  defp routes() do
    :cowboy_router.compile([
      {:_,
       [
         {"/", :cowboy_static, {:priv_file, :crisp, "web/index.html"}},
         {"/js/[...]", :cowboy_static, {:priv_dir, :crisp, "web/js"}},

         #  Serve websocket requests.
         {"/ws/[...]", WebsocketHandler, []}
       ]}
    ])
  end

  @spec subscribe(atom) :: {:ok, atom} | {:error, any}
  def subscribe(topic) do
    GenServer.call(__MODULE__, {:subscribe, topic})
  end

  @spec publish(atom, binary) :: :ok
  def publish(topic, message) do
    GenServer.cast(__MODULE__, {:publish, topic, message})
  end

  @spec state :: map
  def state() do
    GenServer.call(__MODULE__, :state)
  end

  # Call handlers --------------------------------------------------------------
  @impl true
  def handle_call({:subscribe, topic}, {from, _ref}, state) when is_pid(from) do
    topic_subs = Map.get(state.subs, topic, %{}) |> Map.put(from, true)
    Logger.debug("Pubsub client connected to '#{topic}' from #{inspect(from)}")
    Process.monitor(from)
    {:reply, {:ok, topic}, put_in(state, [:subs, topic], topic_subs)}
  end

  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  # Cast handlers --------------------------------------------------------------
  @impl true
  def handle_cast({:publish, topic, message}, state) do
    topic_subs = Map.get(state.subs, topic, %{})

    for {pid, _} <- topic_subs do
      Logger.debug("Sending #{inspect(pid)} <- #{inspect({topic, message})}")
      Process.send(pid, {topic, message}, [])
    end

    {:noreply, state}
  end

  # Info handlers --------------------------------------------------------------
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    Logger.debug("Process #{inspect(pid)} exited: #{inspect(reason)}")

    new_subs =
      Enum.map(state.subs, fn {topic, pidmap} ->
        {topic, Map.delete(pidmap, pid)}
      end)
      |> Map.new()

    {:noreply, Map.put(state, :subs, new_subs)}
  end

  @doc """
  Event handler for Redis :message notifications
  """
  @impl true
  def handle_info(
        {:redix_pubsub, _pubsub, _ref, :pmessage,
         %{channel: channel, payload: payload} = args},
        state
      ) do
    with {:ok, %Event{} = event} <- Event.from_csv(payload),
         {:ok, newstate} <- handle_pubsub_event(event, channel, state),
         {:ok, timestate} <-
           schedule_event(
             newstate,
             :queue_counts,
             :count_timer,
             state.count_delay
           ) do
      Logger.debug(inspect({:message, args}))
      Logger.debug(inspect(timestate))
      {:noreply, timestate}
    else
      error ->
        Logger.error(inspect(error))
        {:noreply, state}
    end
  end

  def handle_info(
        {:redix_pubsub, _pubsub, _ref, msg_type, args},
        state
      ) do
    Logger.debug(inspect({msg_type, args}))
    {:noreply, state}
  end

  def handle_info(:queue_counts, state) do
    Logger.debug("Queue counts")

    case timer_running(state.count_timer) do
      true ->
        Logger.debug("Timer already running, skipping")
        {:noreply, state}

      _ ->
        for {qname, queue} <- Server.queues() do
          Logger.debug("Getting counts for #{qname}")
          Server.queue_counts(queue)
        end

        {:noreply, Map.merge(state, %{queues_updated: %{}})}
    end
  end

  defp handle_pubsub_event(event, channel, state) do
    case {event.action, event.object} do
      {"counts", queue} ->
        handle_counts_event(event, queue, state)

      {_, "queue"} ->
        {:ok, state}

      {_, "job"} ->
        [_ns, qname, _sub] = String.split(channel, ":")
        queues_updated = Map.put(state.queues_updated, qname, true)
        {:ok, Map.put(state, :queues_updated, queues_updated)}
    end
  end

  defp handle_counts_event(event, queue, state) do
    with {:ok, counts} <- QueueCounts.from_csv(event.info, ?:),
         count_map <- Map.take(counts, QueueCounts.fields()),
         event_map <- Map.take(event, Event.fields()),
         global_event <- %{
           event: event_map,
           counts: count_map,
           queue: queue
         },
         {:ok, encoded} <- Jason.encode(global_event) do
      publish(:counts, encoded)
      {:ok, state}
    else
      error ->
        Logger.error(inspect(error))
        {:ok, state}
    end
  end

  defp schedule_event(state, event_type, timer, delay) do
    Logger.debug("Schedule event")

    case {timer_running(Map.get(state, timer)), map_size(state.queues_updated)} do
      {true, _} ->
        {:ok, state}

      {false, num} when num > 0 ->
        {:ok,
         Map.put(
           state,
           timer,
           Process.send_after(self(), event_type, 100)
         )}

      {_, 0} ->
        {:ok,
         Map.put(
           state,
           timer,
           Process.send_after(self(), event_type, delay)
         )}
    end
  end
end

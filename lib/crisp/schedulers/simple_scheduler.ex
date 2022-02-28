defmodule Crisp.SimpleScheduler do
  @moduledoc """
  This module manages client interactions between the application and the remote
  server.

  It has references to the Redis connection as well as any Redis PubSub subscriptions
  """
  use GenServer
  require Logger
  import Crisp.Utilities
  alias Crisp.{Client, Event, Server, Utilities}

  @namespace Application.get_env(:crisp, :namespace, "coq_queue")

  # in ms
  @run_delay 1000
  @count_delay 20000

  # values that can be tweaked
  @config_items [
    :queue_name,
    :queue_pattern,
    :run_delay,
    :count_delay
  ]

  # Genserver Client part ------------------------------------------------------
  @spec start(maybe_improper_list | map) :: {:ok, pid} | {:error, any}
  def start(opts) do
    case start_link(opts) do
      {:ok, _pid} = ok -> ok
      {:error, {:already_started, pid}} -> {:ok, pid}
      error -> error
    end
  end

  defp initial_state() do
    %{
      conn: nil,
      namespace: @namespace,
      run_timer: nil,
      run_delay: @run_delay,
      queue_name: nil,
      queue_pattern: nil,
      queue_glob: nil,
      queues_updated: %{},
      count_delay: @count_delay,
      count_timer: nil,
      status: :starting
    }
  end

  @spec start_link(maybe_improper_list | map) ::
          :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) when is_list(opts), do: start_link(Map.new(opts))

  def start_link(opts) when is_map(opts) do
    arg_state =
      Map.merge(
        initial_state(),
        Map.take(opts, @config_items)
      )

    case Redix.start_link(Application.get_env(:crisp, :redis)) do
      {:ok, conn} ->
        Map.put(arg_state, :conn, conn) |> start_server_check()

      {:error, {:already_started, conn}} ->
        Map.put(arg_state, :conn, conn) |> start_server_check()

      error ->
        error
    end
  end

  defp start_server_check(opts) do
    case {Map.get(opts, :queue_name, nil), Map.get(opts, :queue_pattern, nil)} do
      {nil, nil} ->
        {:error, :no_queue_name}

      {qname, nil} when is_binary(qname) ->
        start_server(Map.put(opts, :pattern, qname))

      {nil, pattern} when is_binary(pattern) ->
        start_server(Map.put(opts, :pattern, pattern))
    end
  end

  defp start_server(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  @spec init(map) :: {:ok, map} | {:stop, :not_started}
  def init(%{conn: _conn, namespace: namespace, pattern: pattern} = state) do
    if is_nil(Process.whereis(Client)) do
      Client.start()
      Process.sleep(50)
    end

    if is_nil(Process.whereis(Server)) do
      Server.start()
      Process.sleep(50)
    end

    Server.update_local()

    with {:ok, pubsub} <- Redix.PubSub.start_link(),
         {:ok, _ref} <-
           Redix.PubSub.psubscribe(
             pubsub,
             Utilities.pulse(namespace, pattern),
             self()
           ),
         {:ok, glob} <- :glob.compile(pattern) do
      schedule_event(
        Map.merge(state, %{pubsub: pubsub, status: :started, queue_glob: glob}),
        :count,
        :count_timer,
        50
      )
    else
      error ->
        Logger.error(inspect(error))
        {:stop, :not_started}
    end
  end

  def state(pid) do
    GenServer.call(pid, :state)
  end

  # Server part ----------------------------------------------------------------
  @impl true
  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  # Fallback handler
  @impl true
  def handle_call(args, _from, state) do
    {:reply, {:error, {:unknown_call, args}}, state}
  end

  # Info handlers --------------------------------------------------------------
  @doc """
  Event handler for Redis :pmessage notifications
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
             :count,
             :count_timer,
             newstate.count_delay
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

  def handle_info(:run, state) do
    Logger.debug(inspect(state))

    case timer_running(state.run_timer) do
      true ->
        {:noreply, state}

      _ ->
        queues = Server.queues()
        Logger.debug(inspect(queues))

        for {qname, _} <- state.queues_updated do
          Logger.debug("Getting counts for #{qname}")

          queue = Map.get(queues, String.to_existing_atom(qname))

          if is_map(queue),
            do: Server.queue_runnable(queue)
        end

        {:noreply, Map.put(state, :queues_updated, %{})}
    end
  end

  def handle_info(:count, state) do
    Logger.debug(inspect(state))

    case timer_running(state.count_timer) do
      true ->
        {:noreply, state}

      _ ->
        queues = Server.queues()
        Logger.debug(inspect(queues))

        for {_, queue} <- Server.queues() do
          with {:glob, true} <-
                 {:glob, :glob.matches(queue.name, state.queue_glob)} do
            Logger.debug("GETTING counts for #{queue.name}")
            Server.queue_runnable(queue)
          else
            {:glob, false} ->
              Logger.debug("Skipping counts for #{queue.name}")
              :ok
          end
        end

        {:noreply, state}
    end
  end

  defp handle_pubsub_event(event, channel, state) do
    case {event.action, event.object} do
      {"counts", _queue} ->
        {:ok, state}

      {_, "queue"} ->
        Server.update_local()
        {:ok, state}

      {_, "job"} ->
        [_ns, qname, _sub] = String.split(channel, ":")

        schedule_event(
          put_in(state, [:queues_updated, qname], true),
          :run,
          :run_timer,
          state.run_delay
        )
    end
  end

  defp schedule_event(state, event_type, timer, delay) do
    case timer_running(Map.get(state, timer)) do
      true ->
        {:ok, state}

      _ ->
        {:ok,
         Map.put(
           state,
           timer,
           Process.send_after(self(), event_type, delay)
         )}
    end
  end
end

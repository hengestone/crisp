defmodule Crisp.MultiScheduler do
  @moduledoc """
  This module manages client interactions between the application and the remote
  server.

  It has references to the Redis connection as well as any Redis PubSub
  subscriptions
  """
  use GenServer
  require Logger
  import Crisp.Utilities
  alias Crisp.{Client, Event, Job, Queue, Server, Utilities}

  @namespace Application.get_env(:crisp, :namespace, "coq_queue")

  # in ms
  @run_delay 1000
  @count_delay 2000

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
      count_delay: @count_delay,
      count_timer: nil,
      jobs_queued: [],
      namespace: @namespace,
      queue_glob: nil,
      queue_name: nil,
      queue_pattern: nil,
      queues_updated: %{},
      run_delay: @run_delay,
      run_timer: nil,
      slots_avail: 0,
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
      {qname, pattern} when is_binary(qname) and is_binary(pattern) ->
        start_server(
          Map.merge(opts, %{queue_pattern: pattern, queue_name: qname})
        )

      {nil, pattern} when is_binary(pattern) ->
        {:error, :no_master_queue_name}

      {qname, nil} when is_binary(qname) ->
        {:error, :no_pattern}

      {nil, nil} ->
        {:error, :no_queus}
    end
  end

  defp start_server(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @impl true
  @spec init(map) :: {:ok, map} | {:stop, :not_started}
  def init(
        %{
          conn: _conn,
          namespace: namespace,
          queue_pattern: pattern,
          queue_name: queue_name
        } = state
      ) do
    if is_nil(Process.whereis(Client)) do
      Client.start()
      Process.sleep(50)
    end

    if is_nil(Process.whereis(Server)) do
      Server.start()
      Process.sleep(50)
    end

    Server.update_local()

    with {:queue, %Queue{} = queue} <-
           {:queue,
            Map.get(Server.queues(), String.to_atom(queue_name))},
         {:ok, pubsub} <- Redix.PubSub.start_link(),
         {:ok, _ref} <-
           Redix.PubSub.psubscribe(
             pubsub,
             Utilities.pulse(namespace, pattern),
             self()
           ),
         {:ok, _ref} <-
           Redix.PubSub.subscribe(
             pubsub,
             Utilities.pulse(namespace, queue_name),
             self()
           ),
         {:ok, glob} <- :glob.compile(pattern),
         {:noreply, new_state} <-
           schedule_event(
             Map.merge(state, %{
               pubsub: pubsub,
               queue: queue,
               queue_glob: glob,
               status: :started
             }),
             :count,
             :count_timer,
             50
           ) do
      {:ok, new_state}
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
         {:noreply, new_state} <- handle_pubsub_event(event, channel, state) do
      Logger.debug(inspect({:message, args}))

      schedule_event(
        new_state,
        :count,
        :count_timer,
        new_state.count_delay
      )
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

  def handle_info(:count, state) do
    Logger.warn(
      inspect(Map.take(state, [:jobs_queued, :slots_avail, :queues_updated]))
    )

    case timer_running(state.count_timer) do
      true ->
        {:noreply, state}

      _ ->
        with {:ok, qcounts} <- Server.queue_counts(state.queue_name),
             queue <- state.queue,
             {:counts, num} <-
               {:counts, queue.runlimit - (qcounts.running + qcounts.runnable)} do
          schedule_event(
            Map.merge(state, %{
              slots_avail: num,
              jobs_queued: [],
              queues_updated: %{}
            }),
            :run,
            :run_timer,
            state.run_delay
          )
        else
          {:counts, 0} ->
            Logger.debug("count 2: slots_avail: 0")

            schedule_event(
              Map.put(state, :slots_avail, 0),
              :count,
              :count_timer,
              state.count_delay
            )
        end
    end
  end

  def handle_info(:run, state) do
    Logger.warn(
      inspect(Map.take(state, [:jobs_queued, :slots_avail, :queues_updated]))
    )

    case timer_running(state.run_timer) do
      true ->
        {:noreply, state}

      _ ->
        with {:numslots, num} when num > 0 <- {:numslots, state.slots_avail},
             {:ok, {worker_name, _num}} <- fetch_runnable(state),
             {:ok, job_csv} when is_binary(job_csv) <-
               dequeue_oldest(state, worker_name),
             {:ok, job} = Job.from_csv(job_csv) do
          Client.queue_job(state.queue, job, :runnable)

          new_state =
            Map.merge(state, %{
              jobs_queued: [job.id | state.jobs_queued],
              slots_avail: state.slots_avail - 1
            })

          schedule_event(
            new_state,
            :run,
            :run_timer,
            state.run_delay
          )
        else
          {:numslots, 0} ->
            {:noreply,
             Map.merge(state, %{
               queues_updated: %{},
               slots_avail: 0,
               jobs_queued: []
             })}

          {:error, :not_found} ->
            Logger.debug("No runnable jobs")
            {:noreply, state}

          error ->
            Logger.error(inspect(error))
            {:noreply, state}
        end
    end
  end

  @spec handle_pubsub_event(%Event{action: any, object: any}, binary, map) ::
          {:noreply, map}
  def handle_pubsub_event(event, channel, state) do
    Logger.debug(inspect({event.action, event.object}))

    case {event.action, event.object} do
      {"counts", qname} ->
        with {:ok, qcounts} <- Event.from_csv(event.info, ?:),
             num when num > 0 <- qcounts.runnable do
          schedule_event(
            put_in(state, [:queues_updated, qname], true),
            :run,
            :run_timer,
            state.run_delay
          )
        else
          _ ->
            {:noreply, state}
        end

      {_, "queue"} ->
        Server.update_local()
        {:noreply, state}

      {"runnable", "job"} ->
        [_ns, qname, _sub] = String.split(channel, ":")

        schedule_event(
          put_in(state, [:queues_updated, qname], true),
          :run,
          :run_timer,
          state.run_delay
        )

      {_, "job"} ->
        {:noreply, state}
    end
  end

  @spec fetch_runnable(map) ::
          {:ok, {binary, non_neg_integer()}} | {:error, any}
  def fetch_runnable(state) do
    Logger.info("fetch-runnable")

    with {:ok, names, worker_counts} <- worker_counts(state),
         {:random, {_name, _num} = pair} <-
           {:random, pick_random(names, worker_counts)} do
      {:ok, pair}
    else
      {:random, nil} ->
        {:error, :not_found}

      error ->
        Logger.error(inspect(error))
        error
    end
  end

  @spec worker_counts(map) ::
          {:ok, list, list}
          | {:error, any}
  def worker_counts(state) do
    with name_cmds when is_list(name_cmds) <- runnable_worker_jobs_cmd(state),
         {names, cmds} <- name_cmds |> Enum.reverse() |> Enum.reduce({[], []}, fn {name, cmd}, {names, cmds}  -> {[name | names], [cmd | cmds]} end),
         {:ok, worker_counts} <- Redix.pipeline(state.conn, cmds) do
      {:ok, names, worker_counts}
    else
      error ->
        Logger.error(inspect(error))
        error
    end
  end

  @spec dequeue_oldest(map, binary | atom) ::
          {:ok, binary} | {:error, :not_found}
  def dequeue_oldest(state, worker_name) do
    case Redix.command(
           state.conn,
           [
             "ZPOPMIN",
             subqueue(state.namespace, worker_name, :runnable)
           ]
         ) do
      {:ok, [job_csv, _ts]} ->
        {:ok, job_csv}

      _ ->
        {:error, :not_found}
    end
  end

  @spec pick_random([binary], [non_neg_integer]) :: {binary, non_neg_integer}
  def pick_random(names, counts) do
    with pairs <- List.zip([names, counts]),
         [] <- Enum.filter(pairs, fn {_name, num} -> num > 0 end) do
      nil
    else
      fpairs -> Enum.random(fpairs)
    end
  end

  @spec runnable_worker_jobs_cmd(map) :: [] | [{any, list}]
  def runnable_worker_jobs_cmd(state) do
    Server.queues()
    |> Enum.map(fn {_, queue} ->
      with {:glob, true} <-
             {:glob, :glob.matches(queue.name, state.queue_glob)} do
        {queue.name,
         [
           "ZCARD",
           subqueue(state.namespace, queue.name, :runnable)
         ]}
      else
        {:glob, false} ->
          {nil, []}
      end
    end)
    |> Enum.filter(fn {name, _cmd} -> is_binary(name) end)
  end

  defp schedule_event(state, event_type, timer, delay) when is_atom(timer) do
    case {delay, timer_running(Map.get(state, timer))} do
      {_, true} ->
        {:noreply, state}

      {0, _} ->
        {:noreply, state}

      _ ->
        Logger.info(
          "schedule event #{event_type} -> #{delay}ms #{
            inspect(
              Map.take(state, [:jobs_queued, :slots_avail, :queues_updated])
            )
          }"
        )

        {:noreply,
         Map.put(
           state,
           timer,
           Process.send_after(self(), event_type, delay)
         )}
    end
  end
end

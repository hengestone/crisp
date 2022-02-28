defmodule Crisp.Server do
  @moduledoc """
  This module manages queuing server interactions between the application and
  the remote queue manager.

  It has references to the Redis connection as well as any Redis PubSub subscriptions
  """
  use GenServer
  require Logger
  import Crisp.Utilities
  alias Crisp.{Event, Job, Queue, QueueCounts}

  @subqueues Crisp.QueueCounts.queues()

  # Genserver Client part ------------------------------------------------------
  @spec start :: :ignore | {:error, any} | {:ok, pid}
  def start() do
    with {:ok, pid} when is_pid(pid) <- start_link([]),
         {:pid, reg_pid} when is_pid(reg_pid) <-
             {:pid, Process.whereis(__MODULE__)} do
      update_local()
    else
      {:error, {:already_started, _pid}} ->
        Process.sleep(50)
        update_local()

      {:pid, nil} ->
        Process.sleep(50)
        start()

      error ->
        Logger.error("Starting #{__MODULE__} failed: #{inspect(error)}")
        error
    end
  end

  @spec start_link(maybe_improper_list | map) ::
            :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) when is_list(opts), do: start_link(Map.new(opts))

  def start_link(opts) when is_map(opts) do
    case Redix.start_link(Application.get_env(:crisp, :redis)) do
      {:ok, conn} ->
        Map.put(opts, :conn, conn)
        |> start_server()

      {:error, {:already_started, conn}} ->
        Map.put(opts, :conn, conn)
        |> start_server()

      error ->
        error
    end
  end

  defp start_server(opts) do
    GenServer.start_link(
      __MODULE__,
      Map.put(
        opts,
        :namespace,
        Application.get_env(:crisp, :namespace, "coq_queue")
      ),
      name: __MODULE__
    )
  end

  @impl true
  @spec init(%{conn: any}) :: {:ok, %{conn: any, queues: %{}, status: :started}}
  def init(%{conn: _conn} = state) do
    {:ok, Map.merge(state, %{status: :started, queues: %{}})}
  end

  def queues() do
    GenServer.call(__MODULE__, :queues)
  end

  def queue(name) when is_binary(name) do
    case Map.get(queues(), String.to_atom(name), nil) do
      nil -> {:error, :unknown_queue}
      queue -> queue
    end
  end

  @doc """
  Create a queue in the current namespace using a queue atom
  """
  def create_queue(name) when is_atom(name) do
    create_queue(Queue.new(Atom.to_string(name)))
  end

  #  Create a queue in the current namespace using a queue name
  def create_queue(name) when is_binary(name) do
    create_queue(Queue.new(name))
  end

  #  Create a queue in the current namespace using a Queue struct
  def create_queue(%Queue{} = queue) do
    GenServer.call(__MODULE__, {:create_queue, queue})
  end

  @doc """
  List all remote queues in the current namespace
  """
  def remote_queues() do
    GenServer.call(__MODULE__, :remote_queues)
  end

  def delete_queue(queue) do
    GenServer.call(__MODULE__, {:delete_queue, queue})
  end

  @doc """
  Get queue metrics
  """
  def queue_counts(queue) do
    GenServer.call(__MODULE__, {:queue_counts, queue})
  end

  @doc """
  Minimal 'scheduler' that simply takes all queued jobs and puts them in
  the runnable queue
  """
  def queue_runnable(%Queue{} = queue) do
    GenServer.call(__MODULE__, {:queue_runnable, queue})
  end

  def queue_runnable(queue_name) when is_binary(queue_name) do
    GenServer.call(__MODULE__, {:queue_runnable, queue(queue_name)})
  end

  def update_local() do
    GenServer.call(__MODULE__, :update_local)
  end

  def state() do
    GenServer.call(__MODULE__, :state)
  end

  @spec subqueues :: [:failed | :finished | :queued | :runnable | :running, ...]
  def subqueues() do
    @subqueues
  end

  # Server part ---------------------------------------------------------------
  @impl true
  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:queues, _from, state) do
    {:reply, state.queues, state}
  end

  @impl true
  def handle_call(:update_local, _from, state) do
    case list_remote_queues(state) do
      {:ok, lines} ->
        queues =
            for line <- lines do
              with {:ok, queue} <- Queue.from_csv(line) do
                {to_atom(queue.name), queue}
              else
                {:error, _} ->
                  nil
              end
            end
            |> Enum.filter(fn e -> !is_nil(e) end)
            |> Map.new()

        {:reply, :ok, Map.put(state, :queues, queues)}

      error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call(:remote_queues, _from, state) do
    case list_remote_queues(state) do
      {:ok, _names} = return ->
        {:reply, return, state}

      error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:create_queue, %Queue{} = queue}, _from, state) do
    retval =
        Redix.pipeline(
          state.conn,
          [
            [
              "ZADD",
              "#{state.namespace}_queues",
              time_usecs(),
              Queue.to_csv(queue)
            ],
            [
              "PUBLISH",
              pulse(state.namespace, queue.name),
              Event.new(:create, :queue, info: queue.name)
              |> Event.to_csv()
            ]
          ]
        )

    aname = to_atom(queue.name)
    {:reply, {retval, queue}, put_in(state, [:queues, aname], queue)}
  end

  @impl true
  def handle_call({:delete_queue, %Queue{} = queue}, _from, state) do
    cmds =
        :lists.append(
          remove_subqueue_commands(queue.name, state.namespace),
          [
            [
              "ZREM",
              "#{state.namespace}_queues",
              Queue.to_csv(queue)
            ],
            [
              "PUBLISH",
              pulse(state.namespace, queue.name),
              Event.new(:remove, :queue, info: queue.name)
              |> Event.to_csv()
            ]
          ]
        )

    retval = Redix.pipeline(state.conn, cmds)

    aname = to_atom(queue.name)
    new_queues = Map.delete(state.queues, aname)
    {:reply, {retval, queue}, Map.put(state, :queues, new_queues)}
  end

  @impl true
  def handle_call({:queue_counts, %Queue{} = queue}, from, state) do
    handle_call({:queue_counts, queue.name}, from, state)
  end

  @impl true
  def handle_call({:queue_counts, queue_name}, _from, state)
      when is_binary(queue_name) do
    retval =
        Redix.pipeline(
          state.conn,
          for db_name <- @subqueues do
            [
              "ZCARD",
              subqueue(state.namespace, queue_name, db_name)
            ]
          end
        )

    case retval do
      {:ok, counts} ->
        Redix.command(
          state.conn,
          [
            "PUBLISH",
            pulse(state.namespace, queue_name),
            Event.new(
              :counts,
              to_atom(queue_name),
              info:
                  Crisp.QueueCounts.new(counts)
                  |> Crisp.QueueCounts.to_csv(":")
            )
            |> Event.to_csv()
          ]
        )

        {:reply, {:ok, QueueCounts.new(counts)}, state}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_call({:queue_runnable, %Queue{} = queue}, from, state) do
    case handle_call({:queue_counts, queue.name}, from, state) do
      {:reply, {:error, _error}, _state} = retval ->
        retval

      {:reply, {:ok, %QueueCounts{} = qcounts}, _state} ->
        queue_runnable_jobs(queue, qcounts, state)
    end
  end

  @impl true
  def handle_call(args, _from, state) do
    {:reply, {:error, {:unknown_call, args}}, state}
  end

  defp queue_runnable_jobs(%Queue{} = queue, %QueueCounts{} = qcounts, state) do
    retval =
        Redix.command(
          state.conn,
          [
            "ZPOPMIN",
            subqueue(state.namespace, queue.name, :queued),
            queue.runlimit - (qcounts.running + qcounts.runnable)
          ]
        )

    case retval do
      {:ok, []} ->
        {:reply, {:ok, {qcounts, []}}, state}

      {:ok, lines} when is_list(lines) ->
        cmds =
            queue_mult_job_commands(
              queue.name,
              Enum.take_every(lines, 2),
              state.namespace
            )

        {status, retval} = Redix.pipeline(state.conn, cmds)
        {:reply, {status, {qcounts, retval}}, state}

      error ->
        error
    end
  end

  defp list_remote_queues(state) do
    Redix.command(
      state.conn,
      [
        "ZRANGE",
        "#{state.namespace}_queues",
        "0",
        "-1"
      ]
    )
  end

  defp queue_mult_job_commands(queue_name, lines, namespace) do
    List.foldr(
      lines,
      [],
      fn job_csv, acc ->
        with {:ok, job} <- Job.from_csv(job_csv),
             {add, publish} <-
                 queue_job_commands(queue_name, job, :runnable, namespace) do
          [add | [publish | acc]]
        else
          error ->
            Logger.warn(
              "Skipping invalid job description:\n#{inspect(job_csv)}\n#{
                  inspect(error)
              }"
            )

            acc
        end
      end
    )
  end

  defp queue_job_commands(queue_name, job, subqueue, namespace) do
    {
      [
        "ZADD",
        subqueue(namespace, queue_name, subqueue),
        time_usecs(),
        Job.to_csv(job)
      ],
      [
        "PUBLISH",
        pulse(namespace, queue_name),
        Event.new(subqueue, :job, info: Job.to_csv(job, ":"))
        |> Event.to_csv()
      ]
    }
  end

  defp remove_subqueue_commands(queue_name, namespace) do
    Enum.map(
      QueueCounts.queues(),
      fn subqueue ->
        [
          "ZREMRANGEBYSCORE",
          subqueue(namespace, queue_name, subqueue),
          "-inf",
          "+inf"
        ]
      end
    )
  end
end

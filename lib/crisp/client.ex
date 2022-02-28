defmodule Crisp.Client do
  @moduledoc """
  This module manages client interactions between the application and the remote
  server.

  It has references to the Redis connection as well as any Redis PubSub subscriptions
  """
  use GenServer
  import Crisp.Utilities
  require Logger
  alias Crisp.{Event, Job, Queue}

  @namespace Application.get_env(:crisp, :namespace, "coq_queue")

  # Genserver Client part ------------------------------------------------------
  @spec start :: :ok
  def start() do
    case start_link([]) do
      {:ok, _} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      error -> error
    end
  end

  @spec start_link(maybe_improper_list | map) ::
          :ignore | {:error, any} | {:ok, pid}
  def start_link(opts) when is_list(opts), do: start_link(Map.new(opts))

  def start_link(opts) when is_map(opts) do
    case Redix.start_link(Application.get_env(:crisp, :redis)) do
      {:ok, conn} ->
        Map.put(opts, :conn, conn) |> start_server()

      {:error, {:already_started, conn}} ->
        Map.put(opts, :conn, conn) |> start_server()

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
        @namespace
      ),
      name: __MODULE__
    )
  end

  @impl true
  @spec init(%{conn: any}) :: {:ok, %{conn: any, queues: %{}, status: :started}}
  def init(%{conn: _conn} = state) do
    {:ok, Map.merge(state, %{status: :started})}
  end

  @spec queue_job(binary | Crisp.Queue.t(), Crisp.Job.t()) ::
          {:ok, binary} | {:error, any}
  def queue_job(queue_name, %Job{} = job)
      when is_binary(queue_name) do
    queue_job(queue_name, job, :queued)
  end

  def queue_job(%Queue{} = queue, %Job{} = job) do
    queue_job(queue.name, job, :queued)
  end

  @spec queue_job(binary | Crisp.Queue.t(), Crisp.Job.t(), atom | binary) ::
          {:ok, binary} | {:error, any}
  def queue_job(%Queue{} = queue, %Job{} = job, subqueue) do
    queue_job(queue.name, job, to_atom(subqueue))
  end

  @spec queue_job(binary, Crisp.Job.t(), atom, boolean) ::
          {:ok, binary} | {:error, any}
  def queue_job(queue_name, %Job{} = job, subqueue, publish \\ true)
      when is_binary(queue_name) and is_atom(subqueue) do
    GenServer.call(__MODULE__, {:queue_job, queue_name, job, subqueue, publish})
  end

  @doc """
  Removes a job from the specified queue

  E.g. `dequeue_job("myqueue", myjob, :runnable)` where `job` is an instance of
  `Crisp.Job`  or
  `dequeue_job(queue, myjob, :runnable)` where `queue` is an instance of
  `Crisp.Queue`.

  The subqueue indicates the current state of the job, and is one of
   `[:queued, :runnable, :running, :finished, :failed]`

  Returns
  `{:ok, job_id}`
  """
  @spec dequeue_job(
          binary | Crisp.Queue.t(),
          Crisp.Job.t(),
          atom | binary
        ) :: binary
  def dequeue_job(%Queue{} = queue, %Job{} = job, subqueue) do
    dequeue_job(queue.name, job, to_atom(subqueue))
  end

  def dequeue_job(queue_name, %Job{} = job, subqueue)
      when is_binary(queue_name) and is_atom(subqueue) do
    GenServer.call(__MODULE__, {:dequeue_job, queue_name, job, subqueue})
  end

  @doc """
  Fetch jobs newer than a specified timestamp (score) from the specified
  queue and subqueue

  E.g. `fetch_newer_jobs("myqueue", :runnable, 1585956973788247, 100)`.

  Returns
  `{:ok, [job_data_1, score_1, job_data_2, score_2, ]}`

  """

  @spec fetch_newer_jobs(
          binary,
          atom | binary,
          non_neg_integer,
          non_neg_integer
        ) :: {:ok, [binary]} | {:error, any}
  def fetch_newer_jobs(queue_name, subqueue, start, count) do
    GenServer.call(
      __MODULE__,
      {:fetch_newer_jobs, queue_name, subqueue, start, count}
    )
  end

  @spec state :: binary
  def namespace() do
    Map.get(GenServer.call(__MODULE__, :state) ,:namespace)
  end

  @spec state :: map
  def state() do
    GenServer.call(__MODULE__, :state)
  end

  # Server part ---------------------------------------------------------------
  @impl true
  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(
        {:queue_job, queue_name, %Job{} = job, subqueue, publish},
        _from,
        state
      )
      when is_atom(subqueue) do
    retval =
      Redix.command(
        state.conn,
        [
          "ZADD",
          subqueue(state.namespace, queue_name, subqueue),
          time_usecs(),
          Job.to_csv(job)
        ]
      )

    case {retval, publish} do
      {{:ok, _}, true} ->
        Redix.command(
          state.conn,
          [
            "PUBLISH",
            pulse(state.namespace, queue_name),
            Event.new(subqueue, :job, info: Job.to_csv(job, ":"))
            |> Event.to_csv()
          ]
        )

        {:reply, {:ok, job.id}, state}

      {{:ok, _}, false} ->
        {:reply, {:ok, job.id}, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:dequeue_job, queue_name, job, subqueue}, _from, state) do
    retval =
      Redix.command(
        state.conn,
        [
          "ZREM",
          subqueue(state.namespace, queue_name, subqueue),
          Job.to_csv(job)
        ]
      )

    case retval do
      {:ok, [1]} ->
        {:reply, {:ok, job.id}, state}

      {:ok, [0]} ->
        {:reply, {:error, :not_found}, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call(
        {:fetch_newer_jobs, queue_name, subqueue, start, count},
        _from,
        state
      ) do
    retval =
      Redix.command(
        state.conn,
        [
          "ZRANGEBYSCORE",
          subqueue(state.namespace, queue_name, subqueue),
          "(#{start}",
          "+inf",
          "WITHSCORES",
          "LIMIT",
          0,
          count
        ]
      )

    case retval do
      {:ok, results} ->
        {:reply, {:ok, results}, state}

      error ->
        {:reply, error, state}
    end
  end
end

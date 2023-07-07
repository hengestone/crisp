defmodule Crisp.WorkerSupervisor do
  @moduledoc """
  This module manages worker processes:
   - subscribes to updates from a channel pulse
   - may poll for runnable jobs
   - starts jobs
   - publishes job alive pulse
   - moves job to finished or failed
  """
  use GenServer
  require Logger
  import Crisp.Utilities
  alias Crisp.{Client, Event, Job}

  # in ms
  @dequeue_delay 100
  @poll_delay 1000
  @poll_max_fetch 100
  @run_delay 50

  # values that can be tweaked
  @config_items [
    :queue_name,
    :dequeue_delay,
    :poll_delay,
    :poll_max_fetch,
    :run_delay
  ]

  # Genserver Client part ------------------------------------------------------
  @spec start(keyword) :: :ignore | {:error, any} | {:ok, pid}
  def start(list) do
    start_link(list)
  end

  @spec start_link(list) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(init_arg) do
    queue_name = Keyword.get(init_arg, :queue_name, "default_queue_name")
    name = "WorkerSupervisor-#{queue_name}"
    IO.inspect({:init_arg, init_arg})
    GenServer.start_link(__MODULE__, init_arg, name: String.to_atom(name))
  end

  def initial_state() do
    %{
      dequeue_delay: @dequeue_delay,
      dequeue_timer: nil,
      # Map of pid => exit reason
      exited_pids: %{},
      namespace: "",
      pid: nil,
      poll_delay: @poll_delay,
      # Timestamp of the last job fetched
      poll_last: 0,
      poll_max_fetch: @poll_max_fetch,
      poll_timer: nil,
      pubsub: nil,
      run_delay: @run_delay,
      run_timer: nil,
      state: :initial_state,
      subscriptions: [],
      # Map of job_id, {time_usecs, spec}
      runnable: %{},
      # Map of %Module{job_id=>{time_usecs, pid, module, :state}}
      running: %{},
      # Map of pid=>{Module, job_id}
      running_pids: %{},
      # Map of glob/worker_spec
      worker_specs: %{}
    }
  end

  @impl true
  @spec init(list) ::
          {:ok, map}
          | {:stop, any}
  def init(args) do
    if is_nil(Process.whereis(Client)), do: Client.start()

    arg_state =
      Map.merge(
        initial_state(),
        Map.new(args)
        |> Map.take(@config_items)
      )

    queue_name = Keyword.get(args, :queue_name, "default_queue_name")
    IO.inspect(queue_name)

    with {:ok, pubsub} <- Redix.PubSub.start_link(),
         pulse <- pulse(Client.namespace(), queue_name),
         {:ok, _ref} <- Redix.PubSub.subscribe(pubsub, pulse, self()) do
      # Start fetching jobs after a short tick
      Process.send_after(self(), :schedule_poll, 10)

      {
        :ok,
        Map.merge(
          arg_state,
          %{
            namespace: Client.namespace(),
            pid: self(),
            pubsub: pubsub,
            queue_name: queue_name,
            state: :started,
            subscriptions: [pulse]
          }
        )
      }
    else
      {:error, e} ->
        {:stop, e}
    end
  end

  @spec register_worker(atom, integer, map) :: :ok
  def register_worker(module, max, pattern) do
    GenServer.call(__MODULE__, {:register_worker, module, max, pattern})
  end

  def register_worker(pid, module, max, pattern) when is_pid(pid) do
    GenServer.call(pid, {:register_worker, module, max, pattern})
  end

  @spec state :: map
  def state() do
    GenServer.call(__MODULE__, :state)
  end

  def stop() do
    case Process.whereis(__MODULE__) do
      nil ->
        :ok

      _ ->
        GenServer.call(__MODULE__, :stop)
    end
  end

  @spec update_state(list) :: map
  def update_state(opts) do
    GenServer.call(__MODULE__, {:update_state, opts})
  end

  @spec dequeue_exited_jobs :: {:ok, map}
  def dequeue_exited_jobs() do
    GenServer.call(__MODULE__, :dequeue_exited_jobs)
  end

  def poll_runnable_queue() do
    GenServer.call(__MODULE__, :poll_runnable_queue)
  end

  def start_runnable() do
    GenServer.call(__MODULE__, :start_runnable)
  end

  # Server part ----------------------------------------------------------------
  @impl true
  def terminate(_reason, state) do
    if state.state == :subscribed do
      Redix.PubSub.unsubscribe(
        state.pubsub,
        pulse(state.namespace, state.queue_name),
        self()
      )
    end
  end

  # Info handlers --------------------------------------------------------------
  @doc """
  Event handler for Redis :subscribed notifications
  """
  @impl true
  def handle_info(
        {:redix_pubsub, _pubsub, _ref, :subscribed, %{} = info},
        state
      ) do
    {
      :noreply,
      Map.merge(state, %{state: :subscribed})
      |> Map.merge(info)
    }
  end

  #  Event handler for Redis :message notifications
  @impl true
  def handle_info(
        {
          :redix_pubsub,
          _pubsub,
          _ref,
          :message,
          %{channel: _channel, payload: payload}
        },
        state
      ) do
    with {:ok, %Event{} = event} <- Event.from_csv(payload),
         {:ok, newstate} <- handle_pubsub_event(event, state) do
      {:noreply, newstate}
    else
      error ->
        Logger.error(inspect(error))
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    Logger.debug("Process #{inspect(pid)} exited: #{inspect(reason)}")

    dequeue_timer =
      case timer_running(state.dequeue_timer) do
        true ->
          state.dequeue_timer

        _ ->
          Process.send_after(self(), :dequeue_exited_jobs, state.dequeue_delay)
      end

    new_exited_pids = Map.put(state.exited_pids, pid, reason)

    {
      :noreply,
      Map.merge(
        state,
        %{
          dequeue_timer: dequeue_timer,
          exited_pids: new_exited_pids
        }
      )
    }
  end

  @impl true
  def handle_info(:start_runnable, state) do
    start_runnable(state)
  end

  def handle_info(:poll_runnable_queue, state) do
    with {:runnable, 0} <- {:runnable, map_size(state.runnable)},
         {:ok, jobs} =
           Client.fetch_newer_jobs(
             state.queue_name,
             :runnable,
             state.poll_last,
             @poll_max_fetch
           ),
         poll_state <- handle_polled_jobs(jobs, state),
         last_poll_state <- Map.merge(poll_state, %{poll_last: List.last(jobs)}) do
      {:noreply, last_poll_state}
    else
      {:runnable, _} ->
        schedule_event(state.run_delay, :run_timer, :start_runnable, state)

      error ->
        Logger.error("Error polling new runnable jobs:\n#{inspect(error)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:schedule_poll, state) do
    schedule_event(
      state.poll_delay,
      :poll_timer,
      :scheduled_poll_handler,
      state
    )
  end

  @impl true
  def handle_info(:schedule_start_runnable, state) do
    schedule_event(state.run_delay, :run_timer, :start_runnable, state)
  end

  def handle_info(:scheduled_poll_handler, state) do
    {:noreply, polled_state} = handle_info(:poll_runnable_queue, state)

    handle_info(:schedule_poll, polled_state)
  end

  def handle_info(:dequeue_exited_jobs, state) do
    {_pids, {_queue_name, new_running_jobs, new_running_pids}} =
      Enum.map_reduce(
        state.exited_pids,
        {state.queue_name, state.running, state.running_pids},
        &handle_job_exit/2
      )

    {
      :noreply,
      Map.merge(
        state,
        %{
          exited_pids: %{},
          running_pids: new_running_pids,
          running: new_running_jobs
        }
      )
    }
  end

  # Info support functions -----------------------------------------------------
  def start_runnable(state) do
    IO.inspect(state)
    {result, {new_runnable, new_running, _state}} =
      Enum.map_reduce(
        state.runnable,
        {state.runnable, state.running, state},
        &start_worker_queued/2
      )

    {_discard, pids} =
      Enum.map_reduce(
        result,
        [],
        fn rval, acc ->
          case rval do
            {:ok, pid, module, job_id} ->
              {true, [{pid, {module, job_id}} | acc]}

            _ ->
              {false, acc}
          end
        end
      )

    new_running_pids = Map.merge(state.running_pids, Map.new(pids))

    running_state =
      Map.merge(
        state,
        %{
          running: new_running,
          runnable: new_runnable,
          running_pids: new_running_pids
        }
      )

    handle_info(:schedule_poll, running_state)
  end

  def handle_polled_jobs(jobs, state) do
    {_vals, new_state} =
      Enum.map_reduce(
        Enum.take_every(jobs, 2),
        state,
        fn job_info, acc ->
          case Job.from_csv(job_info) do
            {:ok, job} ->
              handle_pubsub_event(
                %Crisp.Event{
                  action: "runnable",
                  info: Job.to_csv(job, ":"),
                  object: "job"
                },
                acc
              )

            error ->
              Logger.info(
                "Skipping invalid job spec \n#{job_info}\n#{inspect(error)}"
              )

              {:ok, acc}
          end
        end
      )

    new_state
  end

  @doc """
  Handles new job runnable notifications and puts them into state.runnable list
  """
  def handle_pubsub_event(
        %Crisp.Event{
          action: "runnable",
          info: job_info,
          object: "job"
        } = event,
        state
      ) do
    Logger.debug("Job RUNNABLE event #{Event.to_csv(event)}")

    new_runnable_map =
      Enum.map(
        state.worker_specs,
        fn {glob, spec} ->
          with {:glob, true} <- {:glob, :glob.matches(job_info, glob)},
               {:ok, %Job{} = job} <- Job.from_csv(job_info, ?:),
               {:running_check, job_id} when is_binary(job_id) <-
                 {:running_check, Map.get(state.running, job.id, job.id)} do
            {job.id, {time_usecs(), spec, job}}
          else
            {:glob, false} ->
              Logger.debug("glob #{inspect(glob)} is not a match, skipping")
              {}

            {:running_check, %{}} ->
              Logger.debug("Job already running, skipping")

              {}

            {:error, msg} ->
              Logger.error(inspect(msg))
              {}
          end
        end
      )
      |> Enum.filter(fn tuple -> tuple != {} end)
      |> Map.new()

    {
      :ok,
      put_in(state, [:runnable], Map.merge(state.runnable, new_runnable_map))
    }
  end

  @spec handle_pubsub_event(Crisp.Event.t(), map) :: {:ok, map}
  def handle_pubsub_event(%Event{} = event, state) do
    Logger.debug("Ignoring event #{Event.to_csv(event)}")
    {:ok, state}
  end

  def schedule_event(delay, timer_key, msg, state) do
    case {delay, timer_running(Map.get(state, timer_key))} do
      # timer disabled, pass
      {0, _} ->
        {:noreply, state}

      # timer already running, pass
      {_, true} ->
        {:noreply, state}

      # when the timer has never run, we poll immediately
      {_d, nil} ->
        Logger.info("schedule_event fast: #{timer_key}")
        timer = Process.send_after(self(), msg, 10)
        {:noreply, Map.put(state, timer_key, timer)}

      # schedule timer
      {d, false} ->
        timer = Process.send_after(self(), msg, d)
        {:noreply, Map.put(state, timer_key, timer)}
    end
  end

  # Call handlers --------------------------------------------------------------
  @impl true
  def handle_call(:state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:stop, _from, state) do
    {:stop, :normal, state}
  end

  @impl true
  def handle_call({:update_state, opts}, _from, state) do
    new_state =
      Map.merge(
        state,
        Map.new(opts)
        |> Map.take(@config_items)
      )

    {:reply, new_state, new_state}
  end

  def handle_call(:poll_runnable_queue, _from, state) do
    {:noreply, new_state} = handle_info(:poll_runnable_queue, state)
    {:reply, new_state.runnable, new_state}
  end

  @impl true
  def handle_call({:register_worker, module, max, %Job{} = job}, _from, state) do
    with pattern <- job_pattern(job),
         {:ok, glob} <- :glob.compile(pattern),
         %{} = worker_specs <-
           Map.merge(
             state.worker_specs,
             %{
               glob => %{
                 module: module,
                 max: max
               }
             }
           ) do
      {
        :reply,
        {:ok, worker_specs},
        Map.merge(state, %{worker_specs: worker_specs})
      }
    else
      error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call(:start_runnable, _from, state) do
    {_, new_state} = start_runnable(state)
    {:reply, new_state.runnable, new_state}
  end

  def handle_call(:dequeue_exited_jobs, _from, state) do
    {:noreply, new_state} = handle_info(:dequeue_exited_jobs, state)
    {:reply, {:ok, new_state.running_pids}, new_state}
  end

  # Call support functions -----------------------------------------------------
  def start_worker_queued(
        {job_id, {_usecs, spec, job}},
        {runnable, running, state}
      ) do
    with {:allowed, true} <- {:allowed, workers_available(running, spec)},
         {:dequeue, {:ok, _job_id}} <-
           {:dequeue, Client.dequeue_job(state.queue_name, job, :runnable)},
         {:ok, pid} <- start_worker(spec, job, state) do
      Client.queue_job(state.queue_name, job, :running)
      mod_running = Map.get(running, spec.module, %{})
      Process.send(pid, {:init_callback, nil}, [])

      mod_running_new =
        Map.put(mod_running, job_id, {time_usecs(), job, pid, :running})

      Process.monitor(pid)
      Process.unlink(pid)

      {
        {:ok, pid, spec.module, job.id},
        {
          Map.delete(runnable, job_id),
          Map.put(running, spec.module, mod_running_new),
          state
        }
      }
    else
      {:allowed, false} = result ->
        {result, {runnable, running, state}}

      {:dequeue, {:error, :not_found}} = result ->
        Logger.debug("Unable to dequeue job #{job_id}")
        {result, {runnable, running, state}}

      {:enqueue, {:error, enqueue_error}} = result ->
        Logger.error(
          "Unable to add job to :running queue: #{inspect(enqueue_error)}"
        )

        {result, {runnable, running, state}}

      unexpected ->
        Logger.error(
          "Got unexpected result when starting worker: #{inspect(unexpected)}"
        )

        {unexpected, {runnable, running, state}}
    end
  end

  @spec workers_available(map, %{max: integer, module: atom}) :: boolean
  def workers_available(running, %{module: module} = spec)
      when is_atom(module) do
    map_size(Map.get(running, module, %{})) < spec.max
  end

  @doc """
  Start worker process
  """
  def start_worker(spec, job, _state) do
    case apply(spec.module, :start, [job]) do
      :ignore ->
        Logger.error(
          "Calling #{spec.module}.start() with job argument #{inspect(job)} returned IGNORE"
        )

        {:error, :ignore}

      {:error, term} ->
        Logger.error(
          "Calling #{spec.module}.start() with job argument #{inspect(job)} returned\n#{inspect(term)}"
        )

        {:error, :worker_start_error}

      {:ok, pid} ->
        {:ok, pid}

      {:ok, pid, _} ->
        {:ok, pid}
    end
  end

  @doc """
  Removes pid from the exited, running, and running_pids structures and
  Dequeue from the :running queue
  Move to either :failed or :finished queues
  """
  def handle_job_exit(
        {pid, reason},
        {queue_name, running_jobs, running_pids}
      ) do
    with {module, job_id} <- Map.get(running_pids, pid),
         %{} = module_workers <- Map.get(running_jobs, module),
         {_usec, job, _pid, subqueue} <- Map.get(module_workers, job_id),
         {:ok, _} <- ensure_dequeued(queue_name, job, subqueue),
         new_queue <- exit_queue(reason),
         {:ok, _job_id} <- Client.queue_job(queue_name, job, new_queue),
         {_pid, new_running_pids} <- Map.pop(running_pids, pid),
         {_worker_info, new_module_workers} <- Map.pop(module_workers, job_id),
         %{} = new_running_jobs <-
           Map.put(running_jobs, module, new_module_workers) do
      {pid, {queue_name, new_running_jobs, new_running_pids}}
    else
      unexpected ->
        Logger.error(
          "Unable to requeue job from #{inspect(pid)}:\n#{inspect(unexpected)}"
        )

        {pid, {queue_name, running_jobs, running_pids}}
    end
  end

  @spec exit_queue(:normal | :shutdown | {:shutdown, any} | any) ::
          :failed | :finished
  defp exit_queue(reason) do
    case reason do
      :normal -> :finished
      :shutdown -> :finished
      {:shutdown, {:ok, _}} -> :finished
      _ -> :failed
    end
  end

  defp ensure_dequeued(queue_name, job, subqueue) do
    case Client.dequeue_job(queue_name, job, subqueue) do
      {:ok, _} = success -> success
      {:error, :not_found} -> {:ok, :notfound}
      error -> error
    end
  end

  # Create a glob to match job specs.
  # Pros: string based
  # Cons: probably not as fast as can be
  @spec job_pattern(Crisp.Job.t()) :: binary
  def job_pattern(%Job{} = job) do
    Enum.map(
      Job.fields(),
      fn field ->
        case Map.get(job, field) do
          nil -> "*"
          val -> val
        end
      end
    )
    |> Enum.join("[:,]")
  end
end

defmodule Crisp.WorkerSupervisorTest do
  use ExUnit.Case
  import ExUnit.CaptureLog
  import Crisp.Utilities

  alias Crisp.{
    Client,
    Job,
    JobWorkerBehaviour,
    Queue,
    Server,
    WorkerSupervisor
  }

  require Crisp.TestWorker

  describe "start" do
    test "start helper" do
      assert :ok == Server.start()
      qname = utc_id()

      assert :ok ==
               WorkerSupervisor.start(
                 queue_name: qname,
                 poll_delay: 0,
                 run_delay: 0
               )
    end
  end

  describe "job handlers" do
    setup do
      Server.start()
      qname = "suptest-#{time_usecs()}"
      queue = Queue.new(qname, runlimit: 10)
      {{:ok, _}, queue} = Server.create_queue(queue)

      :ok =
        WorkerSupervisor.start(queue_name: qname, poll_delay: 0, run_delay: 0)

      Mox.defmock(TestWorker, for: JobWorkerBehaviour)

      on_exit(fn ->
        Process.sleep(250)
        Server.start()
        Process.sleep(250)
        Server.delete_queue(queue)
      end)

      %{queue: queue}
    end

    test "job pattern", %{queue: queue} do
      job = Job.new("supervisor_test_job", node: "telmate-scso")

      {:ok, glob} =
        :glob.compile(
          WorkerSupervisor.job_pattern(%Job{
            version: Job.version(),
            node: "telmate-scso"
          })
        )

      assert :glob.matches(Job.to_csv(job, ?:), glob)
      Server.delete_queue(queue)
    end

    test "register job", %{queue: queue} do
      job = Job.new("supervisor_test_job", node: "telmate-scso")

      {:ok, jobspecs} =
        WorkerSupervisor.register_worker(__MODULE__, 10, %Job{
          version: Job.version(),
          node: "telmate-scso"
        })

      [{glob, %{} = spec}] = Map.to_list(jobspecs)

      assert :glob.matches(Job.to_csv(job, ?:), glob)
      assert spec.max == 10
      assert spec.module == __MODULE__
      Server.delete_queue(queue)
    end

    test "job added to internal runnable from fake pubsub event", %{
      queue: _queue
    } do
      job = Job.new("supervisor_test_job", node: "provider-site")

      # Client.queue_job(queue, job)

      {:ok, _jobspecs} =
        WorkerSupervisor.register_worker(TestWorker, 10, %Job{
          version: Job.version(),
          node: job.node
        })

      state = WorkerSupervisor.state()

      {status, newstate} =
        WorkerSupervisor.handle_pubsub_event(
          %Crisp.Event{
            action: "runnable",
            info: Job.to_csv(job, ?:),
            object: "job"
          },
          state
        )

      assert status == :ok
      assert Map.has_key?(newstate.runnable, job.id)
    end

    test "job added to internal runnable from real pubsub event", %{
      queue: queue
    } do
      job = Job.new("supervisor_test_job", node: "provider-site")

      Client.queue_job(queue, job)

      {:ok, _jobspecs} =
        WorkerSupervisor.register_worker(TestWorker, 10, %Job{
          version: Job.version(),
          node: job.node
        })

      {qstatus, {_qcounts, returns}} = Server.queue_runnable(queue)
      assert qstatus == :ok
      assert is_list(returns)
      Process.sleep(250)

      state = WorkerSupervisor.state()

      assert Map.has_key?(state.runnable, job.id)
    end

    test "workers available -> true", %{queue: _queue} do
      state = WorkerSupervisor.state()

      assert WorkerSupervisor.workers_available(state.running, %{
               module: TestModule,
               max: 10
             })
    end

    test "workers available -> false", %{queue: _queue} do
      state = WorkerSupervisor.state()

      assert false ==
               WorkerSupervisor.workers_available(state.running, %{
                 module: TestModule,
                 max: 0
               })
    end

    test "workers available -> false2", %{queue: _queue} do
      running_map =
        Enum.map(1..10, fn i ->
          job = Job.new("supervisor_test_job_#{i}", node: "provider-site")
          {job.id, %{job: job}}
        end)
        |> Map.new()

      running = %{Crisp.TestWorker => running_map}

      spec = %{
        module: Crisp.TestWorker,
        max: 10
      }

      assert false == WorkerSupervisor.workers_available(running, spec)
    end

    test "start worker simple", %{queue: _queue} do
      spec = %{
        module: Crisp.TestWorker,
        max: 10
      }

      job = Job.new("supervisor_test_job", node: "provider-site")

      state = WorkerSupervisor.state()

      {status, pid} = WorkerSupervisor.start_worker(spec, job, state)
      assert status == :ok
      assert is_pid(pid)
    end

    test "start worker, limits OK", %{queue: queue} do
      spec = %{
        module: Crisp.TestWorker,
        max: 10
      }

      job = Job.new("supervisor_test_job", node: "provider-site")
      {status, id} = Client.queue_job(queue, job, :runnable)
      assert status == :ok
      assert id == job.id

      state = WorkerSupervisor.state()

      {_result, {runnable, running, _state}} =
        WorkerSupervisor.start_worker_queued(
          {job.id, {time_usecs(), spec, job}},
          {state.runnable, state.running, state}
        )

      assert runnable == %{}
      assert Map.has_key?(running, Crisp.TestWorker)
      test_workers = Map.get(running, Crisp.TestWorker)
      assert Map.has_key?(test_workers, job.id)

      job_info = Map.get(test_workers, job.id)
      {_usecs, %Job{} = _stored_job, pid, run_status} = job_info

      assert is_pid(pid)
      assert run_status == :running
    end

    test "start multiple workers, limits OK", %{queue: queue} do
      spec = %{
        module: Crisp.TestWorker,
        max: 10
      }

      {:ok, _jobspecs} =
        WorkerSupervisor.register_worker(spec.module, spec.max, %Job{
          version: Job.version(),
          node: "provider-site"
        })

      for i <- 1..2 do
        job = Job.new("supervisor_test_job_#{i}", node: "provider-site")
        {status, id} = Client.queue_job(queue, job, :runnable)
        assert status == :ok
        assert id == job.id
      end

      # Wait for notifications
      Process.sleep(250)

      _runnable = WorkerSupervisor.start_runnable()
      new_state = WorkerSupervisor.state()

      assert new_state.runnable == %{}
      assert Map.has_key?(new_state.running, Crisp.TestWorker)
      test_workers = Map.get(new_state.running, Crisp.TestWorker)
      assert map_size(test_workers) == 2
      assert map_size(new_state.running_pids) == 2
    end

    test "start multiple workers, exceeding limit", %{queue: queue} do
      spec = %{
        module: Crisp.TestWorker,
        max: 3
      }

      {:ok, _jobspecs} =
        WorkerSupervisor.register_worker(spec.module, spec.max, %Job{
          version: Job.version(),
          node: "provider-site"
        })

      for i <- 1..10 do
        job = Job.new("supervisor_test_job_#{i}", node: "provider-site")
        {status, id} = Client.queue_job(queue, job, :runnable)
        assert status == :ok
        assert id == job.id
      end

      # Wait for notifications
      Process.sleep(250)

      _runnable = WorkerSupervisor.start_runnable()
      new_state = WorkerSupervisor.state()

      assert map_size(new_state.runnable) == 7
      assert Map.has_key?(new_state.running, Crisp.TestWorker)
      test_workers = Map.get(new_state.running, Crisp.TestWorker)
      assert map_size(test_workers) == 3
    end
  end

  describe "job handlers with test genserver" do
    setup do
      Server.start()
      qname = "suptest-#{time_usecs()}"
      queue = Queue.new(qname, runlimit: 10)
      {{:ok, _}, queue} = Server.create_queue(queue)

      :ok =
        WorkerSupervisor.start(queue_name: qname, poll_delay: 0, run_delay: 0)

      spec = %{
        module: Crisp.TestWorker,
        max: 10
      }

      {:ok, _jobspecs} =
        WorkerSupervisor.register_worker(spec.module, spec.max, %Job{
          version: Job.version(),
          node: "provider-site"
        })

      job = Job.new("supervisor_test_job", node: "provider-site")
      {:ok, _id} = Client.queue_job(queue, job, :runnable)
      Process.sleep(250)

      _runnable = WorkerSupervisor.start_runnable()
      Process.sleep(250)
      state = WorkerSupervisor.state()

      test_workers = Map.get(state.running, Crisp.TestWorker)
      job_info = Map.get(test_workers, job.id)
      {_usecs, %Job{} = _stored_job, pid, _run_status} = job_info

      on_exit(fn ->
        Server.start()
        Process.sleep(250)
        Server.delete_queue(queue)
      end)

      %{queue: queue, pid: pid, spec: spec}
    end

    test "start worker, test dequeue", %{queue: queue, pid: pid, spec: spec} do
      new_state = WorkerSupervisor.state()

      {result_pid, {result_queue, result_running, result_running_pids}} =
        WorkerSupervisor.handle_job_exit(
          {pid, :normal},
          {new_state.queue_name, new_state.running, new_state.running_pids}
        )

      assert result_pid == pid
      assert result_queue == queue.name
      assert result_running_pids == %{}
      assert Map.has_key?(result_running, spec.module)
      assert Map.get(result_running, spec.module) == %{}
    end

    test "start worker, handle exit manually", %{
      pid: pid,
      spec: spec
    } do
      # Make sure automatic removal doesn't run
      WorkerSupervisor.dequeue_exited_jobs()
      exit_result = Crisp.TestWorker.exit(pid, :normal)
      assert exit_result == :ok
      Process.sleep(100)
      state = WorkerSupervisor.state()

      {:noreply, new_state} =
        WorkerSupervisor.handle_info(
          :dequeue_exited_jobs,
          Map.put(state, :dequeue_delay, 10_000)
        )

      assert map_size(Map.get(new_state, :exited_pids)) == 0

      assert map_size(Map.get(new_state, :exited_pids)) == 0
      assert new_state.running_pids == %{}
      assert Map.has_key?(new_state.running, spec.module)
      assert Map.get(new_state.running, spec.module) == %{}
    end

    test "start worker, handle exit via timer", %{
      pid: pid,
      spec: spec
    } do
      exit_result = Crisp.TestWorker.exit(pid, :normal)
      assert exit_result == :ok
      Process.sleep(250)
      new_state = WorkerSupervisor.state()

      assert map_size(Map.get(new_state, :exited_pids)) == 0
      assert new_state.running_pids == %{}
      assert Map.has_key?(new_state.running, spec.module)
      assert Map.get(new_state.running, spec.module) == %{}
    end

    test "start worker, handle worker crash", %{
      pid: pid,
      spec: spec
    } do
      Process.exit(pid, :kill)
      Process.sleep(250)
      new_state = WorkerSupervisor.state()

      assert map_size(Map.get(new_state, :exited_pids)) == 0
      assert new_state.running_pids == %{}
      assert Map.has_key?(new_state.running, spec.module)
      assert Map.get(new_state.running, spec.module) == %{}
    end
  end

  describe "job polling" do
    setup do
      Server.start()
      qname = "suptest-#{time_usecs()}"
      queue = Queue.new(qname, runlimit: 10)
      {{:ok, _}, queue} = Server.create_queue(queue)

      :ok =
        WorkerSupervisor.start(queue_name: qname, poll_delay: 0, run_delay: 0)

      spec = %{
        module: Crisp.TestWorker,
        max: 5
      }

      # on_exit(fn ->
      #   Server.start()
      #   Process.sleep(250)
      #   Server.delete_queue(queue)
      # end)

      %{queue: queue, spec: spec}
    end

    test "poll runnable", %{
      queue: queue,
      spec: spec
    } do
      fun = fn ->
        for i <- 1..10 do
          {:ok, _id} =
            Client.queue_job(
              queue,
              Job.new("poll_runnable_job_#{i}", node: "provider-site"),
              :runnable
            )
        end
      end

      capture_log(fun)
      Process.sleep(250)

      {:ok, _jobspecs} =
        WorkerSupervisor.register_worker(spec.module, spec.max, %Job{
          version: Job.version(),
          node: "provider-site"
        })

      runfun = fn ->
        runnable = WorkerSupervisor.poll_runnable_queue()
        assert map_size(runnable) == 10
      end

      capture_log(runfun)
    end
  end
end

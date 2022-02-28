defmodule Crisp.TestWorker do
  @moduledoc """
  Example JobWorker
  """
  @behaviour Crisp.JobWorkerBehaviour

  alias Crisp.Job

  use GenServer, restart: :transient

  @impl true
  @spec start(Crisp.Job.t()) :: :ignore | {:error, any} | {:ok, pid}
  def start(%Job{} = job) do
    GenServer.start(__MODULE__, %{job: job})
  end

  @impl true
  @spec init(any) :: {:ok, any}
  def init(state) do
    {:ok, state}
  end

  @doc """
  Returns the job id
  """
  @impl true
  @spec id(pid) :: {:ok, binary}
  def id(pid) do
    GenServer.call(pid, :id)
  end

  @doc """
  Tells the worker to exit async with the given reason
  """
  def exit(pid, reason) do
    GenServer.cast(pid, {:exit, reason})
  end

  # Server part ----------------------------------------------------------------
  @impl true
  @spec handle_call(:id, {pid, any}, map) :: {:reply, {:ok, binary}, map}
  def handle_call(:id, _from, %{job: job} = state) do
    {:reply, {:ok, job.id}, state}
  end

  @impl true
  @spec handle_cast({:exit, any}, map) :: {:stop, any, map}
  def handle_cast({:exit, reason}, state) do
    {:stop, reason, state}
  end
end

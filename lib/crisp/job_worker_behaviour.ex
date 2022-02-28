defmodule Crisp.JobWorkerBehaviour do
  @moduledoc """
  This module specifies the behaviour of workers started using a Job spec:
  """

  @callback start(Crisp.Job.t()) :: :ignore | {:error, any} | {:ok, pid}
  @callback id(pid) :: {:ok, binary}
end

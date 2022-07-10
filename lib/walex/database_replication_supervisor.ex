defmodule WalEx.DatabaseReplicationSupervisor do
  use Supervisor

  alias WalEx.Adapters.Postgres.PostgrexReplication
  alias WalEx.Replication

  def start_link(config) do
    Supervisor.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(config) do
    children = [
      Replication,
      {PostgrexReplication, config}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end

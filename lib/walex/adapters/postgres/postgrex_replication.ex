defmodule WalEx.Adapters.Postgres.PostgrexReplication do
  use Postgrex.ReplicationConnection
  require Logger

  defmodule(State,
    do:
      defstruct(
        postgrex_params: nil,
        delays: [0],
        publication_name: nil,
        slot_name: nil,
        wal_position: nil,
        max_replication_lag_in_mb: 0,
        replication_server_pid: nil,
        step: :disconnected
      )
  )

  def acknowledge_lsn(lsn) do
    GenServer.call(__MODULE__, {:ack_lsn, lsn})
  end

  def start_link(opts) do
    # Automatically reconnect if we lose connection.
    extra_opts = [
      auto_reconnect: true
    ]

    Postgrex.ReplicationConnection.start_link(
      __MODULE__,
      opts,
      extra_opts ++ (opts |> Keyword.get(:postgrex_params)) ++ [name: __MODULE__]
    )
  end

  @impl true
  def init(
        postgrex_params: postgrex_params,
        publications: publications,
        slot_name: slot_name,
        wal_position: {xlog, offset} = wal_position,
        max_replication_lag_in_mb: max_replication_lag_in_mb
      )
      when is_list(postgrex_params) and is_list(publications) and
             (is_binary(slot_name) or is_atom(slot_name)) and is_binary(xlog) and
             is_binary(offset) and is_number(max_replication_lag_in_mb) do
    publications
    |> generate_publication_name()
    |> case do
      {:ok, publication_name} ->
        state = %State{
          publication_name: publication_name,
          postgrex_params: postgrex_params,
          wal_position: wal_position,
          slot_name: slot_name |> String.replace("'", "\\'") |> String.downcase(),
          max_replication_lag_in_mb: max_replication_lag_in_mb
        }

        {:ok, state}

      {:error, error} ->
        {:stop, error, nil}
    end
  end

  @impl true
  def handle_connect(state = %State{}) do
    query =
      "SELECT COUNT(*) >= 1 FROM pg_replication_slots WHERE slot_name = '#{state.slot_name}'"

    {:query, query, %{state | step: :check_slot}}
  end

  @impl true
  def handle_result([%{rows: [["f"]]}], %State{step: :check_slot} = state) do
    query =
      "CREATE_REPLICATION_SLOT #{state.slot_name} TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"

    {:query, query, %{state | step: :create_slot}}
  end

  @impl true
  def handle_result(results, %State{step: :check_slot} = state) do
    results
    |> handle_result(%{state | step: :create_slot})
  end

  @impl true
  def handle_result(results, %State{step: :create_slot, wal_position: {xlog, offset}} = state)
      when is_list(results) do
    query =
      "START_REPLICATION SLOT #{state.slot_name} LOGICAL #{xlog}/#{offset} (proto_version '1', publication_names '#{state.publication_name}')"

    {:stream, query, [],
     %{state | step: :streaming, replication_server_pid: Process.whereis(WalEx.Replication)}}
  end

  @impl true
  # https://www.postgresql.org/docs/14/protocol-replication.html
  def handle_data(
        <<?w, _wal_start::64, _wal_end::64, _clock::64, rest::binary>>,
        state = %State{replication_server_pid: replication_server_pid}
      ) do
    replication_server_pid
    |> Process.send({:postgrex_replication, rest}, [:noconnect])

    {:noreply, state}
  end

  def handle_data(<<?k, wal_end::64, _clock::64, reply>>, state) do
    messages =
      case reply do
        1 -> [<<?r, wal_end + 1::64, wal_end + 1::64, wal_end + 1::64, current_time()::64, 0>>]
        0 -> []
      end

    {:noreply, messages, state}
  end

  @impl true
  def handle_call({:ack_lsn, {xlog, offset}}, from, state) do
    from
    |> Postgrex.ReplicationConnection.reply(:ok)

    <<last_processed_lsn::integer-64>> = <<xlog::integer-32, offset::integer-32>>

    messages = [
      <<?r, last_processed_lsn::64, last_processed_lsn::64, last_processed_lsn::64,
        current_time()::64, 0>>
    ]

    {:noreply, messages, state}
  end

  @impl true
  def handle_call({:ack_lsn, _}, from, state) do
    from
    |> Postgrex.ReplicationConnection.reply(:error)

    {:noreply, state}
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time(), do: System.os_time(:microsecond) - @epoch

  defp generate_publication_name(publications) when is_list(publications) do
    with true <- Enum.all?(publications, fn pub -> is_binary(pub) end),
         publication_name when publication_name != "" <-
           publications
           |> Enum.intersperse(",")
           |> IO.iodata_to_binary()
           |> String.replace("'", "\\'") do
      {:ok, publication_name}
    else
      _ -> {:error, :bad_publications}
    end
  end

  defp generate_publication_name(_publications) do
    {:error, :bad_publications}
  end
end

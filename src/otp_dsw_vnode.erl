-module(otp_dsw_vnode).
-behaviour(riak_core_vnode).

-include_lib("riak_core_lite/include/riak_core_vnode.hrl").
-include("otp_dsw_records.hrl").

-export([init/1, handle_command/3, handle_coverage/4, handle_exit/3,
         handle_handoff_data/2, handle_handoff_command/2, terminate/2,
         code_change/3, delete/1, encode_handoff_item/2, handle_handoff_command/3,
         handle_overload_command/3, handle_overload_info/2,
         handoff_cancelled/1, handoff_finished/2, handoff_starting/2,
         is_empty/1]).

-record(state, {
    idx  :: eleveldb:db_ref()
}).

%% Initialize the vnode with a reference to the LevelDB database.
init([]) ->
    Path = case application:get_env(otp_dsw, leveldb_path) of
        {ok, P} ->
            P;
        undefined ->
            error({env_var_not_set, "leveldb_path"})
    end,
    {ok, Ref} = eleveldb:open(Path, []),
    {ok, #state{idx = Ref}}.

%% Implement the commands for the vnode.
handle_command({create_partition, Topic, Partition}, _Sender, State) ->
    %% Create the key for storing the partition in LevelDB
    Key = <<Topic/binary, Partition:32>>,

    %% Write the new partition to LevelDB
    ok = eleveldb:put(State#state.idx, Key, term_to_binary([]), []),

    {reply, ok, State};

%% Add a replica to a partition.
handle_command({add_replica, PartitionId, Replica}, _Sender, State) ->
    {ok, Partition} = get_partition(PartitionId, State),
    NewReplicas = lists:umerge([Replica], Partition#partition.replicas),
    NewPartition = Partition#partition{replicas = NewReplicas},
    put_partition(PartitionId, NewPartition, State),
    {reply, ok, State};

%% Remove a replica from a partition.
handle_command({remove_replica, PartitionId, Replica}, _Sender, State) ->
    {ok, Partition} = get_partition(PartitionId, State),
    NewReplicas = Partition#partition.replicas -- [Replica],
    NewPartition = Partition#partition{replicas = NewReplicas},
    put_partition(PartitionId, NewPartition, State),
    {reply, ok, State};

%% Set the leader for a partition.
handle_command({set_leader, PartitionId, Leader}, _Sender, State) ->
    {ok, Partition} = get_partition(PartitionId, State),
    NewPartition = Partition#partition{leader = Leader},
    put_partition(PartitionId, NewPartition, State),
    {reply, ok, State};

%% Add an In-Sync Replica (ISR) to a partition.
handle_command({add_isr, PartitionId, InSyncReplica}, _Sender, State) ->
    {ok, Partition} = get_partition(PartitionId, State),
    NewIsr = lists:umerge([InSyncReplica], Partition#partition.isr),
    NewPartition = Partition#partition{isr = NewIsr},
    put_partition(PartitionId, NewPartition, State),
    {reply, ok, State};

%% Remove an In-Sync Replica (ISR) from a partition.
handle_command({remove_isr, PartitionId, InSyncReplica}, _Sender, State) ->
    {ok, Partition} = get_partition(PartitionId, State),
    NewIsr = Partition#partition.isr -- [InSyncReplica],
    NewPartition = Partition#partition{isr = NewIsr},
    put_partition(PartitionId, NewPartition, State),
    {reply, ok, State};

%% Create a new topic with a specified number of partitions
handle_command({create_topic, TopicName, NumPartitions}, _Sender, State) ->
    %% Create the partitions for the new topic
    lists:foreach(
        fun(PartitionId) ->
            create_partition(TopicName, PartitionId, State)
        end,
        lists:seq(1, NumPartitions)
    ),
    {reply, ok, State};

%% Retrieve information about a specific topic
handle_command({get_topic, TopicName}, _Sender, State) ->
    %% Get all partitions for the topic
    Partitions = get_partitions_by_topic(TopicName, State),
    {reply, {ok, Partitions}, State};

%% Delete a topic and all its associated partitions
handle_command({delete_topic, TopicName}, _Sender, State) ->
    %% Get all partitions for the topic
    Partitions = get_partitions_by_topic(TopicName, State),

    %% Delete each partition associated with the topic
    lists:foreach(
        fun(Partition) ->
            delete_partition(Partition#partition.id, TopicName, State)
        end,
        Partitions
    ),
    {reply, ok, State};

%% Write a message to the specified partition
handle_command({write_message, PartitionId, Key, Value}, _Sender, State) ->
    %% Create a new message
    Message = #message{
        key = Key,
        value = Value,
        timestamp = erlang:system_time(millisecond),
        partition_id = PartitionId,
        offset = get_next_offset(PartitionId, State)
    },

    %% Write the message to LevelDB
    ok = write_message_to_db(Message, State),

    {reply, ok, State};

%% Read a message from the specified partition and offset
handle_command({read_message, PartitionId, Offset}, _Sender, State) ->
    %% Read the message from LevelDB
    {ok, Message} = read_message_from_db(PartitionId, Offset, State),

    {reply, {ok, Message}, State};

%% Handle any other commands.
handle_command(_Other, _Sender, State) ->
    {noreply, State}.

handle_coverage(_Req, _Filter, _Sender, State) ->
    {reply, not_implemented, State}.

handle_exit(_Reason, _OldState, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

handle_handoff_command(_Cmd, State) ->
    {reply, not_implemented, State}.

terminate(_Reason, State) ->
    ok = eleveldb:close(State#state.idx),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

delete(_Key) ->
    {ok, not_implemented}.

encode_handoff_item(_Key, _Data) ->
    {ok, not_implemented}.

handle_handoff_command(_Cmd, _Sender, State) ->
    {reply, not_implemented, State}.

handle_overload_command(_Cmd, _Sender, State) ->
    {reply, not_implemented, State}.

handle_overload_info(_Info, State) ->
    {noreply, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_Idx, State) ->
    {ok, State}.

handoff_starting(_Idx, State) ->
    {ok, State}.

is_empty(_State) ->
    false.

%% internal functions

%% Add functions for getting and updating partitions.
get_partition(PartitionId, State) ->
    Key = <<PartitionId:32>>,
    case eleveldb:get(State#state.idx, Key, []) of
        {ok, Binary} ->
            {ok, binary_to_term(Binary)};
        not_found ->
            {error, not_found}
    end.

put_partition(PartitionId, Partition, State) ->
    Key = <<PartitionId:32>>,
    Value = term_to_binary(Partition),
    ok = eleveldb:put(State#state.idx, Key, Value, []),
    ok.

%% Create a new partition for the specified topic
create_partition(TopicName, PartitionId, State) ->
    Partition = #partition{
        id = PartitionId,
        topic = TopicName,
        replicas = [],
        isr = [],
        leader = undefined
    },
    %% Add the new partition to partition storage (LevelDB via Riak Core Lite)
    Key = <<TopicName/binary, PartitionId:32>>,
    ok = eleveldb:put(State#state.idx, Key, term_to_binary(Partition), []),
    ok.

%% Get all partitions for a specified topic
get_partitions_by_topic(TopicName, State) ->
    Fun = fun({Key, Value}) ->
        <<Topic:((byte_size(Key) - 4)*8)/binary, _/binary>> = Key,
        case Topic of
            TopicName ->
                {halt, [binary_to_term(Value)]};
            _ ->
                continue
        end
    end,
    {ok, Partitions} = eleveldb:fold(State#state.idx, Fun, []),
    Partitions.

%% Delete a specific partition for the specified topic
delete_partition(PartitionId, TopicName, State) ->
    Key = <<TopicName/binary, PartitionId:32>>,
    ok = eleveldb:delete(State#state.idx, Key, []),
    ok.

write_message_to_db(Message, State) ->
    Key = <<(Message#message.partition_id):32, (Message#message.offset):64>>,
    ok = eleveldb:put(State#state.idx, Key, term_to_binary(Message), []),
    ok.

read_message_from_db(PartitionId, Offset, State) ->
    Key = <<PartitionId:32, Offset:64>>,
    case eleveldb:get(State#state.idx, Key, []) of
        {ok, BinaryMessage} ->
            {ok, binary_to_term(BinaryMessage)};
        not_found ->
            {error, not_found}
    end.

get_next_offset(PartitionId, State) ->
    %% It's possible to implement a more efficient way to get the next available offset
    Offset = 0,
    case read_message_from_db(PartitionId, Offset, State) of
        {error, not_found} ->
            Offset;
        {ok, _Message} ->
            find_next_offset(PartitionId, Offset, State)
    end.

find_next_offset(PartitionId, Offset, State) ->
    case read_message_from_db(PartitionId, Offset + 1, State) of
        {error, not_found} ->
            Offset + 1;
        {ok, _Message} ->
            find_next_offset(PartitionId, Offset + 1, State)
    end.

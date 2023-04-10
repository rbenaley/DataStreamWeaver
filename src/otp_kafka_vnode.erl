-module(otp_kafka_vnode).
-behaviour(riak_core_vnode).

-include_lib("riak_core_lite/include/riak_core_vnode.hrl").
-include("otp_kafka_records.hrl").

-export([init/1, handle_command/3, handle_coverage/4, handle_exit/3,
         handle_handoff_data/2, handle_handoff_command/2, terminate/2,
         code_change/3]).

-record(state, {
    vnode_id,
    partition_db
}).

init([VnodeId]) ->
    %% Initialize RocksDB
    {ok, DbOpts} = erocksdb:options(),
    {ok, CFDescriptors} = erocksdb:cf_descriptors([{"default", DbOpts}]),
    {ok, PartitionDb} = erocksdb:open("otp_kafka_partitions", DbOpts, CFDescriptors),
    {ok, #state{vnode_id=VnodeId, partition_db=PartitionDb}}.

handle_command({create_partition, Topic, Partition}, _Sender, State) ->
    %% Code to create a new partition using RocksDB
    Key = term_to_binary({Topic, Partition}),
    {ok, _} = erocksdb:put(State#state.partition_db, Key, <<>>),
    {reply, ok, State};

handle_command({store_message, Topic, Partition, Message}, _Sender, State) ->
    %% Code to store a message in RocksDB
    Key = term_to_binary({Topic, Partition}),
    {ok, _} = erocksdb:put(State#state.partition_db, Key, term_to_binary(Message)),
    {reply, ok, State};

handle_command(_Cmd, _Sender, State) ->
    {reply, {error, unknown_command}, State}.

handle_coverage(_Req, _KeySpaces, _Repl, State) ->
    {reply, {error, not_implemented}, State}.

handle_exit(_Reason, _Pid, State) ->
    {noreply, State}.

handle_handoff_data(_Data, State) ->
    {noreply, State}.

handle_handoff_command(_Cmd, State) ->
    {reply, {error, not_implemented}, State}.

terminate(_Reason, State) ->
    %% Close RocksDB
    erocksdb:close(State#state.partition_db),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

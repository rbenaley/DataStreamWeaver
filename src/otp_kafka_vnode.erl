-module(otp_kafka_vnode).
-behaviour(riak_core_vnode).

-include_lib("riak_core_lite/include/riak_core_vnode.hrl").
-include("otp_kafka_records.hrl").

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
    Path = case application:get_env(otp_kafka, leveldb_path) of
        {ok, P} ->
            P;
        undefined ->
            error({env_var_not_set, "leveldb_path"})
    end,
    {ok, Ref} = eleveldb:open(Path, []),
    {ok, #state{idx = Ref}}.

%% Implement the commands for your vnode.
handle_command({create_partition, Topic, Partition}, _Sender, State) ->
    %% Create the key for storing the partition in LevelDB
    Key = <<Topic/binary, Partition:32>>,

    %% Write the new partition to LevelDB
    ok = eleveldb:put(State#state.idx, Key, term_to_binary([]), []),

    {reply, ok, State};
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
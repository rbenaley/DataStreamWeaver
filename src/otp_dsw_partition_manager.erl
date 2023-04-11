-module(otp_dsw_partition_manager).
-export([create_partition/2, get_partition/1]).

-include("otp_dsw_records.hrl").

%% Creates a new partition with the specified number and replicas.
create_partition(Topic, PartitionId) ->
    Partition = #partition{
        id = PartitionId,
        topic = Topic,
        replicas = [],
        isr = [],
        leader = undefined
    },
    %% Add the new partition to partition storage (LevelDB via Riak Core Lite)
    otp_dsw_vnode:put({Topic, PartitionId}, Partition),
    ok.

%% Retrieves an existing partition by its id.
get_partition({Topic, PartitionId}) ->
    case otp_dsw_vnode:get({Topic, PartitionId}) of
        {ok, Partition} ->
            {ok, Partition};
        not_found ->
            {error, not_found}
    end.

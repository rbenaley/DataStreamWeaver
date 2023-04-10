-record(topic, {
    name :: binary(),
    num_partitions :: non_neg_integer()
}).

-record(partition, {
    id        :: non_neg_integer(),
    topic     :: binary(),
    replicas  :: [riak_core_lite:partition_id()],
    isr       :: [riak_core_lite:partition_id()],
    leader    :: riak_core_lite:partition_id() | undefined,
    vnode     :: riak_core_lite:vnode_id() | undefined
}).

-record(topic, {
    name :: binary(),
    num_partitions :: non_neg_integer()
}).

-record(partition, {
    topic :: binary(),
    partition_id :: non_neg_integer(),
    vnode :: pid() % 'replicas' and 'leader' ==> 'vnode' (Riak Core)
}).

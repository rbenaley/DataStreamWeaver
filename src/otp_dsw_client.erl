-module(otp_dsw_client).
-behaviour(gen_server).

-include_lib("kafka_protocol/include/kpro.hrl").

-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    socket      :: inet:socket(),
    buffer      :: binary(),
    vnode       :: riak_core_vnode:vnode()
}).

-define(API_KEY_PRODUCE, 0).
-define(API_KEY_FETCH, 1).
-define(ERROR_OFFSET_OUT_OF_RANGE, 1).

%% Start the client
start_link(Socket) ->
    gen_server:start_link(?MODULE, Socket, []).

%% Initialize the client and set the socket to passive mode
init(Socket) ->
    inet:setopts(Socket, [{active, once}]),
    {ok, #state{socket = Socket}}.

%% Handle incoming data from the client
handle_info({tcp, _Socket, Data}, State) ->
    process_data(Data, State),
    inet:setopts(State#state.socket, [{active, once}]),
    {noreply, State};

%% Handle client disconnect
handle_info({tcp_closed, _Socket}, State) ->
    {stop, normal, State}.

%% Ignore other messages
handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    ok = gen_tcp:close(State#state.socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% internal functions

%% Processes incoming data from clients, parses requests, and dispatches them to the appropriate request handlers
process_data(Data, State) ->
    %% Parse the received data to determine the type of request
    Request = parse_request(Data),

    %% Handle the request based on its type (producer or consumer request)
    case Request of
        {producer_request, ProduceRequest} ->
            %% Handle producer request (e.g., write message to the specified topic and partition)
            handle_producer_request(ProduceRequest, State);
        {consumer_request, FetchRequest} ->
            %% Handle consumer request (e.g., read message from the specified topic, partition, and offset)
            handle_consumer_request(FetchRequest, State);
        _Other ->
            %% Handle unknown or unsupported request types
            handle_unknown_request(State)
    end.

%% Parses incoming client requests and determines the request type (producer, consumer, or unknown)
parse_request(BinaryData) ->
    %% Decode the Kafka message
    case decode_kafka_message(BinaryData) of
        {ok, ?API_KEY_PRODUCE, Request} ->
            {producer_request, Request};
        {ok, ?API_KEY_FETCH, Request} ->
            {consumer_request, Request};
        {error, _Reason} ->
            unknown_request
    end.

decode_kafka_message(Data) ->
    case kafka_protocol:decode_request(Data) of
        {ok, API_Key, _API_Version, _CorrelationId, _ClientId, Request} ->
            {API_Key, Request};
        {error, Reason} ->
            {error, Reason}
    end.

%% Handles Kafka producer requests, sends messages to appropriate partitions and returns a response to the client
handle_producer_request(ProduceRequest, State) ->
    %% Extract topic and messages from the request
    {ok, TopicData} = kafka_protocol:produce_request(ProduceRequest),
    %% Process each topic and its messages
    Responses = lists:map(fun({Topic, Messages}) ->
        PartitionResponses = process_messages(Topic, Messages, State),
        {Topic, PartitionResponses}
    end, TopicData),
    %% Send the ProduceResponse back to the client
    Response = kafka_protocol:encode_produce_response(Responses),
    gen_tcp:send(State#state.socket, Response),
    {noreply, State}.

process_messages(Topic, Messages, State) ->
    PartitionResponses = lists:map(fun({Partition, MessageSet}) ->
        %% Send messages to the corresponding vnode and partition
        {ok, Result} = otp_dsw_vnode:write_messages(Topic, Partition, MessageSet, State#state.vnode),
        {Partition, Result}
    end, Messages),
    {Topic, PartitionResponses}.

%% Handles Kafka consumer requests, fetches messages from appropriate partitions and returns a response to the client
handle_consumer_request(FetchRequest, State) ->
    %% Extract partition, offset, and max bytes from the request
    {Topic, Partition, Offset, MaxBytes} = kafka_protocol:fetch_request(FetchRequest),
    %% Fetch messages from the specified partition and offset
    {ok, Messages} = otp_dsw_vnode:get_messages(Topic, Partition, Offset, MaxBytes, State#state.vnode),
    %% Use Kafka Fetch API key when encoding the response
    Response = encode_kafka_fetch_response(Partition, Offset, Messages, ?API_KEY_FETCH),
    send_response(Response, State#state.socket, State),
    {noreply, State}.

encode_kafka_fetch_response(Partition, Offset, Messages, API_Key) ->
    [{API_Key, [{Partition, [{Offset, Messages}]}]}].

send_response(Response, Socket, _State) ->
    gen_tcp:send(Socket, Response).

%% Handles unknown requests by returning an appropriate error response to the client
handle_unknown_request(State) ->
    {noreply, State}.

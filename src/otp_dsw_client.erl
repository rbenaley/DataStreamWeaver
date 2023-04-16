-module(otp_dsw_client).
-behaviour(gen_server).

-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    socket :: inet:socket()
}).

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

%% Process data from the client
process_data(Data, State) ->
    %% Parse the received data to determine the type of request
    Request = parse_request(Data),

    %% Handle the request based on its type (producer or consumer request)
    case Request of
        {producer_request, Topic, Partition, Key, Value} ->
            %% Handle producer request (e.g., write message to the specified topic and partition)
            handle_producer_request(Topic, Partition, Key, Value, State);
        {consumer_request, Topic, Partition, Offset} ->
            %% Handle consumer request (e.g., read message from the specified topic, partition, and offset)
            handle_consumer_request(Topic, Partition, Offset, State);
        _Other ->
            %% Handle unknown or unsupported request types
            handle_unknown_request(State)
    end.

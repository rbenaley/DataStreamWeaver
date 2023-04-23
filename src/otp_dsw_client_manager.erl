-module(otp_dsw_client_manager).
-behaviour(gen_server).

-export([start/1, start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    listen_socket :: inet:socket()
}).

%% Start the TCP server and bind it to the specified port
start(Port) ->
    {ok, ListenSocket} = gen_tcp:listen(Port, [{active, false}, {reuseaddr, true}, binary, {packet, 0}, {nodelay, true}]),
    spawn(fun() -> accept_connections(ListenSocket) end).

%% Start the client manager
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Initialize the client manager and listen for incoming connections
init([]) ->
    {ok, ListenSocket} = gen_tcp:listen(0, [{active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(ListenSocket),
    io:format("Client manager listening on port ~p~n", [Port]),
    gen_server:cast(self(), accept_client),
    {ok, #state{listen_socket = ListenSocket}}.

%% Handle incoming client connections
handle_cast(accept_client, State) ->
    gen_tcp:async_accept(State#state.listen_socket, {self(), tcp_accept}),
    {noreply, State}.

handle_info({tcp_accept, Socket}, State) ->
    {ok, ClientSocket} = gen_tcp:accept(Socket),
    gen_tcp:controlling_process(ClientSocket, self()),
    gen_server:cast(self(), accept_client),
    spawn(fun() -> otp_dsw_client:start_link(ClientSocket) end),
    {noreply, State}.

%% Ignore other messages
handle_call(_Request, _From, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    ok = gen_tcp:close(State#state.listen_socket),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internal functions

accept_connections(ListenSocket) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    {ok, _Pid} = otp_dsw_client:start_link(Socket),
    accept_connections(ListenSocket).
%%%-------------------------------------------------------------------
%% @doc otp_kafka public API
%% @end
%%%-------------------------------------------------------------------

-module(otp_kafka_app).

-behaviour(application).

-export([start/2, stop/1]).

-type state() :: {counter, integer()}.
-spec start(application:start_type(), []) -> {ok, pid()} | {ok, pid(), state()} | {error, term()}.
start(_StartType, _StartArgs) ->
%%  riak_core:register_vnode_module(?MODULE, otp_kafka_vnode),
    case application:ensure_all_started(otp_kafka) of
        {ok, _StartedApps} ->
            otp_kafka_sup:start_link();
        {error, {App, Reason}} ->
            {error, {App, Reason}}
    end.

-spec stop(any()) -> ok.
stop(_State) ->
    application:stop(otp_kafka),
    ok.

%% internal functions
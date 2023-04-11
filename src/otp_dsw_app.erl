%%%-------------------------------------------------------------------
%% @doc otp_dsw public API
%% @end
%%%-------------------------------------------------------------------

-module(otp_dsw_app).

-behaviour(application).

-export([start/2, stop/1]).

-type state() :: {counter, integer()}.
-spec start(application:start_type(), []) -> {ok, pid()} | {ok, pid(), state()} | {error, term()}.
start(_StartType, _StartArgs) ->
    case application:ensure_all_started(otp_dsw) of
        {ok, _StartedApps} ->
            otp_dsw_sup:start_link();
        {error, {App, Reason}} ->
            {error, {App, Reason}}
    end.

-spec stop(any()) -> ok.
stop(_State) ->
    application:stop(otp_dsw),
    ok.

%% internal functions

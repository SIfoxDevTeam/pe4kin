-module(pe4kin_gun_srv).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([get_gun_conn/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("hut/include/hut.hrl").

-record(state, {conn :: pid()}).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

get_gun_conn(Pid) ->
    gen_server:call(Pid, get_gun_conn, 5000).

init([]) ->
    {ok, Conn} = pe4kin_http:open(),
    {ok, #state{conn = Conn}}.

handle_call(get_gun_conn, _From, #state{conn = Conn} = State) ->
    {reply, Conn, State};
handle_call(_Request, _From, State) ->
    {reply, {error, bad_request}, State}.
handle_cast(Msg, State) ->
    ?log(warning, "Unexpected cast ~p; state ~p", [Msg, State]),
    {noreply, State}.

handle_info({gun_down, _Pid, _Protocol, Reason, _KilledStreams, _UnprocessedStreams}, State) ->
    ?log(debug, "gun_down Reason ~p", [Reason]),
    {noreply, State};
handle_info({gun_up, Pid, _}, State) ->
    ?log(debug, "gun_up", []),
    {noreply, State#state{conn = Pid}};
handle_info(Info, State) ->
    ?log(warning, "Unexpected info msg ~p; state ~p", [Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
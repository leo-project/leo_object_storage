%%======================================================================
%%
%% Leo Compaction Manager
%%
%% Copyright (c) 2012 Rakuten, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------
%% Leo Object Storage - Server
%% @doc
%% @end
%%======================================================================
-module(leo_compaction_manager_fsm).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-behaviour(gen_fsm).

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0]).
-export([start/1, suspend/0, resume/0, status/0, done_child/1]).

-export([init/1,
         state_idle/3,
         state_running/3,
         state_suspend/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4,
         format_status/2]).

-record(state, {
          %% get from leo_object_storage_api // storage_pids       :: list(),
          max_num_of_concurrent = 1  :: integer(),
          num_of_concurrent     = 0  :: integer(),
          rest_pids             = [] :: list(),
          start_datetime        = 0  :: integer() %% greg sec
         }).
-define(DEF_TIMEOUT, 5).

%%====================================================================
%% API
%%====================================================================
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
-spec(start_link() ->
             ok | {error, any()}).
start_link() ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE,
                       [], []).

%%--------------------------------------------------------------------
%% API - object operations.
%%--------------------------------------------------------------------
%% @doc start compaction
%%
-spec(start(binary()) ->
             ok | {error, any()}).
start(ParamBin) ->
    case parse_param(ParamBin) of
        {ok, {PidList, NumConcurrent}} ->
            gen_fsm:sync_send_event({local, ?MODULE}, {start, PidList, NumConcurrent}, ?DEF_TIMEOUT);
        Error ->
            Error
    end.

suspend() ->
    void.
resume() ->
    void.
status() ->
    void.
done_child(_Pid) ->
    void.

%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
% Description: Initiates the server
init([]) ->
    {ok, state_idle, #state{}}.

state_idle(_Event, _From, State) ->
    {next_state, state_running, State}.
state_running(_Event, _From, State) ->
    {next_state, state_running, State}.
state_suspend(_Event, _From, State) ->
    {next_state, state_running, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.
handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.
    
%% Function: handle_info(Info, State) -> {noreply, State}          |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, _StateName, _State) ->
    ok.

%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

format_status(_Opt, [_PDict, State]) ->
    State.

%%====================================================================
%% INNER FUNCTIONS
%%====================================================================
%%--------------------------------------------------------------------
%% object operations.
%%--------------------------------------------------------------------
%% @doc
%% @private
parse_param(ParamBin) when is_binary(ParamBin) ->
    void.

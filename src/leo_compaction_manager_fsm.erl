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
%% Leo Compaction Manager - FSM
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
-export([start/3, suspend/0, resume/0, status/0, stop/1]).

-export([init/1,
         state_idle/2,
         state_idle/3,
         state_running/2,
         state_running/3,
         state_suspend/2,
         state_suspend/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4,
         format_status/2]).

-record(state, {max_num_of_concurrent = 1  :: integer(),
                inspect_fun                :: function(),
                target_pids           = [] :: list(),
                in_progress_pids      = [] :: list(),
                child_pids            = [] :: any(),    %% orddict(), {Chid :: pid(), hasJob :: boolean()}
                reserve_pids          = [] :: list(),
                start_datetime        = 0  :: integer() %% gregory-sec
               }).

-define(DEF_TIMEOUT, 5).

%%====================================================================
%% API
%%====================================================================
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
-spec(start_link() ->
             {ok, pid()} | ignore | {error, any()}).
start_link() ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE,
                       [], []).

%%--------------------------------------------------------------------
%% API - object operations.
%%--------------------------------------------------------------------
%% @doc start compaction
-spec(start(list(string() | atom()), integer(), fun()) ->
             ok | {error, any()}).
start(TargetPids, MaxConNum, InspectFun) ->
    gen_fsm:sync_send_event(
      ?MODULE, {start, TargetPids, MaxConNum, InspectFun}, ?DEF_TIMEOUT).

stop(_Id) ->
    void.


%% @doc Suspend compaction
-spec(suspend() ->
             term()).
suspend() ->
    gen_fsm:sync_send_event(?MODULE, suspend, ?DEF_TIMEOUT).


%% @doc Resume compaction
-spec(resume() ->
             term()).
resume() ->
    gen_fsm:sync_send_event(?MODULE, resume, ?DEF_TIMEOUT).


%% @doc Retrieve all compaction statuses
-spec(status() ->
             term()).
status() ->
    gen_fsm:sync_send_all_state_event(?MODULE, status, ?DEF_TIMEOUT).


%% @doc Terminate child
-spec(done_child(pid(), atom()) ->
             term()).
done_child(Pid, Id) ->
    gen_fsm:send_event(?MODULE, {done_child, Pid, Id}).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
% Description: Initiates the server
init([]) ->
    {ok, state_idle, #state{}}.


%% @doc State of 'idle'
%%
-spec(state_idle({start, list(), pos_integer(), function()} | suspend | resume,
                 From::pid(), State::#state{}) ->
             {next_state, state_running | state_idle, #state{}}).
state_idle({start, TargetPids, MaxConNum, InspectFun}, From, State) ->
    AllPids    = leo_object_storage_api:get_object_storage_pid('all'),
    RemainPids = State#state.target_pids,

    ReservePids = case (length(TargetPids) == length(AllPids)) of
                      true  ->
                          [];
                      false when RemainPids == [] ->
                          lists:subtract(AllPids, TargetPids);
                      false when RemainPids /= [] ->
                          lists:subtract(RemainPids, TargetPids)
                  end,

    NewState = start_jobs_as_possible(State#state{target_pids           = TargetPids,
                                                  max_num_of_concurrent = MaxConNum,
                                                  inspect_fun           = InspectFun,
                                                  reserve_pids          = ReservePids,
                                                  start_datetime        = leo_date:now()}),
    gen_fsm:reply(From, ok),
    {next_state, state_running, NewState};


state_idle(suspend, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    {next_state, state_idle, State};

state_idle(resume, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    {next_state, state_idle, State}.


-spec(state_idle({done_child, list(), pos_integer(), atom()}, #state{}) ->
             {stop, string(), #state{}}).
state_idle({done_child, _DonePid, _Id}, State) ->
    % never happen
    {stop, "receive invalid done_child", State}.


%% @doc State of 'running'
%%
-spec(state_running({start, list(), pos_integer(), atom()} | suspend | resume,
                    From::pid(), State::#state{}) ->
             {next_state, state_running, #state{}}).
state_running({start, _, _, _}, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    {next_state, state_running, State};

state_running(suspend, From, State) ->
    gen_fsm:reply(From, ok),
    {next_state, state_suspend, State};

state_running(resume, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    {next_state, state_running, State}.


-spec(state_running({done_child, pid(), atom()}, #state{}) ->
             {next_state, state_running, #state{}}).
state_running({done_child, DonePid, DoneId}, #state{target_pids      = [Id|Rest],
                                                    in_progress_pids = InProgPids} = State) ->
    erlang:send(DonePid, {compact, Id}),
    {next_state, state_running,
     State#state{target_pids      = Rest,
                 in_progress_pids = [Id|lists:delete(DoneId, InProgPids)]}};

state_running({done_child, DonePid, DoneId}, #state{target_pids      = [],
                                                    in_progress_pids = [_,_|_],
                                                    child_pids       = ChildPids} = State) ->
    erlang:send(DonePid, stop),
    {next_state, state_running,
     State#state{in_progress_pids = lists:delete(DoneId, State#state.in_progress_pids),
                 child_pids       = orddict:erase(DonePid, ChildPids)}};

state_running({done_child,_DonePid,_DoneId}, #state{target_pids      = [],
                                                    in_progress_pids = [_|_],
                                                    child_pids       = ChildPids,
                                                    reserve_pids     = ReservePids} = State) ->
    [erlang:send(Pid, stop) || {Pid, _} <- orddict:to_list(ChildPids)],
    case ReservePids of
        [] ->
            {next_state, state_idle,
             State#state{in_progress_pids = [],
                         child_pids       = orddict:new()}};
        _ ->
            {next_state, state_idle,
             State#state{target_pids      = ReservePids,
                         in_progress_pids = [],
                         child_pids       = orddict:new(),
                         reserve_pids     = []}}
    end.


%% @doc State of 'suspend'
%%
-spec(state_suspend({start, list(), pos_integer(), function()} | suspend | resume,
                    From::pid(), State::#state{}) ->
             {next_state, state_suspend | state_running, #state{}}).
state_suspend({start, _, _, _}, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    {next_state, state_suspend, State};

state_suspend(suspend, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    {next_state, state_suspend, State};

state_suspend(resume, From, #state{target_pids      = [_Id|_Rest],
                                   in_progress_pids = InProgPids,
                                   child_pids       = ChildPids} = State) ->
    TargetPids = State#state.target_pids,

    {NewTargetPids, NewInProgPids, NewChildPids} =
        orddict:fold(
          fun(_Pid, true, Acc) ->
                  Acc;
             (Pid, false, {TargetPidsIn, InProgPidsIn, ChildPidsIn}) ->
                  case length(TargetPidsIn) of
                      0 ->
                          erlang:send(Pid, stop),
                          {[], InProgPidsIn, orddict:erase(Pid, ChildPidsIn)};
                      _ ->
                          Id = hd(TargetPidsIn),
                          erlang:send(Pid, {compact, Id}),

                          {lists:delete(Id, TargetPidsIn),
                           [Id|InProgPidsIn], orddict:store(Pid, true, ChildPidsIn)}
                  end
          end, {TargetPids, InProgPids, ChildPids}, ChildPids),

    gen_fsm:reply(From, ok),
    {next_state, state_running, State#state{target_pids      = NewTargetPids,
                                            in_progress_pids = NewInProgPids,
                                            child_pids       = NewChildPids}};

state_suspend(resume, From, #state{target_pids      = [],
                                   in_progress_pids = [_H|_Rest]} = State) ->
    gen_fsm:reply(From, ok),
    {next_state, state_running, State};

state_suspend(resume, From, #state{target_pids      = [],
                                   in_progress_pids = []} = State) ->
    %% never hapend
    gen_fsm:reply(From, ok),
    {next_state, state_idle, State}.


-spec(state_suspend({done_child, pid(), atom()}, #state{}) ->
             {next_state, state_suspend | state_idle, #state{}}).
state_suspend({done_child, DonePid, DoneId}, #state{target_pids      = [_Id|_Rest],
                                                    in_progress_pids = InProgressPids0,
                                                    child_pids       = ChildPids0} = State) ->
    InProgressPids1 = lists:delete(DoneId, InProgressPids0),
    ChildPids1      = orddict:store(DonePid, false, ChildPids0),

    {next_state, state_suspend, State#state{in_progress_pids = InProgressPids1,
                                            child_pids       = ChildPids1}};

state_suspend({done_child, DonePid, DoneId}, #state{target_pids      = [],
                                                    in_progress_pids = [_H,_H2|_Rest],
                                                    child_pids       = ChildPids0} = State) ->
    erlang:send(DonePid, stop),
    InProgressPids = lists:delete(DoneId, State#state.in_progress_pids),
    ChildPids1     = orddict:erase(DonePid, ChildPids0),

    {next_state, state_suspend, State#state{in_progress_pids = InProgressPids,
                                            child_pids       = ChildPids1}};

state_suspend({done_child,_DonePid,_DoneId}, #state{target_pids      = [],
                                                    in_progress_pids = [_H|_Rest],
                                                    child_pids       = ChildPids,
                                                    reserve_pids     = ReservePids} = State) ->
    [erlang:send(Pid, stop) || {Pid, _} <- orddict:to_list(ChildPids)],

    {next_state, state_idle, State#state{target_pids      = ReservePids,
                                         in_progress_pids = [],
                                         child_pids       = orddict:new(),
                                         reserve_pids     = []}}.

%% @doc Handle events
%%
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.


%% @doc Handle 'status' event
handle_sync_event(status, _From, StateName, #state{target_pids      = RestPids,
                                                   in_progress_pids = InProgPids,
                                                   start_datetime   = LastStart} = State) ->
    {reply, {ok, {RestPids, InProgPids, LastStart}}, StateName, State}.


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
%% @doc Start compaction processes as many as possible
%% @private
-spec(start_jobs_as_possible(#state{}) ->
             list()).
start_jobs_as_possible(State) ->
    start_jobs_as_possible(State#state{child_pids = orddict:new()}, 0).

start_jobs_as_possible(#state{target_pids           = [Id|Rest],
                              max_num_of_concurrent = MaxProc,
                              inspect_fun           = FunHasChargeOfNode,
                              in_progress_pids      = InProgPids,
                              child_pids            = ChildPids} = State, NumChild) when NumChild < MaxProc ->
    Pid  = spawn_link(fun() ->
                         loop_child(?MODULE, FunHasChargeOfNode)
                 end),
    erlang:send(Pid, {compact, Id}),

    start_jobs_as_possible(
        State#state{target_pids      = Rest,
                    in_progress_pids = [Id|InProgPids],
                    child_pids       = orddict:store(Pid, true, ChildPids)}, NumChild + 1);

start_jobs_as_possible(State, _NumChild) ->
    State.


%% @doc Loop of job executor(child)
%% @private
-spec(loop_child(pid(), fun()) ->
             ok | {error, any()}).
loop_child(From, FunHasChargeOfNode) ->
    receive
        {compact, Id} ->
            ok = leo_misc:set_env(?APP_NAME, {?ENV_COMPACTION_STATUS, Id}, ?STATE_COMPACTING),
            _  = leo_object_storage_server:compact(Id, FunHasChargeOfNode),
            ok = leo_misc:set_env(?APP_NAME, {?ENV_COMPACTION_STATUS, Id}, ?STATE_ACTIVE),
            _  = done_child(self(), Id),
            loop_child(From, FunHasChargeOfNode);
        stop ->
            ok;
        _ ->
            {error, unknown_message}
    end.

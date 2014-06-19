%%======================================================================
%%
%% Leo Compaction Manager
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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
-export([start/3, stop/1,
         suspend/0, resume/0, status/0, done_child/2]).

-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4,
         format_status/2]).

-export([idle/2,
         idle/3,
         running/2,
         running/3,
         suspend/2,
         suspend/3]).

-record(state, {max_num_of_concurrent = 1 :: non_neg_integer(),
                inspect_fun               :: function() | undefined,
                total_num_of_targets = 0  :: non_neg_integer(),
                reserved_targets = []     :: list(),
                pending_targets  = []     :: list(),
                ongoing_targets  = []     :: list(),
                child_pids       = []     :: orddict:orddict(), %% {Chid :: pid(), hasJob :: boolean()}
                start_datetime   = 0      :: non_neg_integer(), %% gregory-sec
                status = ?COMPACTION_STATUS_IDLE :: compaction_status()
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
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% API - object operations.
%%--------------------------------------------------------------------
%% @doc start compaction
-spec(start([string()|atom()], non_neg_integer(), function()) ->
             term()).
start(TargetPids, MaxConNum, InspectFun) ->
    gen_fsm:sync_send_event(
      ?MODULE, {start, TargetPids, MaxConNum, InspectFun}, ?DEF_TIMEOUT).

-spec(stop(_) ->
             term()).
stop(_Id) ->
    gen_fsm:sync_send_all_state_event(?MODULE, stop, ?DEF_TIMEOUT).


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
%% @doc Initiates the server
%%
init([]) ->
    AllTargets = leo_object_storage_api:get_object_storage_pid('all'),
    TotalNumOfTargets = erlang:length(AllTargets),

    {ok, idle, #state{status = ?COMPACTION_STATUS_IDLE,
                      total_num_of_targets = TotalNumOfTargets,
                      pending_targets      = AllTargets}}.


%% @doc State of 'idle'
%%
-spec(idle({'start', [], non_neg_integer(), function()}
           | 'suspend' | 'resume', {_,_}, #state{}) ->
             {next_state, compaction_status(), #state{}}).
idle({start, TargetPids, MaxConNum, InspectFun}, From, State) ->
    AllTargets     = leo_object_storage_api:get_object_storage_pid('all'),
    PendingTargets = State#state.pending_targets,

    ReservedTargets = case (length(TargetPids) == length(AllTargets)) of
                          true  ->
                              [];
                          false when PendingTargets == [] ->
                              lists:subtract(AllTargets, TargetPids);
                          false when PendingTargets /= [] ->
                              lists:subtract(PendingTargets, TargetPids)
                      end,

    NextState = ?COMPACTION_STATUS_RUNNING,
    NewState  = start_jobs_as_possible(
                  State#state{status = NextState,
                              pending_targets       = TargetPids,
                              reserved_targets      = ReservedTargets,
                              max_num_of_concurrent = MaxConNum,
                              inspect_fun           = InspectFun,
                              start_datetime        = leo_date:now()}),
    gen_fsm:reply(From, ok),
    {next_state, NextState, NewState};

idle(suspend, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextState = ?COMPACTION_STATUS_IDLE,
    {next_state, NextState, State#state{status = NextState}};

idle(resume, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextState = ?COMPACTION_STATUS_IDLE,
    {next_state, NextState, State#state{status = NextState}}.


-spec(idle({'done_child',_,_},_) ->
             {stop, string(), _}).
idle({done_child,_DonePid,_Id}, State) ->
    {stop, "receive invalid done_child", State}.


%% @doc State of 'running'
%%
-spec(running({'start',_,_,_} | 'suspend' | 'resume', {_,_}, #state{}) ->
             {next_state, 'suspend' | 'running', #state{}}).
running({start,_,_,_}, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextState = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextState, State#state{status = NextState}};

running(suspend, From, #state{child_pids = ChildPids} = State) ->
    [erlang:send(Pid, suspend) || {Pid, _} <- orddict:to_list(ChildPids)],
    gen_fsm:reply(From, ok),
    NextState = ?COMPACTION_STATUS_SUSPEND,
    {next_state, NextState, State#state{status = NextState}};

running(resume, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextState = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextState, State#state{status = NextState}}.


-spec(running({done_child, pid(), atom()}, #state{}) ->
             {next_state, running, #state{}}).
running({done_child, DonePid, DoneId}, #state{pending_targets = [Id|Rest],
                                              ongoing_targets = InProgPids} = State) ->
    erlang:send(DonePid, {compact, Id}),
    NextState = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextState,
     State#state{status = NextState,
                 pending_targets = Rest,
                 ongoing_targets = [Id|lists:delete(DoneId, InProgPids)]}};

running({done_child, DonePid, DoneId}, #state{pending_targets = [],
                                              ongoing_targets = [_,_|_],
                                              child_pids      = ChildPids} = State) ->
    erlang:send(DonePid, stop),
    NextState = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextState,
     State#state{status = NextState,
                 ongoing_targets = lists:delete(DoneId, State#state.ongoing_targets),
                 child_pids       = orddict:erase(DonePid, ChildPids)}};

running({done_child,_DonePid,_DoneId}, #state{pending_targets  = [],
                                              ongoing_targets  = [_|_],
                                              child_pids       = ChildPids,
                                              reserved_targets = ReservedTargets} = State) ->
    [erlang:send(Pid, stop) || {Pid, _} <- orddict:to_list(ChildPids)],
    NextState = ?COMPACTION_STATUS_IDLE,
    PendingTargets = pending_targets(ReservedTargets),
    {next_state, NextState, State#state{status = NextState,
                                        pending_targets  = PendingTargets,
                                        ongoing_targets  = [],
                                        child_pids       = [],
                                        reserved_targets = []}}.



%% @doc State of 'suspend'
%%
-spec(suspend({'start',_,_,_} | 'suspend' | 'resume', {_,_}, #state{}) ->
             {next_state, 'idle' | 'suspend' | 'running', #state{}}).
suspend({start,_,_,_}, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextState = ?COMPACTION_STATUS_SUSPEND,
    {next_state, NextState, State#state{status = NextState}};

suspend(suspend, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextState = ?COMPACTION_STATUS_SUSPEND,
    {next_state, NextState, State#state{status = NextState}};

suspend(resume, From, #state{pending_targets = [_|_],
                             ongoing_targets = InProgPids,
                             child_pids      = ChildPids} = State) ->
    TargetPids = State#state.pending_targets,

    {NewTargetPids, NewInProgPids, NewChildPids} =
        orddict:fold(
          fun(Pid, true, Acc) ->
                  erlang:send(Pid, resume),
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
    NextState = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextState, State#state{status = NextState,
                                        pending_targets = NewTargetPids,
                                        ongoing_targets = NewInProgPids,
                                        child_pids      = NewChildPids}};

suspend(resume, From, #state{pending_targets = [],
                             ongoing_targets = [_|_]} = State) ->
    gen_fsm:reply(From, ok),
    NextState = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextState, State#state{status = NextState}};

suspend(resume, From, #state{pending_targets = [],
                             ongoing_targets = []} = State) ->
    %% never hapend
    gen_fsm:reply(From, ok),
    NextState = ?COMPACTION_STATUS_IDLE,
    {next_state, NextState, State#state{status = NextState}}.


-spec(suspend({done_child, pid(), atom()}, #state{}) ->
             {next_state, suspend | idle, #state{}}).
suspend({done_child, DonePid, DoneId}, #state{pending_targets = [_|_],
                                              ongoing_targets = InProgressPids0,
                                              child_pids      = ChildPids0} = State) ->
    InProgressPids1 = lists:delete(DoneId, InProgressPids0),
    ChildPids1      = orddict:store(DonePid, false, ChildPids0),

    NextState = ?COMPACTION_STATUS_SUSPEND,
    {next_state, NextState, State#state{status = NextState,
                                        ongoing_targets = InProgressPids1,
                                        child_pids      = ChildPids1}};

suspend({done_child, DonePid, DoneId}, #state{pending_targets = [],
                                              ongoing_targets = [_,_|_],
                                              child_pids      = ChildPids0} = State) ->
    erlang:send(DonePid, stop),
    InProgressPids = lists:delete(DoneId, State#state.ongoing_targets),
    ChildPids1     = orddict:erase(DonePid, ChildPids0),

    NextState = ?COMPACTION_STATUS_SUSPEND,
    {next_state, NextState, State#state{status = NextState,
                                        ongoing_targets = InProgressPids,
                                        child_pids      = ChildPids1}};

suspend({done_child,_DonePid,_DoneId}, #state{pending_targets  = [],
                                              ongoing_targets  = [_|_],
                                              child_pids       = ChildPids,
                                              reserved_targets = ReservedTargets} = State) ->
    [erlang:send(Pid, stop) || {Pid, _} <- orddict:to_list(ChildPids)],
    NextState = ?COMPACTION_STATUS_IDLE,
    PendingTargets = pending_targets(ReservedTargets),
    {next_state, NextState, State#state{status = NextState,
                                        pending_targets  = PendingTargets,
                                        ongoing_targets  = [],
                                        child_pids       = [],
                                        reserved_targets = []}}.


%% @doc Handle events
%%
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.


%% @doc Handle 'status' event
handle_sync_event(status, _From, StateName, #state{status = Status,
                                                   total_num_of_targets = TotalNumOfTargets,
                                                   reserved_targets     = ReservedTargets,
                                                   pending_targets      = PendingTargets,
                                                   ongoing_targets      = OngoingTargets,
                                                   start_datetime       = LastestExecDate} = State) ->
    {reply, {ok, #compaction_stats{status = Status,
                                   total_num_of_targets    = TotalNumOfTargets,
                                   num_of_reserved_targets = length(ReservedTargets),
                                   num_of_pending_targets  = length(PendingTargets),
                                   num_of_ongoing_targets  = length(OngoingTargets),
                                   reserved_targets        = ReservedTargets,
                                   pending_targets         = PendingTargets,
                                   ongoing_targets         = OngoingTargets,
                                   latest_exec_datetime    = LastestExecDate}}, StateName, State};

%% @doc Handle 'stop' event
handle_sync_event(stop, _From, _StateName, Status) ->
    {stop, shutdown, ok, Status}.

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
terminate(Reason, _StateName, _State) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "terminate/2"},
                           {line, ?LINE}, {body, Reason}]),
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
             #state{}).
start_jobs_as_possible(State) ->
    start_jobs_as_possible(State#state{child_pids = []}, 0).

start_jobs_as_possible(#state{pending_targets = [Id|Rest],
                              ongoing_targets = InProgPids,
                              max_num_of_concurrent = MaxProc,
                              inspect_fun = FunHasChargeOfNode,
                              child_pids  = ChildPids} = State, NumChild) when NumChild < MaxProc ->
    Pid  = spawn_link(fun() ->
                              loop_child(FunHasChargeOfNode)
                      end),
    erlang:send(Pid, {compact, Id}),

    start_jobs_as_possible(
      State#state{pending_targets = Rest,
                  ongoing_targets = [Id|InProgPids],
                  child_pids      = orddict:store(Pid, true, ChildPids)}, NumChild + 1);

start_jobs_as_possible(State, _NumChild) ->
    State.


%% @doc Loop of job executor(child)
%% @private
-spec(loop_child(fun()) ->
             ok | {error, any()}).
loop_child(FunHasChargeOfNode) ->
    loop_child(FunHasChargeOfNode, undefined).

-spec(loop_child(fun(), atom()) ->
             ok | {error, any()}).
loop_child(FunHasChargeOfNode, TargetId) ->
    receive
        {compact, Id} ->
            ok = leo_misc:set_env(?APP_NAME, {?ENV_COMPACTION_STATUS, Id}, ?STATE_COMPACTING),
            _  = leo_object_storage_server:compact(Id, FunHasChargeOfNode),
            loop_child(FunHasChargeOfNode, Id);
        suspend ->
            _  = leo_object_storage_server:compact_suspend(TargetId),
            ok = leo_misc:set_env(?APP_NAME, {?ENV_COMPACTION_STATUS, TargetId}, ?STATE_ACTIVE),
            loop_child(FunHasChargeOfNode, TargetId);
        resume ->
            _  = leo_object_storage_server:compact_resume(TargetId),
            ok = leo_misc:set_env(?APP_NAME, {?ENV_COMPACTION_STATUS, TargetId}, ?STATE_COMPACTING),
            loop_child(FunHasChargeOfNode, TargetId);
        done ->
            ok = leo_misc:set_env(?APP_NAME, {?ENV_COMPACTION_STATUS, TargetId}, ?STATE_ACTIVE),
            _  = done_child(self(), TargetId),
            loop_child(FunHasChargeOfNode, undefined);
        stop ->
            ok;
        _ ->
            {error, unknown_message}
    end.


%% @doc Retrieve pending targets
%% @private
pending_targets([]) ->
    leo_object_storage_api:get_object_storage_pid('all');
pending_targets(ReservedTargets) ->
    ReservedTargets.

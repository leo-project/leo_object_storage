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
%%======================================================================
-module(leo_compact_fsm_controller).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-behaviour(gen_fsm).

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/0]).
-export([start/3, stop/1,
         lock/1,
         suspend/0, resume/0,
         state/0, finish/2]).

-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4,
         format_status/2]).

-export([idling/2,
         idling/3,
         running/2,
         running/3,
         suspending/2,
         suspending/3]).

-record(state, {max_num_of_concurrent = 1 :: non_neg_integer(),
                callback_fun              :: function() | undefined,
                total_num_of_targets = 0  :: non_neg_integer(),
                reserved_targets = []     :: [atom()],
                pending_targets  = []     :: [atom()],
                ongoing_targets  = []     :: [atom()],
                locked_targets   = []     :: [atom()],
                child_pids       = []     :: orddict:orddict(), %% {Chid :: pid(), hasJob :: boolean()}
                start_datetime   = 0      :: non_neg_integer(), %% gregory-sec
                status = ?ST_IDLING :: state_of_compaction()
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
start(TargetPids, MaxConNum, CallbackFun) ->
    gen_fsm:sync_send_event(
      ?MODULE, {?EVENT_RUN, TargetPids,
                MaxConNum, CallbackFun}, ?DEF_TIMEOUT).

-spec(stop(_) ->
             term()).
stop(_Id) ->
    gen_fsm:sync_send_all_state_event(?MODULE, stop, ?DEF_TIMEOUT).


%% @doc Terminate child
-spec(lock(atom()) ->
             term()).
lock(Id) ->
    gen_fsm:send_event(?MODULE, {?EVENT_LOCK, Id}).


%% @doc Suspend compaction
-spec(suspend() ->
             term()).
suspend() ->
    gen_fsm:sync_send_event(?MODULE, ?EVENT_SUSPEND, ?DEF_TIMEOUT).


%% @doc Resume compaction
-spec(resume() ->
             term()).
resume() ->
    gen_fsm:sync_send_event(?MODULE, ?EVENT_RESUME, ?DEF_TIMEOUT).


%% @doc Retrieve all compaction statuses
-spec(state() ->
             term()).
state() ->
    gen_fsm:sync_send_all_state_event(?MODULE, state, ?DEF_TIMEOUT).


%% @doc Terminate child
-spec(finish(pid(), atom()) ->
             term()).
finish(Pid, Id) ->
    gen_fsm:send_event(?MODULE, {?EVENT_FINISH, Pid, Id}).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% @doc Initiates the server
%%
init([]) ->
    AllTargets = leo_object_storage_api:get_object_storage_pid('all'),
    TotalNumOfTargets = erlang:length(AllTargets),
    {ok, ?ST_IDLING, #state{status = ?ST_IDLING,
                            total_num_of_targets = TotalNumOfTargets,
                            pending_targets      = AllTargets}}.


%% @doc State of 'idle'
%%
-spec(idling({?EVENT_RUN, [], non_neg_integer(), function()}|_, _, #state{}) ->
             {next_state, ?ST_RUNNING|?ST_IDLING, #state{}}).
idling({?EVENT_RUN, TargetPids, MaxConNum, CallbackFun}, From, State) ->
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

    NextState = ?ST_RUNNING,
    {ok, NewState} = start_jobs_as_possible(
                       State#state{status = NextState,
                                   pending_targets       = TargetPids,
                                   reserved_targets      = ReservedTargets,
                                   max_num_of_concurrent = MaxConNum,
                                   callback_fun          = CallbackFun,
                                   start_datetime        = leo_date:now()}),
    gen_fsm:reply(From, ok),
    {next_state, NextState, NewState};

idling(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextState = ?ST_IDLING,
    {next_state, NextState, State#state{status = NextState}}.


-spec(idling({?EVENT_FINISH,_,_},_) ->
             {stop, string(), _}).
idling({?EVENT_FINISH,_Pid,_Id}, State) ->
    {stop, "receive an invalid message", State}.


%% @doc State of 'running'
%%
-spec(running({?EVENT_RUN,_,_,_}|?EVENT_SUSPEND|_, _, #state{}) ->
             {next_state, ?ST_RUNNING|?ST_SUSPENDING, #state{}}).
running({?EVENT_RUN,_,_,_}, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextState = ?ST_RUNNING,
    {next_state, NextState, State#state{status = NextState}};

running(?EVENT_SUSPEND, From, #state{child_pids = ChildPids} = State) ->
    [erlang:send(Pid, suspend) || {Pid, _} <- orddict:to_list(ChildPids)],
    gen_fsm:reply(From, ok),
    NextState = ?ST_SUSPENDING,
    {next_state, NextState, State#state{status = NextState}};

running(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextState = ?ST_RUNNING,
    {next_state, NextState, State#state{status = NextState}}.


-spec(running({?EVENT_LOCK, atom()}|{?EVENT_FINISH, pid(), atom()}, #state{}) ->
             {next_state, running, #state{}}).
running({?EVENT_LOCK, Id}, #state{locked_targets = LockedTargets} = State) ->
    NextState = ?ST_RUNNING,
    {next_state, NextState,
     State#state{status = NextState,
                 locked_targets = [Id|LockedTargets]}};

running({?EVENT_FINISH, Pid, FinishedId}, #state{pending_targets = [Id|Rest],
                                                 ongoing_targets = InProgPids} = State) ->
    %% Execute data-compaction of a pending target
    erlang:send(Pid, {run, Id}),
    NextState = ?ST_RUNNING,
    {next_state, NextState,
     State#state{status = NextState,
                 pending_targets = Rest,
                 ongoing_targets = [Id|lists:delete(FinishedId, InProgPids)]}};

running({?EVENT_FINISH, Pid, FinishedId}, #state{pending_targets = [],
                                                 ongoing_targets = [_,_|_],
                                                 child_pids      = ChildPids} = State) ->
    erlang:send(Pid, stop),
    NextState = ?ST_RUNNING,
    {next_state, NextState,
     State#state{status = NextState,
                 ongoing_targets = lists:delete(FinishedId, State#state.ongoing_targets),
                 child_pids       = orddict:erase(Pid, ChildPids)}};

running({?EVENT_FINISH,_Pid,_FinishedId}, #state{pending_targets  = [],
                                                 ongoing_targets  = [_|_],
                                                 child_pids       = ChildPids,
                                                 reserved_targets = ReservedTargets} = State) ->
    [erlang:send(Pid, stop) || {Pid, _} <- orddict:to_list(ChildPids)],
    NextState = ?ST_IDLING,
    PendingTargets = pending_targets(ReservedTargets),
    {next_state, NextState, State#state{status = NextState,
                                        reserved_targets = [],
                                        pending_targets  = PendingTargets,
                                        ongoing_targets  = [],
                                        child_pids       = [],
                                        locked_targets   = []
                                       }}.


%% @doc State of 'suspend'
%%
-spec(suspending(?EVENT_RESUME|_, _, #state{}) ->
             {next_state, ?ST_SUSPENDING | ?ST_RUNNING, #state{}}).
suspending(?EVENT_RESUME, From, #state{pending_targets = [_|_],
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
                          erlang:send(Pid, {run, Id}),

                          {lists:delete(Id, TargetPidsIn),
                           [Id|InProgPidsIn], orddict:store(Pid, true, ChildPidsIn)}
                  end
          end, {TargetPids, InProgPids, ChildPids}, ChildPids),

    gen_fsm:reply(From, ok),
    NextState = ?ST_RUNNING,
    {next_state, NextState, State#state{status = NextState,
                                        pending_targets = NewTargetPids,
                                        ongoing_targets = NewInProgPids,
                                        child_pids      = NewChildPids}};

suspending(?EVENT_RESUME, From, #state{pending_targets = [],
                                       ongoing_targets = [_|_]} = State) ->
    gen_fsm:reply(From, ok),
    NextState = ?ST_RUNNING,
    {next_state, NextState, State#state{status = NextState}};

suspending(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextState = ?ST_SUSPENDING,
    {next_state, NextState, State#state{status = NextState}}.


-spec(suspending({?EVENT_FINISH, pid(), atom()}, #state{}) ->
             {next_state, ?ST_SUSPENDING|?ST_IDLING, #state{}}).
suspending({?EVENT_FINISH, Pid, FinishedId}, #state{pending_targets = [_|_],
                                                    ongoing_targets = InProgressPids0,
                                                    child_pids      = ChildPids0} = State) ->
    InProgressPids1 = lists:delete(FinishedId, InProgressPids0),
    ChildPids1      = orddict:store(Pid, false, ChildPids0),

    NextState = ?ST_SUSPENDING,
    {next_state, NextState, State#state{status = NextState,
                                        ongoing_targets = InProgressPids1,
                                        child_pids      = ChildPids1}};

suspending({?EVENT_FINISH, Pid, FinishedId}, #state{pending_targets = [],
                                                    ongoing_targets = [_,_|_],
                                                    child_pids      = ChildPids0} = State) ->
    erlang:send(Pid, stop),
    InProgressPids = lists:delete(FinishedId, State#state.ongoing_targets),
    ChildPids1     = orddict:erase(Pid, ChildPids0),

    NextState = ?ST_SUSPENDING,
    {next_state, NextState, State#state{status = NextState,
                                        ongoing_targets = InProgressPids,
                                        child_pids      = ChildPids1}};

suspending({?EVENT_FINISH,_Pid,_FinishedId}, #state{pending_targets  = [],
                                                    ongoing_targets  = [_|_],
                                                    child_pids       = ChildPids,
                                                    reserved_targets = ReservedTargets} = State) ->
    [erlang:send(Pid, stop) || {Pid, _} <- orddict:to_list(ChildPids)],
    NextState = ?ST_IDLING,
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


%% @doc Handle 'state' event
handle_sync_event(state, _From, StateName, #state{status = Status,
                                                  total_num_of_targets = TotalNumOfTargets,
                                                  reserved_targets     = ReservedTargets,
                                                  pending_targets      = PendingTargets,
                                                  ongoing_targets      = OngoingTargets,
                                                  locked_targets       = LockedTargets,
                                                  start_datetime       = LastestExecDate} = State) ->
    {reply, {ok, #compaction_stats{status = Status,
                                   total_num_of_targets    = TotalNumOfTargets,
                                   num_of_reserved_targets = length(ReservedTargets),
                                   num_of_pending_targets  = length(PendingTargets),
                                   num_of_ongoing_targets  = length(OngoingTargets),
                                   reserved_targets        = ReservedTargets,
                                   pending_targets         = PendingTargets,
                                   ongoing_targets         = OngoingTargets,
                                   locked_targets          = LockedTargets,
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
             {ok, #state{}}).
start_jobs_as_possible(State) ->
    start_jobs_as_possible(State#state{child_pids = []}, 0).

start_jobs_as_possible(#state{
                          pending_targets = [Id|Rest],
                          ongoing_targets = InProgPids,
                          max_num_of_concurrent = MaxProc,
                          callback_fun = CallbackFun,
                          child_pids   = ChildPids} = State, NumChild) when NumChild < MaxProc ->
    Pid = spawn_link(fun() ->
                             loop(CallbackFun)
                     end),
    erlang:send(Pid, {run, Id}),
    start_jobs_as_possible(
      State#state{pending_targets = Rest,
                  ongoing_targets = [Id|InProgPids],
                  child_pids      = orddict:store(Pid, true, ChildPids)}, NumChild + 1);

start_jobs_as_possible(State, _NumChild) ->
    {ok, State}.


%% @doc Loop of job executor(child)
%% @private
-spec(loop(fun()|undifined) ->
             ok | {error, any()}).
loop(CallbackFun) ->
    loop(CallbackFun, undefined).

-spec(loop(fun()|undifined, {atom(),atom()}|undefined) ->
             ok | {error, any()}).
loop(CallbackFun, TargetId) ->
    receive
        {run, Id} ->
            {ok, Id_1} = leo_object_storage_server:get_compaction_worker(Id),
            ok = leo_compact_fsm_worker:run(Id_1, self(), CallbackFun),
            loop(CallbackFun, {Id, Id_1});
        {lock, Id} ->
            ok = lock(Id),
            loop(CallbackFun, TargetId);
        suspend ->
            {_ObjStorageId, CompactionWorkerId} = TargetId,
            ok = leo_compact_fsm_worker:suspend(CompactionWorkerId),
            loop(CallbackFun, TargetId);
        resume ->
            {_ObjStorageId, CompactionWorkerId} = TargetId,
            ok = leo_compact_fsm_worker:resume(CompactionWorkerId),
            loop(CallbackFun, TargetId);
        finish ->
            {ObjStorageId,_CompactionWorkerId} = TargetId,
            _  = finish(self(), ObjStorageId),
            loop(CallbackFun, TargetId);
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

%%======================================================================
%%
%% Leo Compaction Manager
%%
%% Copyright (c) 2012-2017 Rakuten, Inc.
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
%% @doc FSM of the data-compaction controller, which manages FSM of the data-compaction's workers
%% @reference https://github.com/leo-project/leo_object_storage/blob/master/src/leo_compact_fsm_controller.erl
%% @end
%%======================================================================
-module(leo_compact_fsm_controller).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-behaviour(gen_statem).

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1]).
-export([run/0, run/1, run/2, run/3,
         diagnose/0, diagnose/1,
         recover_metadata/0, recover_metadata/1,
         stop/1,
         lock/1,
         suspend/0, resume/0,
         state/0,
         state_of_workers/0,
         finish/2, finish/3,
         increase/0,
         decrease/0
        ]).

-export([init/1,
         callback_mode/0,
         handle_event/4,
         terminate/3,
         code_change/4,
         format_status/1]).

-export([idling/3,
         running/3,
         suspending/3]).

-record(state, {
          id :: atom(),
          server_pairs = [] :: [{atom(), atom()}],
          pid_pairs = []    :: [{pid(), atom()}],
          num_of_concurrency = 1 :: non_neg_integer(),
          is_diagnosing = false  :: boolean(),
          is_recovering = false  :: boolean(),
          callback_fun             :: function() | undefined,
          total_num_of_targets = 0 :: non_neg_integer(),
          reserved_targets = []    :: [atom()],
          pending_targets  = []    :: [atom()],
          ongoing_targets  = []    :: [atom()],
          locked_targets   = []    :: [atom()],
          child_pids       = []    :: orddict:orddict(), %% {Child :: pid(), hasJob :: boolean()}
          start_datetime   = 0     :: non_neg_integer(), %% gregory-sec
          reports          = []    :: [#compaction_report{}],
          status = ?ST_IDLING :: compaction_state()
         }).

-record(event_info, {
          id :: atom(),
          event = ?EVENT_RUN    :: compaction_event(),
          client_pid            :: pid(),
          target_pids = []      :: [atom()],
          finished_id           :: atom(),
          report = #compaction_report{} :: #compaction_report{}|undefined,
          num_of_concurrency = 1         :: pos_integer(),
          is_diagnosing = false :: boolean(),
          is_recovering = false :: boolean(),
          callback :: function()
         }).

-define(DEF_TIMEOUT, 3000).

%%====================================================================
%% API
%%====================================================================
%% @doc Creates a gen_fsm process as part of a supervision tree
-spec(start_link(ServerPairL) ->
             {ok, pid()} |
             ignore |
             {error, any()} when ServerPairL::[{atom(), atom()}]).
start_link(ServerPairL) ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [ServerPairL], []).


%%--------------------------------------------------------------------
%% API - object operations.
%%--------------------------------------------------------------------
%% @doc Request launch of data-compaction to the data-compaction's workers
%% @end
-spec(run() ->
             term()).
run() ->
    run(1, undefined).

-spec(run(NumOfConcurrency) ->
             term() when NumOfConcurrency::pos_integer()).
run(NumOfConcurrency) ->
    run(NumOfConcurrency, undefined).

-spec(run(NumOfConcurrency, CallbackFun) ->
             term() when NumOfConcurrency::pos_integer(),
                         CallbackFun::function()|undefined).
run(NumOfConcurrency, CallbackFun) ->
    TargetPids = [Id || {Id,_} <-
                            leo_object_storage_api:get_object_storage_pid('all')],
    run(TargetPids, NumOfConcurrency, CallbackFun).

-spec(run(TargetPids, NumOfConcurrency, CallbackFun) ->
             term() when TargetPids::[pid()|atom()],
                         NumOfConcurrency::pos_integer(),
                         CallbackFun::function()|undefined).
run(TargetPids, NumOfConcurrency, CallbackFun) ->
    gen_statem:call(
      ?MODULE, #event_info{event = ?EVENT_RUN,
                           target_pids = TargetPids,
                           num_of_concurrency = NumOfConcurrency,
                           is_diagnosing = false,
                           callback      = CallbackFun}, ?DEF_TIMEOUT).


%% @doc Request diagnosing data-compaction to the data-compaction's workers
%% @end
-spec(diagnose() ->
             term()).
diagnose() ->
    TargetPids = [Id || {Id,_} <-
                            leo_object_storage_api:get_object_storage_pid('all')],
    gen_statem:call(
      ?MODULE, #event_info{event = ?EVENT_RUN,
                           target_pids = TargetPids,
                           num_of_concurrency = 1,
                           is_diagnosing = true,
                           is_recovering = false,
                           callback      = undefined}, ?DEF_TIMEOUT).

-spec(diagnose(TargetContainers) ->
             term() when TargetContainers::[non_neg_integer()]).
diagnose(TargetContainers) ->
    TargetPids = ?get_object_storage_id(TargetContainers),
    gen_statem:call(
      ?MODULE, #event_info{event = ?EVENT_RUN,
                           target_pids = TargetPids,
                           num_of_concurrency = 1,
                           is_diagnosing = true,
                           is_recovering = false,
                           callback      = undefined}, ?DEF_TIMEOUT).


%% @doc Request recover metadata to the data-compaction's workers
%% @end
-spec(recover_metadata() ->
             term()).
recover_metadata() ->
    TargetPids = [Id || {Id,_} <-
                            leo_object_storage_api:get_object_storage_pid('all')],
    gen_statem:call(
      ?MODULE, #event_info{event = ?EVENT_RUN,
                           target_pids = TargetPids,
                           num_of_concurrency = 1,
                           is_diagnosing = true,
                           is_recovering = true,
                           callback      = undefined}, ?DEF_TIMEOUT).

-spec(recover_metadata(TargetContainers) ->
             term() when TargetContainers::[non_neg_integer()]).
recover_metadata(TargetContainers) ->
    TargetPids = ?get_object_storage_id(TargetContainers),
    gen_statem:call(
      ?MODULE, #event_info{event = ?EVENT_RUN,
                           target_pids = TargetPids,
                           num_of_concurrency = 1,
                           is_diagnosing = true,
                           is_recovering = true,
                           callback      = undefined}, ?DEF_TIMEOUT).


%% @doc Request stop of data-compaction to the data-compaction's workers
%% @end
-spec(stop(Id) ->
             term() when Id::atom()).
stop(_Id) ->
    gen_statem:call(
      ?MODULE, stop, ?DEF_TIMEOUT).


%% @doc Request 'lock'
-spec(lock(Id) ->
             term() when Id::atom()).
lock(Id) ->
    gen_statem:cast(
      ?MODULE, #event_info{id = Id,
                           event = ?EVENT_LOCK}).


%% @doc Request 'suspend compaction' to the data-compaction's workers
-spec(suspend() ->
             term()).
suspend() ->
    gen_statem:call(
      ?MODULE, #event_info{event = ?EVENT_SUSPEND}, ?DEF_TIMEOUT).


%% @doc Request 'resume compaction' to the data-compaction's workers
-spec(resume() ->
             term()).
resume() ->
    gen_statem:call(
      ?MODULE, #event_info{event = ?EVENT_RESUME}, ?DEF_TIMEOUT).


%% @doc Retrieve the all compaction statuses from the data-compaction's workers
-spec(state() ->
             term()).
state() ->
    gen_statem:call(
      ?MODULE, state, ?DEF_TIMEOUT).


%% @doc Retrieve the all compaction statuses from the data-compaction's workers
-spec(state_of_workers() ->
             term()).
state_of_workers() ->
    gen_statem:call(
      ?MODULE, state_of_workers, ?DEF_TIMEOUT).


%% @doc Terminate a child
-spec(finish(Pid, FinishedId) ->
             term() when Pid::pid(),
                         FinishedId::atom()).
finish(Pid, FinishedId) ->
    gen_statem:cast(
      ?MODULE, #event_info{event = ?EVENT_FINISH,
                           client_pid  = Pid,
                           finished_id = FinishedId,
                           report      = undefined
                          }).

-spec(finish(Pid, FinishedId, Report) ->
             term() when Pid::pid(),
                         FinishedId::atom(),
                         Report::#compaction_report{}).
finish(Pid, FinishedId, Report) ->
    gen_statem:cast(
      ?MODULE, #event_info{event = ?EVENT_FINISH,
                           client_pid  = Pid,
                           finished_id = FinishedId,
                           report      = Report
                          }).


%% @doc Request 'incrase performance of the compaction processing' to the data-compaction's workers
-spec(increase() ->
             term()).
increase() ->
    gen_statem:call(
      ?MODULE, #event_info{event = ?EVENT_INCREASE}, ?DEF_TIMEOUT).


%% @doc Request 'decrease performance of the compaction processing' to the data-compaction's workers
-spec(decrease() ->
             term()).
decrease() ->
    gen_statem:call(
      ?MODULE, #event_info{event = ?EVENT_DECREASE}, ?DEF_TIMEOUT).


%%====================================================================
%% GEN_STATEM CALLBACKS
%%====================================================================
%% @doc Returns the callback mode
callback_mode() ->
    state_functions.

%% @doc Initiates the server
%%
-spec(init(Option) ->
             {ok, ?ST_IDLING, State} when Option::[any()],
                                          State::#state{}).
init([ServerPairL]) ->
    TargetPids = [Id || {Id,_} <-
                            leo_object_storage_api:get_object_storage_pid('all')],
    TotalNumOfTargets = erlang:length(TargetPids),
    {ok, ?ST_IDLING, #state{status = ?ST_IDLING,
                            server_pairs = ServerPairL,
                            total_num_of_targets = TotalNumOfTargets,
                            pending_targets = TargetPids}}.


%% @doc State of 'idle'
%%
-spec(idling(EventType, EventInfo, State) ->
             {next_state, ?ST_RUNNING|?ST_IDLING, State}
                 when EventType::{call, From::gen_statem:from()} | cast | info,
                      EventInfo::#event_info{}|any(),
                      State::#state{}).
idling({call, From}, #event_info{event = ?EVENT_RUN,
                                  target_pids = TargetPids,
                                  num_of_concurrency = NumOfConcurrency,
                                  is_diagnosing = IsDiagnose,
                                  is_recovering = IsRecovering,
                                  callback      = Callback}, #state{server_pairs = ServerPairs} = State) ->
    AllTargets = [Id || {Id,_} <-
                            leo_object_storage_api:get_object_storage_pid('all')],
    PendingTargets  = State#state.pending_targets,
    ReservedTargets = case (length(TargetPids) == length(AllTargets)) of
                          true  ->
                              [];
                          false when PendingTargets == [] ->
                              lists:subtract(AllTargets, TargetPids);
                          false when PendingTargets /= [] ->
                              lists:subtract(PendingTargets, TargetPids)
                      end,

    [leo_object_storage_server:unlock(ObjStorageId)
     || {_, ObjStorageId} <- ServerPairs],

    NextState = ?ST_RUNNING,
    {ok, NewState} = start_jobs_as_possible(
                       State#state{status = NextState,
                                   pending_targets    = TargetPids,
                                   reserved_targets   = ReservedTargets,
                                   num_of_concurrency = NumOfConcurrency,
                                   is_diagnosing      = IsDiagnose,
                                   is_recovering      = IsRecovering,
                                   callback_fun       = Callback,
                                   start_datetime     = leo_date:now(),
                                   pid_pairs = [],
                                   reports = []
                                  }),
    {next_state, NextState, NewState, [{reply, From, ok}]};

idling({call, From}, #event_info{}, State) ->
    NextState = ?ST_IDLING,
    {next_state, NextState, State#state{status = NextState}, [{reply, From, {error, badstate}}]};

idling({call, From}, state_of_workers, #state{server_pairs = ServerPairs} = State) ->
    Ret = [
           [{status, CStatus},
            {interval, I},
            {num_of_batch_procs, N},
            {is_locked, L}
           ]
           || {CStatus, #compaction_worker_state{
                           interval = I,
                           num_of_batch_procs = N,
                           is_locked = L
                          }}
                  <-  [sys:get_state(WorkerId)
                       || {WorkerId,_} <- ServerPairs]
          ],
    {keep_state, State, [{reply, From, {ok, Ret}}]};

idling({call, From}, state, #state{status = Status,
                                    total_num_of_targets = TotalNumOfTargets,
                                    reserved_targets     = ReservedTargets,
                                    pending_targets      = PendingTargets,
                                    ongoing_targets      = OngoingTargets,
                                    locked_targets       = LockedTargets,
                                    start_datetime       = LatestExecDate,
                                    reports              = AccReports} = State) ->
    {keep_state, State, [{reply, From, {ok, #compaction_stats{status = Status,
                                   total_num_of_targets    = TotalNumOfTargets,
                                   num_of_reserved_targets = length(ReservedTargets),
                                   num_of_pending_targets  = length(PendingTargets),
                                   num_of_ongoing_targets  = length(OngoingTargets),
                                   reserved_targets        = ReservedTargets,
                                   pending_targets         = PendingTargets,
                                   ongoing_targets         = OngoingTargets,
                                   locked_targets          = LockedTargets,
                                   latest_exec_datetime    = LatestExecDate,
                                   acc_reports             = AccReports
                                  }}}]};

idling({call, From}, stop, State) ->
    {stop_and_reply, shutdown, [{reply, From, ok}], State};

%% @doc Handle cast events in idle state
idling(cast, _, State) ->
    NextState = ?ST_IDLING,
    {next_state, NextState, State#state{status = NextState}};

%% @doc Handle info events in idle state
idling(info, {'DOWN', _Ref, _, Pid, _}, #state{pid_pairs = PidPairs} = State) ->
    case lists:keyfind(Pid, 1, PidPairs) of
        false ->
            void;
        {_, ObjStorageId} ->
            finish(Pid, ObjStorageId)
    end,
    {next_state, ?ST_IDLING, State};
idling(info, _, State) ->
    {keep_state, State}.

%% @doc State of 'running'
%%
-spec(running(EventType, EventInfo, State) ->
             {next_state, ?ST_RUNNING|?ST_SUSPENDING|?ST_IDLING, State}
                 when EventType::{call, From::gen_statem:from()} | cast | info,
                      EventInfo::#event_info{} | ?EVENT_SUSPEND | any(),
                      State::#state{}).
running({call, From}, #event_info{event = ?EVENT_SUSPEND}, #state{child_pids = ChildPids} = State) ->
    [erlang:send(Pid, suspend) || {Pid, _} <- orddict:to_list(ChildPids)],
    NextState = ?ST_SUSPENDING,
    {next_state, NextState, State#state{status = NextState}, [{reply, From, ok}]};

running({call, From}, #event_info{event = ?EVENT_INCREASE}, #state{child_pids = ChildPids} = State) ->
    [erlang:send(Pid, ?EVENT_INCREASE) || {Pid, _} <- orddict:to_list(ChildPids)],
    NextState = ?ST_RUNNING,
    {next_state, NextState, State#state{status = NextState}, [{reply, From, ok}]};

running({call, From}, #event_info{event = ?EVENT_DECREASE}, #state{child_pids = ChildPids} = State) ->
    [erlang:send(Pid, ?EVENT_DECREASE) || {Pid, _} <- orddict:to_list(ChildPids)],
    NextState = ?ST_RUNNING,
    {next_state, NextState, State#state{status = NextState}, [{reply, From, ok}]};

running({call, From}, #event_info{}, State) ->
    NextState = ?ST_RUNNING,
    {next_state, NextState, State#state{status = NextState}, [{reply, From, {error, badstate}}]};

running({call, From}, state_of_workers, #state{server_pairs = ServerPairs} = State) ->
    Ret = [
           [{status, CStatus},
            {interval, I},
            {num_of_batch_procs, N},
            {is_locked, L}
           ]
           || {CStatus, #compaction_worker_state{
                           interval = I,
                           num_of_batch_procs = N,
                           is_locked = L
                          }}
                  <-  [sys:get_state(WorkerId)
                       || {WorkerId,_} <- ServerPairs]
          ],
    {keep_state, State, [{reply, From, {ok, Ret}}]};

running({call, From}, state, #state{status = Status,
                                     total_num_of_targets = TotalNumOfTargets,
                                     reserved_targets     = ReservedTargets,
                                     pending_targets      = PendingTargets,
                                     ongoing_targets      = OngoingTargets,
                                     locked_targets       = LockedTargets,
                                     start_datetime       = LatestExecDate,
                                     reports              = AccReports} = State) ->
    {keep_state, State, [{reply, From, {ok, #compaction_stats{status = Status,
                                   total_num_of_targets    = TotalNumOfTargets,
                                   num_of_reserved_targets = length(ReservedTargets),
                                   num_of_pending_targets  = length(PendingTargets),
                                   num_of_ongoing_targets  = length(OngoingTargets),
                                   reserved_targets        = ReservedTargets,
                                   pending_targets         = PendingTargets,
                                   ongoing_targets         = OngoingTargets,
                                   locked_targets          = LockedTargets,
                                   latest_exec_datetime    = LatestExecDate,
                                   acc_reports             = AccReports
                                  }}}]};

running({call, From}, stop, State) ->
    {stop_and_reply, shutdown, [{reply, From, ok}], State};

%% @doc Handle cast events in running state
running(cast, #event_info{id = Id,
                          event = ?EVENT_LOCK}, #state{server_pairs   = ServerPairs,
                                                       locked_targets = LockedTargets} = State) ->
    %% Set locked target ids
    LockedTargets_1 = [Id|LockedTargets],
    ObjStorageId = leo_misc:get_value(Id, ServerPairs),
    ok = leo_object_storage_server:lock(ObjStorageId),
    ok = leo_object_storage_server:block_del(ObjStorageId),

    NextState = ?ST_RUNNING,
    {next_state, NextState,
     State#state{status = NextState,
                 locked_targets = LockedTargets_1}};

running(cast, #event_info{event = ?EVENT_FINISH,
                          client_pid  = Pid,
                          finished_id = FinishedId,
                          report      = Report}, #state{server_pairs    = ServerPairs,
                                                        pending_targets = [Id|Rest],
                                                        ongoing_targets = InProgPids,
                                                        locked_targets  = LockedTargets,
                                                        child_pids      = _ChildPids,
                                                        is_diagnosing   = IsDiagnose,
                                                        is_recovering   = IsRecovering,
                                                        pid_pairs       = PidPairs,
                                                        reports         = AccReports} = State) ->
    %% Execute data-compaction of a pending target
    erlang:send(Pid, {run, Id, IsDiagnose, IsRecovering}),
    %% update pid_pairs
    NewPidPairs = lists:keystore(Pid, 1, PidPairs, {Pid, Id}),
    %% Set locked target ids
    LockedTargets_1 = lists:delete(FinishedId, LockedTargets),
    ObjStorageId = leo_misc:get_value(FinishedId, ServerPairs),
    ok = leo_object_storage_server:unlock(ObjStorageId),

    NextState = ?ST_RUNNING,
    {next_state, NextState,
     State#state{status = NextState,
                 pending_targets = Rest,
                 ongoing_targets = [Id|lists:delete(FinishedId, InProgPids)],
                 locked_targets  = LockedTargets_1,
                 pid_pairs = NewPidPairs,
                 reports = [Report|AccReports]
                }};

running(cast, #event_info{event = ?EVENT_FINISH,
                          client_pid  = Pid,
                          finished_id = FinishedId,
                          report      = Report}, #state{server_pairs    = ServerPairs,
                                                        pending_targets = [],
                                                        ongoing_targets = [_,_|_],
                                                        locked_targets  = LockedTargets,
                                                        child_pids      = ChildPids,
                                                        pid_pairs       = PidPairs,
                                                        reports         = AccReports} = State) ->
    %% Send stop message to client
    erlang:send(Pid, stop),
    %% Set locked target ids
    LockedTargets_1 = lists:delete(FinishedId, LockedTargets),
    ObjStorageId = leo_misc:get_value(FinishedId, ServerPairs),
    ok = leo_object_storage_server:unlock(ObjStorageId),

    NextState = ?ST_RUNNING,
    {next_state, NextState,
     State#state{status = NextState,
                 ongoing_targets = lists:delete(FinishedId, State#state.ongoing_targets),
                 locked_targets  = LockedTargets_1,
                 child_pids      = orddict:erase(Pid, ChildPids),
                 pid_pairs = lists:keydelete(Pid, 1, PidPairs),
                 reports = [Report|AccReports]
                }};

running(cast, #event_info{event  = ?EVENT_FINISH,
                          report = Report}, #state{server_pairs     = ServerPairs,
                                                   pending_targets  = [],
                                                   ongoing_targets  = [_|_],
                                                   child_pids       = ChildPids,
                                                   reserved_targets = ReservedTargets,
                                                   reports = AccReports
                                                  } = State) ->
    AccReports_1 = lists:sort(lists:flatten([Report|AccReports])),
    [erlang:send(Pid, stop) || {Pid, _} <- orddict:to_list(ChildPids)],
    [leo_object_storage_server:unlock(ObjStorageId)
     || {_, ObjStorageId} <- ServerPairs],

    NextState = ?ST_IDLING,
    PendingTargets = pending_targets(ReservedTargets),

    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "running/3"},
                           {line, ?LINE}, {body, "FINISHED Compaction|Diagnosis|Recovery"}]),
    {next_state, NextState, State#state{status = NextState,
                                        reserved_targets = [],
                                        pending_targets  = PendingTargets,
                                        ongoing_targets  = [],
                                        child_pids       = [],
                                        pid_pairs        = [],
                                        locked_targets   = [],
                                        reports          = AccReports_1
                                       }};
running(cast, _, State) ->
    {next_state, ?ST_RUNNING, State};

%% @doc Handle info events in running state
running(info, {'DOWN', _Ref, _, Pid, _}, #state{pid_pairs = PidPairs} = State) ->
    case lists:keyfind(Pid, 1, PidPairs) of
        false ->
            void;
        {_, ObjStorageId} ->
            finish(Pid, ObjStorageId)
    end,
    {next_state, ?ST_RUNNING, State};
running(info, _, State) ->
    {keep_state, State}.


%% @doc State of 'suspend'
%%
-spec(suspending(EventType, EventInfo, State) ->
             {next_state, ?ST_SUSPENDING | ?ST_RUNNING | ?ST_IDLING, State}
                 when EventType::{call, From::gen_statem:from()} | cast | info,
                      EventInfo::#event_info{},
                      State::#state{}).
suspending({call, From}, #event_info{event = ?EVENT_RESUME}, #state{pending_targets = [_|_],
                                                                     ongoing_targets = InProgPids,
                                                                     child_pids      = ChildPids,
                                                                     is_diagnosing   = IsDiagnose,
                                                                     is_recovering   = IsRecovering} = State) ->
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
                          erlang:send(Pid, {run, Id, IsDiagnose, IsRecovering}),

                          {lists:delete(Id, TargetPidsIn),
                           [Id|InProgPidsIn], orddict:store(Pid, true, ChildPidsIn)}
                  end
          end, {TargetPids, InProgPids, ChildPids}, ChildPids),

    NextState = ?ST_RUNNING,
    {next_state, NextState, State#state{status = NextState,
                                        pending_targets = NewTargetPids,
                                        ongoing_targets = NewInProgPids,
                                        child_pids      = NewChildPids}, [{reply, From, ok}]};

suspending({call, From}, #event_info{event = ?EVENT_RESUME}, #state{pending_targets = [],
                                                                     ongoing_targets = [_|_]} = State) ->
    NextState = ?ST_RUNNING,
    {next_state, NextState, State#state{status = NextState}, [{reply, From, ok}]};

suspending({call, From}, #event_info{}, State) ->
    NextState = ?ST_SUSPENDING,
    {next_state, NextState, State#state{status = NextState}, [{reply, From, {error, badstate}}]};

suspending({call, From}, state_of_workers, #state{server_pairs = ServerPairs} = State) ->
    Ret = [
           [{status, CStatus},
            {interval, I},
            {num_of_batch_procs, N},
            {is_locked, L}
           ]
           || {CStatus, #compaction_worker_state{
                           interval = I,
                           num_of_batch_procs = N,
                           is_locked = L
                          }}
                  <-  [sys:get_state(WorkerId)
                       || {WorkerId,_} <- ServerPairs]
          ],
    {keep_state, State, [{reply, From, {ok, Ret}}]};

suspending({call, From}, state, #state{status = Status,
                                        total_num_of_targets = TotalNumOfTargets,
                                        reserved_targets     = ReservedTargets,
                                        pending_targets      = PendingTargets,
                                        ongoing_targets      = OngoingTargets,
                                        locked_targets       = LockedTargets,
                                        start_datetime       = LatestExecDate,
                                        reports              = AccReports} = State) ->
    {keep_state, State, [{reply, From, {ok, #compaction_stats{status = Status,
                                   total_num_of_targets    = TotalNumOfTargets,
                                   num_of_reserved_targets = length(ReservedTargets),
                                   num_of_pending_targets  = length(PendingTargets),
                                   num_of_ongoing_targets  = length(OngoingTargets),
                                   reserved_targets        = ReservedTargets,
                                   pending_targets         = PendingTargets,
                                   ongoing_targets         = OngoingTargets,
                                   locked_targets          = LockedTargets,
                                   latest_exec_datetime    = LatestExecDate,
                                   acc_reports             = AccReports
                                  }}}]};

suspending({call, From}, stop, State) ->
    {stop_and_reply, shutdown, [{reply, From, ok}], State};

%% @doc Handle cast events in suspending state
suspending(cast, #event_info{event = ?EVENT_FINISH,
                             client_pid = Pid,
                             finished_id = FinishedId}, #state{pending_targets = [_|_],
                                                               ongoing_targets = InProgressPids0,
                                                               child_pids      = ChildPids0} = State) ->
    InProgressPids1 = lists:delete(FinishedId, InProgressPids0),
    ChildPids1      = orddict:store(Pid, false, ChildPids0),

    NextState = ?ST_SUSPENDING,
    {next_state, NextState, State#state{status = NextState,
                                        ongoing_targets = InProgressPids1,
                                        child_pids      = ChildPids1}};

suspending(cast, #event_info{event = ?EVENT_FINISH,
                             client_pid = Pid,
                             finished_id = FinishedId}, #state{pending_targets = [],
                                                               ongoing_targets = [_,_|_],
                                                               child_pids      = ChildPids0} = State) ->
    erlang:send(Pid, stop),
    InProgressPids = lists:delete(FinishedId, State#state.ongoing_targets),
    ChildPids1     = orddict:erase(Pid, ChildPids0),

    NextState = ?ST_SUSPENDING,
    {next_state, NextState, State#state{status = NextState,
                                        ongoing_targets = InProgressPids,
                                        child_pids      = ChildPids1}};

suspending(cast, #event_info{event = ?EVENT_FINISH}, #state{pending_targets  = [],
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
                                        reserved_targets = []}};

%% @doc Handle info events in suspending state
suspending(info, {'DOWN', _Ref, _, Pid, _}, #state{pid_pairs = PidPairs} = State) ->
    case lists:keyfind(Pid, 1, PidPairs) of
        false ->
            void;
        {_, ObjStorageId} ->
            finish(Pid, ObjStorageId)
    end,
    {next_state, ?ST_SUSPENDING, State};
suspending(info, _, State) ->
    {keep_state, State}.


%% @doc Handle events that are common to all states
%%      This function handles state-independent events
handle_event({call, From}, state_of_workers, _StateName, #state{server_pairs = ServerPairs} = State) ->
    Ret = [
           [{status, CStatus},
            {interval, I},
            {num_of_batch_procs, N},
            {is_locked, L}
           ]
           || {CStatus, #compaction_worker_state{
                           interval = I,
                           num_of_batch_procs = N,
                           is_locked = L
                          }}
                  <-  [sys:get_state(WorkerId)
                       || {WorkerId,_} <- ServerPairs]
          ],
    {keep_state, State, [{reply, From, {ok, Ret}}]};

handle_event({call, From}, state, _StateName, #state{status = Status,
                                                      total_num_of_targets = TotalNumOfTargets,
                                                      reserved_targets     = ReservedTargets,
                                                      pending_targets      = PendingTargets,
                                                      ongoing_targets      = OngoingTargets,
                                                      locked_targets       = LockedTargets,
                                                      start_datetime       = LatestExecDate,
                                                      reports              = AccReports} = State) ->
    {keep_state, State, [{reply, From, {ok, #compaction_stats{status = Status,
                                   total_num_of_targets    = TotalNumOfTargets,
                                   num_of_reserved_targets = length(ReservedTargets),
                                   num_of_pending_targets  = length(PendingTargets),
                                   num_of_ongoing_targets  = length(OngoingTargets),
                                   reserved_targets        = ReservedTargets,
                                   pending_targets         = PendingTargets,
                                   ongoing_targets         = OngoingTargets,
                                   locked_targets          = LockedTargets,
                                   latest_exec_datetime    = LatestExecDate,
                                   acc_reports             = AccReports
                                  }}}]};

%% @doc Handle 'stop' event
handle_event({call, From}, stop, _StateName, State) ->
    {stop_and_reply, shutdown, [{reply, From, ok}], State};

%% @doc Handling all non call/cast messages
handle_event(info, {'DOWN',_Ref,_, Pid,_}, StateName, #state{pid_pairs = PidPairs} = State) ->
    case lists:keyfind(Pid, 1, PidPairs) of
        false ->
            void;
        {_,ObjStorageId} ->
            finish(Pid, ObjStorageId)
    end,
    {next_state, StateName, State};
handle_event(info, _Info, _StateName, State) ->
    {keep_state, State}.

%% @doc This function is called by a gen_server when it is about to
%%      terminate. It should be the opposite of Module:init/1 and do any necessary
%%      cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(Reason, _StateName, _State) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "terminate/2"},
                           {line, ?LINE}, {body, Reason}]),
    ok.

%% @doc Convert process state when code is changed
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% @doc This function is called by a gen_statem when it should update
%%      its internal state data during a release upgrade/downgrade
format_status(Status) ->
    Status.


%%====================================================================
%% INNER FUNCTIONS
%%====================================================================
%% @doc Start compaction processes as many as possible
%% @private
-spec(start_jobs_as_possible(#state{}) ->
             {ok, #state{}}).
start_jobs_as_possible(State) ->
    start_jobs_as_possible(State#state{child_pids = []}, 0).

start_jobs_as_possible(#state{pid_pairs = PidPairs,
                              pending_targets = [Id|Rest],
                              ongoing_targets = InProgPids,
                              num_of_concurrency = NumOfConcurrency,
                              callback_fun  = CallbackFun,
                              is_diagnosing = IsDiagnose,
                              is_recovering = IsRecovering,
                              child_pids    = ChildPids} = State, NumChild) when NumChild < NumOfConcurrency ->
    {Pid, _Ref} = spawn_monitor(fun() ->
                        loop(CallbackFun)
                end),
    erlang:send(Pid, {run, Id, IsDiagnose, IsRecovering}),
    start_jobs_as_possible(
      State#state{pending_targets = Rest,
                  ongoing_targets = [Id|InProgPids],
                  pid_pairs  = [{Pid, Id}|PidPairs],
                  child_pids = orddict:store(Pid, true, ChildPids)}, NumChild + 1);

start_jobs_as_possible(State, _NumChild) ->
    {ok, State}.


%% @doc Loop of job executor(child)
%% @private
-spec(loop(fun()|undifined) ->
             ok | {error, any()}).
loop(CallbackFun) ->
    loop(CallbackFun, undefined).

-spec(loop(CallbackFun, Params) ->
             ok | {error, any()}
                 when CallbackFun::fun()|undifined,
                      Params::{atom(),atom(),atom(),atom()}|undefined).
loop(CallbackFun, Params) ->
    receive
        {run, Id, IsDiagnose, IsRecovering} ->
            {ok, Id_1} = leo_object_storage_server:get_compaction_worker(Id),
            case leo_compact_fsm_worker:run(
                   Id_1, self(), IsDiagnose, IsRecovering, CallbackFun) of
                ok ->
                    ok = leo_object_storage_server:block_del(Id),
                    loop(CallbackFun, {Id, Id_1, IsDiagnose, IsRecovering});
                {error,_Cause} ->
                    ok = finish(self(), Id)
            end;
        run ->
            loop(CallbackFun, Params);
        {lock, Id} ->
            ok = lock(Id),
            loop(CallbackFun, Params);
        suspend = Event ->
            operate(Event, Params),
            loop(CallbackFun, Params);
        resume = Event ->
            operate(Event, Params),
            loop(CallbackFun, Params);
        {finish, {ObjStorageId, Report}} ->
            ok = finish(self(), ObjStorageId, Report),
            loop(CallbackFun, Params);
        increase = Event ->
            operate(Event, Params),
            loop(CallbackFun, Params);
        decrease = Event ->
            operate(Event, Params),
            loop(CallbackFun, Params);
        stop ->
            ok;
        _ ->
            loop(CallbackFun, Params)
    after
        ?DEF_COMPACTION_TIMEOUT ->
            {_, WorkerId, IsDiagnose, IsRecovering} = Params,
            leo_compact_fsm_worker:forced_run(
              WorkerId, IsDiagnose, IsRecovering),
            loop(CallbackFun, Params)
    end.


%% @private
operate(?EVENT_SUSPEND, {_,WorkerId,_,_}) ->
    leo_compact_fsm_worker:suspend(WorkerId);
operate(?EVENT_RESUME, {_,WorkerId,_,_}) ->
    resume(WorkerId, ?MAX_RETRY_TIMES);
operate(?EVENT_INCREASE, {_,WorkerId,_,_}) ->
    leo_compact_fsm_worker:increase(WorkerId);
operate(?EVENT_DECREASE, {_,WorkerId,_,_}) ->
    leo_compact_fsm_worker:decrease(WorkerId);
operate(_,_) ->
    ok.


%% @private
resume(_,0) ->
    error_logger:error_msg("~p,~p,~p,~p~n",
                           [{module, ?MODULE_STRING},
                            {function, "resume/2"},
                            {line, ?LINE},
                            {body, resume_operation_failure}]),
    {error, resume_operation_failure};
resume(WorkerId, RetryTimes) ->
    try leo_compact_fsm_worker:resume(WorkerId) of
        ok ->
            ok;
        {error, Reason} ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "resume/2"},
                                      {line, ?LINE},
                                      {body, {resume_failed, WorkerId, Reason, RetryTimes}}]),
            timer:sleep(100),
            resume(WorkerId, RetryTimes - 1)
    catch
        _:CatchReason ->
            error_logger:warning_msg("~p,~p,~p,~p~n",
                                     [{module, ?MODULE_STRING},
                                      {function, "resume/2"},
                                      {line, ?LINE},
                                      {body, {resume_exception, WorkerId, CatchReason, RetryTimes}}]),
            timer:sleep(100),
            resume(WorkerId, RetryTimes - 1)
    end.


%% @doc Retrieve pending targets
%% @private
pending_targets([]) ->
    [Id || {Id,_} <-
               leo_object_storage_api:get_object_storage_pid('all')];
pending_targets(ReservedTargets) ->
    ReservedTargets.

%%======================================================================
%%
%% Leo Object Storage
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
%% @doc FSM of the data-compaction worker, which handles removing unnecessary objects from a object container.
%% @reference https://github.com/leo-project/leo_object_storage/blob/master/src/leo_compact_fsm_controller.erl
%% @end
%%======================================================================
-module(leo_compact_fsm_worker).

-author('Yosuke Hara').

-behaviour(gen_fsm).

-include("leo_object_storage.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/4, stop/1]).
-export([run/3, run/5,
         suspend/1,
         resume/1,
         state/2,
         incr_interval/1, decr_interval/1,
         incr_batch_of_msgs/1, decr_batch_of_msgs/1
        ]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4,
         format_status/2]).

-export([idling/2, idling/3,
         running/2, running/3,
         suspending/2, suspending/3]).

-compile(nowarn_deprecated_type).
-define(DEF_TIMEOUT, timer:seconds(30)).

-record(event_info, {
          id :: atom(),
          event = ?EVENT_RUN :: compaction_event(),
          controller_pid :: pid(),
          client_pid     :: pid(),
          is_diagnosing = false :: boolean(),
          is_recovering = false :: boolean(),
          callback :: function()
         }).

-record(compaction_prms, {
          key_bin  = <<>> :: binary(),
          body_bin = <<>> :: binary(),
          metadata = #?METADATA{} :: #?METADATA{},
          next_offset = 0         :: non_neg_integer()|eof,
          start_lock_offset = 0   :: non_neg_integer(),
          callback_fun            :: function(),
          num_of_active_objs = 0  :: non_neg_integer(),
          size_of_active_objs = 0 :: non_neg_integer(),
          total_num_of_objs = 0   :: non_neg_integer(),
          total_size_of_objs = 0  :: non_neg_integer()
         }).

-record(state, {
          id :: atom(),
          obj_storage_id :: atom(),
          meta_db_id     :: atom(),
          obj_storage_info = #backend_info{} :: #backend_info{},
          compact_cntl_pid       :: pid(),
          diagnosis_log_id       :: atom(),
          status = ?ST_IDLING    :: compaction_state(),
          is_locked = false      :: boolean(),
          is_diagnosing = false  :: boolean(),
          is_recovering = false  :: boolean(),
          %% interval_between_batch_procs:
          interval_between_batch_procs = 0       :: non_neg_integer(),
          max_interval_between_batch_procs = 0   :: non_neg_integer(),
          min_interval_between_batch_procs = 0   :: non_neg_integer(),
          step_interval_between_batch_procs = 0  :: non_neg_integer(),
          %% batch-procs:
          count_procs = 0        :: non_neg_integer(),
          num_of_batch_procs = 0        :: non_neg_integer(),
          max_num_of_batch_procs = 0    :: non_neg_integer(),
          min_num_of_batch_procs = 0    :: non_neg_integer(),
          step_num_of_batch_procs = 0   :: non_neg_integer(),
          %% compaction-info:
          compaction_prms = #compaction_prms{} :: #compaction_prms{},
          start_datetime = 0 :: non_neg_integer(),
          error_pos = 0      :: non_neg_integer(),
          set_errors         :: set(),
          acc_errors = []    :: [{pos_integer(), pos_integer()}],
          result :: compaction_ret()
         }).


%%====================================================================
%% API
%%====================================================================
%% @doc Creates a gen_fsm process as part of a supervision tree
-spec(start_link(Id, ObjStorageId, MetaDBId, LoggerId) ->
             {ok, pid()} | {error, any()} when Id::atom(),
                                               ObjStorageId::atom(),
                                               MetaDBId::atom(),
                                               LoggerId::atom()).
start_link(Id, ObjStorageId, MetaDBId, LoggerId) ->
    gen_fsm:start_link({local, Id}, ?MODULE,
                       [Id, ObjStorageId, MetaDBId, LoggerId], []).


%% @doc Stop this server
%%
-spec(stop(Id) ->
             ok when Id::atom()).
stop(Id) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "stop/1"},
                           {line, ?LINE}, {body, Id}]),
    gen_fsm:sync_send_all_state_event(Id, stop, ?DEF_TIMEOUT).


%% @doc Run the process
%%
-spec(run(Id, IsDiagnosing, IsRecovering) ->
             ok | {error, any()} when Id::atom(),
                                      IsDiagnosing::boolean(),
                                      IsRecovering::boolean()).
run(Id, IsDiagnosing, IsRecovering) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_RUN,
                                       is_diagnosing = IsDiagnosing,
                                       is_recovering = IsRecovering
                                      }).

-spec(run(Id, ControllerPid, IsDiagnosing, IsRecovering, CallbackFun) ->
             ok | {error, any()} when Id::atom(),
                                      ControllerPid::pid(),
                                      IsDiagnosing::boolean(),
                                      IsRecovering::boolean(),
                                      CallbackFun::function()).
run(Id, ControllerPid, IsDiagnosing, IsRecovering, CallbackFun) ->
    gen_fsm:sync_send_event(Id, #event_info{event = ?EVENT_RUN,
                                            controller_pid = ControllerPid,
                                            is_diagnosing  = IsDiagnosing,
                                            is_recovering  = IsRecovering,
                                            callback = CallbackFun}, ?DEF_TIMEOUT).


%% @doc Retrieve an object from the object-storage
%%
-spec(suspend(Id) ->
             ok | {error, any()} when Id::atom()).
suspend(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_SUSPEND}).


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(resume(Id) ->
             ok | {error, any()} when Id::atom()).
resume(Id) ->
    gen_fsm:sync_send_event(Id, #event_info{event = ?EVENT_RESUME}, ?DEF_TIMEOUT).


%% @doc Retrieve the storage stats specfied by Id
%%      which contains number of objects and so on.
%%
-spec(state(Id, Client) ->
             ok | {error, any()} when Id::atom(),
                                      Client::pid()).
state(Id, Client) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_STATE,
                                       client_pid = Client}).


%% @doc Increase waiting time of data-compaction
%%
-spec(incr_interval(Id) ->
             ok when Id::atom()).
incr_interval(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_INCR_WT}).


%% @doc Decrease waiting time of data-compaction
%%
-spec(decr_interval(Id) ->
             ok when Id::atom()).
decr_interval(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_DECR_WT}).


%% @doc Increase number of batch procs
%%
-spec(incr_batch_of_msgs(Id) ->
             ok when Id::atom()).
incr_batch_of_msgs(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_INCR_BP}).


%% @doc Decrease number of batch procs
%%
-spec(decr_batch_of_msgs(Id) ->
             ok when Id::atom()).
decr_batch_of_msgs(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_DECR_BP}).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% @doc Initiates the server
%%
init([Id, ObjStorageId, MetaDBId, LoggerId]) ->
    {ok, ?ST_IDLING, #state{id = Id,
                            obj_storage_id     = ObjStorageId,
                            meta_db_id         = MetaDBId,
                            diagnosis_log_id   = LoggerId,
                            interval_between_batch_procs       = ?env_compaction_interval_between_batch_procs_reg(),
                            min_interval_between_batch_procs   = ?env_compaction_interval_between_batch_procs_min(),
                            max_interval_between_batch_procs   = ?env_compaction_interval_between_batch_procs_max(),
                            step_interval_between_batch_procs  = ?env_compaction_interval_between_batch_procs_step(),
                            num_of_batch_procs        = ?env_compaction_num_of_batch_procs_reg(),
                            max_num_of_batch_procs    = ?env_compaction_num_of_batch_procs_max(),
                            min_num_of_batch_procs    = ?env_compaction_num_of_batch_procs_min(),
                            step_num_of_batch_procs   = ?env_compaction_num_of_batch_procs_step(),
                            compaction_prms = #compaction_prms{
                                                 key_bin  = <<>>,
                                                 body_bin = <<>>,
                                                 metadata = #?METADATA{},
                                                 next_offset = 0,
                                                 num_of_active_objs  = 0,
                                                 size_of_active_objs = 0}
                           }}.

%% @doc Handle events
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc Handle 'status' event
handle_sync_event(state, _From, StateName, State) ->
    {reply, {ok, StateName}, StateName, State};

%% @doc Handle 'stop' event
handle_sync_event(stop, _From, _StateName, Status) ->
    {stop, shutdown, ok, Status}.


%% @doc Handling all non call/cast messages
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.


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

%% @doc This function is called by a gen_fsm when it should update
%%      its internal state data during a release upgrade/downgrade
format_status(_Opt, [_PDict, State]) ->
    State.


%%====================================================================
%% CALLBACKS
%%====================================================================
%% @doc State of 'idle'
%%
-spec(idling(EventInfo, From, State) ->
             {next_state, ?ST_IDLING | ?ST_RUNNING, State} when EventInfo::#event_info{},
                                                                From::{pid(),Tag::atom()},
                                                                State::#state{}).
idling(#event_info{event = ?EVENT_RUN,
                   controller_pid = ControllerPid,
                   is_diagnosing  = IsDiagnosing,
                   is_recovering  = IsRecovering,
                   callback = CallbackFun}, From, #state{id = Id,
                                                         compaction_prms =
                                                             CompactionPrms} = State) ->
    NextStatus = ?ST_RUNNING,
    State_1 = State#state{compact_cntl_pid = ControllerPid,
                          status        = NextStatus,
                          is_diagnosing = IsDiagnosing,
                          is_recovering = IsRecovering,
                          error_pos     = 0,
                          set_errors    = sets:new(),
                          acc_errors    = [],
                          min_interval_between_batch_procs   = ?env_compaction_interval_between_batch_procs_min(),
                          max_interval_between_batch_procs   = ?env_compaction_interval_between_batch_procs_max(),
                          step_interval_between_batch_procs  = ?env_compaction_interval_between_batch_procs_step(),
                          min_num_of_batch_procs             = ?env_compaction_num_of_batch_procs_min(),
                          max_num_of_batch_procs             = ?env_compaction_num_of_batch_procs_max(),
                          step_num_of_batch_procs            = ?env_compaction_num_of_batch_procs_step(),
                          compaction_prms =
                              CompactionPrms#compaction_prms{
                                num_of_active_objs  = 0,
                                size_of_active_objs = 0,
                                next_offset = ?AVS_SUPER_BLOCK_LEN,
                                callback_fun = CallbackFun
                               },
                          start_datetime = leo_date:now()
                         },
    case prepare(State_1) of
        {ok, State_2} ->
            gen_fsm:reply(From, ok),
            ok = run(Id, IsDiagnosing, IsRecovering),
            {next_state, NextStatus, State_2};
        {{error, Cause}, #state{obj_storage_info =
                                    #backend_info{write_handler = WriteHandler,
                                                  read_handler  = ReadHandler}}} ->
            catch leo_object_storage_haystack:close(WriteHandler, ReadHandler),
            gen_fsm:reply(From, {error, Cause}),
            {next_state, ?ST_IDLING, State_1}
    end;
idling(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus}}.

-spec(idling(EventInfo, State) ->
             {next_state, ?ST_IDLING, State} when EventInfo::#event_info{},
                                                  State::#state{}).
idling(#event_info{event = ?EVENT_STATE,
                   client_pid = Client}, State) ->
    NextStatus = ?ST_IDLING,
    erlang:send(Client, NextStatus),
    {next_state, NextStatus, State#state{status = NextStatus}};


idling(#event_info{event = ?EVENT_INCR_WT}, #state{interval_between_batch_procs = WaitingTime,
                                                   max_interval_between_batch_procs  = MaxWaitingTime,
                                                   step_interval_between_batch_procs = StepWaitingTime} = State) ->
    NextStatus = ?ST_IDLING,
    WaitingTime_1 = incr_interval_fun(WaitingTime, MaxWaitingTime, StepWaitingTime),
    {next_state, NextStatus, State#state{interval_between_batch_procs = WaitingTime_1,
                                         status = NextStatus}};

idling(#event_info{event = ?EVENT_DECR_WT}, #state{interval_between_batch_procs = WaitingTime,
                                                   min_interval_between_batch_procs  = MinWaitingTime,
                                                   step_interval_between_batch_procs = StepWaitingTime} = State) ->
    NextStatus = ?ST_IDLING,
    WaitingTime_1 = decr_interval_fun(WaitingTime, MinWaitingTime, StepWaitingTime),
    {next_state, NextStatus, State#state{interval_between_batch_procs = WaitingTime_1,
                                         status = NextStatus}};

idling(#event_info{event = ?EVENT_INCR_BP}, #state{num_of_batch_procs      = BatchProcs,
                                                   max_num_of_batch_procs  = MaxBatchProcs,
                                                   step_num_of_batch_procs = StepBatchProcs} = State) ->
    NextStatus = ?ST_IDLING,
    BatchProcs_1 = incr_batch_of_msgs_fun(BatchProcs, MaxBatchProcs, StepBatchProcs),
    {next_state, NextStatus, State#state{num_of_batch_procs = BatchProcs_1,
                                         status = NextStatus}};

idling(#event_info{event = ?EVENT_DECR_BP}, #state{num_of_batch_procs      = BatchProcs,
                                                   min_num_of_batch_procs  = MinBatchProcs,
                                                   step_num_of_batch_procs = StepBatchProcs} = State) ->
    NextStatus = ?ST_IDLING,
    BatchProcs_1 = decr_batch_of_msgs_fun(BatchProcs, MinBatchProcs, StepBatchProcs),
    {next_state, NextStatus, State#state{num_of_batch_procs = BatchProcs_1,
                                         status = NextStatus}};
idling(_, State) ->
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


%% @doc State of 'running'
-spec(running(EventInfo, State) ->
             {next_state, ?ST_RUNNING, State} when EventInfo::#event_info{},
                                                   State::#state{}).
running(#event_info{event = ?EVENT_RUN},
        #state{obj_storage_id = #backend_info{linked_path = []}} = State) ->
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus}};
running(#event_info{event = ?EVENT_RUN},
        #state{obj_storage_id = #backend_info{file_path = []}} = State) ->
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus}};
running(#event_info{event = ?EVENT_RUN,
                    is_diagnosing = IsDiagnosing,
                    is_recovering = IsRecovering},
        #state{id = Id,
               obj_storage_id   = ObjStorageId,
               compact_cntl_pid = CompactCntlPid,
               interval_between_batch_procs  = WaitingTime,
               count_procs = CountProcs,
               num_of_batch_procs = BatchProcs,
               compaction_prms =
                   #compaction_prms{
                      start_lock_offset = StartLockOffset}} = State) ->
    %% Temporally suspend the compaction
    %% in order to decrease i/o load
    CountProcs_1 = case (CountProcs < 1) of
                       true ->
                           WaitingTime_1 = case (WaitingTime < 1) of
                                               true  -> ?DEF_MAX_COMPACTION_WT;
                                               false -> WaitingTime
                                           end,
                           timer:sleep(WaitingTime_1),
                           BatchProcs;
                       false ->
                           CountProcs - 1
                   end,

    {NextStatus, State_3} =
        case catch execute(State#state{is_diagnosing = IsDiagnosing,
                                       is_recovering = IsRecovering}) of
            %% Lock the object-storage in order to
            %%     reject requests during the data-compaction
            {ok, {next, #state{is_locked = IsLocked,
                               compaction_prms =
                                   #compaction_prms{next_offset = NextOffset}
                              } = State_1}} when NextOffset > StartLockOffset ->
                State_2 = case IsLocked of
                              true ->
                                  State_1;
                              false ->
                                  ok = leo_object_storage_server:lock(ObjStorageId),
                                  erlang:send(CompactCntlPid, {lock, Id}),
                                  State_1#state{is_locked = true}
                          end,
                ok = run(Id, IsDiagnosing, IsRecovering),
                {?ST_RUNNING, State_2};
            %% Execute the data-compaction repeatedly
            {ok, {next, State_1}} ->
                ok = run(Id, IsDiagnosing, IsRecovering),
                {?ST_RUNNING, State_1};

            %% An unxepected error has occured
            {'EXIT', Cause} ->
                error_logger:info_msg("~p,~p,~p,~p~n",
                                      [{module, ?MODULE_STRING}, {function, "running/2"},
                                       {line, ?LINE}, {body, {Id, Cause}}]),
                {ok, State_1} = after_execute({error, Cause},
                                              State#state{result = ?RET_FAIL}),
                {?ST_IDLING, State_1};
            %% Reached end of the object-container
            {ok, {eof, State_1}} ->
                {ok, State_2} = after_execute(ok,
                                              State_1#state{result = ?RET_SUCCESS}),
                {?ST_IDLING, State_2};
            %% An epected error has occured
            {{error, Cause}, State_1} ->
                error_logger:info_msg("~p,~p,~p,~p~n",
                                      [{module, ?MODULE_STRING}, {function, "running/2"},
                                       {line, ?LINE}, {body, {Id, Cause}}]),
                {ok, State_2} = after_execute({error, Cause},
                                              State_1#state{result = ?RET_FAIL}),
                {?ST_IDLING, State_2}
        end,
    {next_state, NextStatus, State_3#state{status = NextStatus,
                                           count_procs = CountProcs_1}};

running(#event_info{event = ?EVENT_SUSPEND}, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}};

running(#event_info{event = ?EVENT_STATE,
                    client_pid = Client}, #state{id = Id,
                                                 is_diagnosing = IsDiagnosing,
                                                 is_recovering = IsRecovering} = State) ->
    ok = run(Id, IsDiagnosing, IsRecovering),
    NextStatus = ?ST_RUNNING,
    erlang:send(Client, NextStatus),
    {next_state, NextStatus, State#state{status = NextStatus}};

running(#event_info{event = ?EVENT_INCR_WT}, #state{id = Id,
                                                    is_diagnosing = IsDiagnosing,
                                                    is_recovering = IsRecovering,
                                                    interval_between_batch_procs = WaitingTime,
                                                    max_interval_between_batch_procs  = MaxWaitingTime,
                                                    step_interval_between_batch_procs = StepWaitingTime,
                                                    num_of_batch_procs = BatchProcs,
                                                    min_num_of_batch_procs = MinBatchProcs} = State) ->
    {NextStatus, WaitingTime_1} =
        case (WaitingTime >= MaxWaitingTime andalso
              BatchProcs  =< MinBatchProcs) of
            true ->
                {?ST_SUSPENDING, MaxWaitingTime};
            false ->
                ok = run(Id, IsDiagnosing, IsRecovering),
                {?ST_RUNNING,
                 incr_interval_fun(WaitingTime, MaxWaitingTime, StepWaitingTime)}
        end,
    {next_state, NextStatus, State#state{interval_between_batch_procs = WaitingTime_1,
                                         status = NextStatus}};

running(#event_info{event = ?EVENT_DECR_WT}, #state{id = Id,
                                                    is_diagnosing = IsDiagnosing,
                                                    is_recovering = IsRecovering,
                                                    interval_between_batch_procs = WaitingTime,
                                                    min_interval_between_batch_procs  = MinWaitingTime,
                                                    step_interval_between_batch_procs = StepWaitingTime} = State) ->
    WaitingTime_1 = decr_interval_fun(
                      WaitingTime, MinWaitingTime, StepWaitingTime),
    ok = run(Id, IsDiagnosing, IsRecovering),
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{interval_between_batch_procs = WaitingTime_1,
                                         status = NextStatus}};

running(#event_info{event = ?EVENT_INCR_BP}, #state{id = Id,
                                                    is_diagnosing = IsDiagnosing,
                                                    is_recovering = IsRecovering,
                                                    num_of_batch_procs = BatchProcs,
                                                    max_num_of_batch_procs  = MaxBatchProcs,
                                                    step_num_of_batch_procs = StepBatchProcs} = State) ->
    BatchProcs_1 = incr_batch_of_msgs_fun(BatchProcs, MaxBatchProcs, StepBatchProcs),
    ok = run(Id, IsDiagnosing, IsRecovering),
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{num_of_batch_procs = BatchProcs_1,
                                         status = NextStatus}};

running(#event_info{event = ?EVENT_DECR_BP}, #state{id = Id,
                                                    is_diagnosing = IsDiagnosing,
                                                    is_recovering = IsRecovering,
                                                    num_of_batch_procs = BatchProcs,
                                                    min_num_of_batch_procs  = MinBatchProcs,
                                                    step_num_of_batch_procs = StepBatchProcs,
                                                    interval_between_batch_procs = WaitingTime,
                                                    max_interval_between_batch_procs = MaxWaitingTime} = State) ->
    {NextStatus, BatchProcs_1} =
        case (WaitingTime >= MaxWaitingTime andalso
              BatchProcs  =< MinBatchProcs) of
            true ->
                {?ST_SUSPENDING, MinBatchProcs};
            false ->
                ok = run(Id, IsDiagnosing, IsRecovering),
                {?ST_RUNNING,
                 decr_batch_of_msgs_fun(BatchProcs, MinBatchProcs, StepBatchProcs)}
        end,
    {next_state, NextStatus, State#state{num_of_batch_procs = BatchProcs_1,
                                         status = NextStatus}};

running(_, #state{id = Id,
                  is_diagnosing = IsDiagnosing,
                  is_recovering = IsRecovering} = State) ->
    ok = run(Id, IsDiagnosing, IsRecovering),
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


-spec(running( _, _, #state{}) ->
             {next_state, ?ST_SUSPENDING|?ST_RUNNING, #state{}}).
running(_, From, #state{id = Id,
                        is_diagnosing = IsDiagnosing,
                        is_recovering = IsRecovering} = State) ->
    gen_fsm:reply(From, {error, badstate}),
    ok = run(Id, IsDiagnosing, IsRecovering),
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


%% @doc State of 'suspend'
%%
-spec(suspending(EventInfo, State) ->
             {next_state, ?ST_SUSPENDING, State} when EventInfo::#event_info{},
                                                      State::#state{}).
suspending(#event_info{event = ?EVENT_RUN}, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}};

suspending(#event_info{event = ?EVENT_STATE,
                       client_pid = Client}, State) ->
    NextStatus = ?ST_SUSPENDING,
    erlang:send(Client, NextStatus),
    {next_state, NextStatus, State#state{status = NextStatus}};

suspending(#event_info{event = ?EVENT_DECR_WT}, #state{id = Id,
                                                       is_diagnosing = IsDiagnosing,
                                                       is_recovering = IsRecovering,
                                                       interval_between_batch_procs = WaitingTime,
                                                       min_interval_between_batch_procs  = MinWaitingTime,
                                                       step_interval_between_batch_procs = StepWaitingTime} = State) ->
    WaitingTime_1 = decr_interval_fun(WaitingTime, MinWaitingTime, StepWaitingTime),

    %% resume the data-compaction
    NextStatus = ?ST_RUNNING,
    timer:apply_after(timer:seconds(1), ?MODULE, run, [Id, IsDiagnosing, IsRecovering]),
    {next_state, NextStatus, State#state{interval_between_batch_procs = WaitingTime_1,
                                         status = NextStatus}};

suspending(#event_info{event = ?EVENT_INCR_BP}, #state{id = Id,
                                                       is_diagnosing = IsDiagnosing,
                                                       is_recovering = IsRecovering,
                                                       num_of_batch_procs = BatchProcs,
                                                       max_num_of_batch_procs  = MaxBatchProcs,
                                                       step_num_of_batch_procs = StepBatchProcs} = State) ->
    BatchProcs_1 = incr_batch_of_msgs_fun(BatchProcs, MaxBatchProcs, StepBatchProcs),

    %% resume the data-compaction
    NextStatus = ?ST_RUNNING,
    timer:apply_after(timer:seconds(1), ?MODULE, run, [Id, IsDiagnosing, IsRecovering]),
    {next_state, NextStatus, State#state{num_of_batch_procs = BatchProcs_1,
                                         status = NextStatus}};

suspending(_, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


-spec(suspending(EventInfo, From, State) ->
             {next_state, ?ST_SUSPENDING | ?ST_RUNNING, State} when EventInfo::#event_info{},
                                                                    From::{pid(),Tag::atom()},
                                                                    State::#state{}).
suspending(#event_info{event = ?EVENT_RESUME}, From, #state{id = Id,
                                                            is_diagnosing = IsDiagnosing,
                                                            is_recovering = IsRecovering} = State) ->
    %% resume the data-compaction
    gen_fsm:reply(From, ok),

    NextStatus = ?ST_RUNNING,
    timer:apply_after(
      timer:seconds(1), ?MODULE, run, [Id, IsDiagnosing, IsRecovering]),
    {next_state, NextStatus, State#state{status = NextStatus}};

suspending(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


%%--------------------------------------------------------------------
%% Inner Functions
%%--------------------------------------------------------------------
%% @doc Calculate remain disk-sizes.
%% @private
-spec(calc_remain_disksize(MetaDBId, FilePath) ->
             {ok, integer()} | {error, any()} when MetaDBId::atom(),
                                                   FilePath::string()).
calc_remain_disksize(MetaDBId, FilePath) ->
    case leo_file:file_get_mount_path(FilePath) of
        {ok, MountPath} ->
            case leo_backend_db_api:get_db_raw_filepath(MetaDBId) of
                {ok, MetaDir} ->
                    case catch leo_file:file_get_total_size(MetaDir) of
                        {'EXIT', Reason} ->
                            {error, Reason};
                        MetaSize ->
                            AvsSize = filelib:file_size(FilePath),
                            Remain  = leo_file:file_get_remain_disk(MountPath),
                            {ok, erlang:round(Remain - (AvsSize + MetaSize))}
                    end;
                _ ->
                    {error, ?ERROR_COULD_NOT_GET_MOUNT_PATH}
            end;
        Error ->
            Error
    end.

%% @doc compact objects from the object-container on a independent process.
%% @private
-spec(prepare(State) ->
             {ok, State} | {{error, any()}, State} when State::#state{}).
prepare(#state{obj_storage_id = ObjStorageId,
               meta_db_id  = MetaDBId} = State) ->
    %% Retrieve the current container's path
    {ok, #backend_info{
            linked_path = LinkedPath,
            file_path   = FilePath}} = ?get_obj_storage_info(ObjStorageId),

    %% Retrieve the remain disk-size
    case calc_remain_disksize(MetaDBId, LinkedPath) of
        {ok, RemainSize} ->
            case (RemainSize > 0) of
                true ->
                    %% Open the current object-container
                    %% and a new object-container
                    prepare_1(LinkedPath, FilePath, State);
                false ->
                    {{error, system_limit}, State}
            end;
        Error ->
            {Error, State}
    end.


prepare_1(LinkedPath, FilePath, #state{compaction_prms = CompactionPrms,
                                       is_diagnosing   = true} = State) ->
    case leo_object_storage_haystack:open(FilePath, read) of
        {ok, [_, ReadHandler, AVSVsnBinPrv]} ->
            FileSize = filelib:file_size(FilePath),
            ok = file:advise(ReadHandler,  0, FileSize, sequential),
            {ok, State#state{
                   obj_storage_info =
                       #backend_info{
                          avs_ver_cur   = <<>>,
                          avs_ver_prv   = AVSVsnBinPrv,
                          write_handler = undefined,
                          read_handler  = ReadHandler,
                          linked_path   = LinkedPath,
                          file_path     = []
                         },
                   compaction_prms =
                       CompactionPrms#compaction_prms{
                         start_lock_offset = FileSize
                        }
                  }
            };
        Error ->
            {Error, State}
    end;

prepare_1(LinkedPath, FilePath, #state{meta_db_id = MetaDBId,
                                       compaction_prms = CompactionPrms} = State) ->
    %% Create the new container
    NewFilePath = ?gen_raw_file_path(LinkedPath),

    case leo_object_storage_haystack:open(NewFilePath, write) of
        {ok, [WriteHandler, _, AVSVsnBinCur]} ->
            %% Open the current container
            case leo_object_storage_haystack:open(FilePath, read) of
                {ok, [_, ReadHandler, AVSVsnBinPrv]} ->
                    FileSize = filelib:file_size(FilePath),
                    ok = file:advise(WriteHandler, 0, FileSize, dont_need),
                    ok = file:advise(ReadHandler,  0, FileSize, sequential),

                    case leo_backend_db_api:run_compaction(MetaDBId) of
                        ok ->
                            {ok, State#state{
                                   obj_storage_info =
                                       #backend_info{
                                          avs_ver_cur   = AVSVsnBinCur,
                                          avs_ver_prv   = AVSVsnBinPrv,
                                          write_handler = WriteHandler,
                                          read_handler  = ReadHandler,
                                          linked_path   = LinkedPath,
                                          file_path     = NewFilePath
                                         },
                                   compaction_prms =
                                       CompactionPrms#compaction_prms{
                                         start_lock_offset = FileSize
                                        }
                                  }
                            };
                        Error ->
                            {Error, State}
                    end;
                Error ->
                    {Error, State}
            end;
        Error ->
            {Error, State}
    end.


%% @doc Reduce unnecessary objects from object-container.
%% @private
-spec(execute(State) ->
             {ok, State} | {{error, any()}, State} when State::#state{}).
execute(#state{meta_db_id       = MetaDBId,
               diagnosis_log_id = LoggerId,
               obj_storage_info = StorageInfo,
               is_diagnosing    = IsDiagnosing,
               is_recovering    = IsRecovering,
               compaction_prms  = CompactionPrms} = State) ->
    %% Initialize set-error property
    State_1 = State#state{set_errors = sets:new()},
    Offset   = CompactionPrms#compaction_prms.next_offset,
    Metadata = CompactionPrms#compaction_prms.metadata,

    %% Execute compaction
    case (Offset == ?AVS_SUPER_BLOCK_LEN) of
        true ->
            execute_1(State_1);
        false ->
            #compaction_prms{key_bin  = Key,
                             body_bin = Body,
                             callback_fun = CallbackFun,
                             num_of_active_objs  = NumOfActiveObjs,
                             size_of_active_objs = ActiveSize,
                             total_num_of_objs  = TotalObjs,
                             total_size_of_objs = TotaSize} = CompactionPrms,
            NumOfReplicas   = Metadata#?METADATA.num_of_replicas,
            HasChargeOfNode = case (CallbackFun == undefined) of
                                  true ->
                                      true;
                                  false ->
                                      CallbackFun(Key, NumOfReplicas)
                              end,

            case (is_removed_obj(MetaDBId, StorageInfo, Metadata, IsRecovering)
                  orelse HasChargeOfNode == false)  of
                true when IsDiagnosing == false ->
                    execute_1(State_1);
                true when IsDiagnosing == true ->
                    ok = output_diagnosis_log(LoggerId, Metadata),
                    execute_1(State_1#state{
                                compaction_prms = CompactionPrms#compaction_prms{
                                                    total_num_of_objs  = TotalObjs + 1,
                                                    total_size_of_objs = TotaSize  +
                                                        leo_object_storage_haystack:calc_obj_size(Metadata)
                                                   }});
                %%
                %% For data-compaction processing
                %%
                false when IsDiagnosing == false ->
                    %% Insert into the temporary object-container.
                    {Ret_1, NewState} =
                        case leo_object_storage_haystack:put_obj_to_new_cntnr(
                               StorageInfo#backend_info.write_handler, Metadata, Key, Body) of
                            {ok, Offset_1} ->
                                Metadata_1 = Metadata#?METADATA{offset = Offset_1},
                                KeyOfMeta  = ?gen_backend_key(StorageInfo#backend_info.avs_ver_cur,
                                                              Metadata#?METADATA.addr_id,
                                                              Metadata#?METADATA.key),
                                Ret = leo_backend_db_api:put_value_to_new_db(
                                        MetaDBId, KeyOfMeta, term_to_binary(Metadata_1)),

                                %% Calculate num of objects and total size of objects
                                execute_2(Ret, CompactionPrms, Metadata_1,
                                          NumOfActiveObjs, ActiveSize, State_1);
                            Error ->
                                {Error,
                                 State_1#state{compaction_prms =
                                                   CompactionPrms#compaction_prms{
                                                     metadata = Metadata}}}
                        end,
                    execute_1(Ret_1, NewState);
                %%
                %% For diagnosis processing
                %%
                false when IsDiagnosing == true ->
                    ok = output_diagnosis_log(LoggerId, Metadata),
                    {Ret_1, NewState} = execute_2(
                                          ok, CompactionPrms, Metadata,
                                          NumOfActiveObjs, ActiveSize, State_1),
                    CompactionPrms_1 = NewState#state.compaction_prms,
                    execute_1(Ret_1,
                              NewState#state{
                                compaction_prms = CompactionPrms_1#compaction_prms{
                                                    total_num_of_objs  = TotalObjs + 1,
                                                    total_size_of_objs = TotaSize  +
                                                        leo_object_storage_haystack:calc_obj_size(Metadata)
                                                   }})
            end
    end.


%% @doc Reduce unnecessary objects from object-container.
%% @private
-spec(execute_1(State) ->
             {ok, {next|eof, State}} |
             {{error, any()}, State} when State::#state{}).
execute_1(State) ->
    execute_1(ok, State).

-spec(execute_1(Ret, State) ->
             {ok, {next|eof, State}} |
             {{error, any()}, State} when Ret::ok|{error,any()},
                                          State::#state{}).
execute_1(ok, #state{meta_db_id       = MetaDBId,
                     obj_storage_info = StorageInfo,
                     compaction_prms  = CompactionPrms,
                     is_recovering    = IsRecovering} = State) ->
    erlang:garbage_collect(self()),
    ReadHandler = StorageInfo#backend_info.read_handler,
    NextOffset  = CompactionPrms#compaction_prms.next_offset,

    case leo_object_storage_haystack:get_obj_for_new_cntnr(
           ReadHandler, NextOffset) of
        {ok, NewMetadata, [_HeaderValue, NewKeyValue,
                           NewBodyValue, NewNextOffset]} ->
            %% If is-recovering is true, put a metadata to the backend-db
            case IsRecovering of
                true ->
                    #?METADATA{key = Key,
                               addr_id = AddrId} = NewMetadata,
                    KeyOfMetadata = ?gen_backend_key(
                                       StorageInfo#backend_info.avs_ver_prv, AddrId, Key),

                    case leo_backend_db_api:put(
                           MetaDBId, KeyOfMetadata, term_to_binary(NewMetadata)) of
                        ok ->
                            ok;
                        {error, Cause} ->
                            error_logger:info_msg("~p,~p,~p,~p~n",
                                                  [{module, ?MODULE_STRING},
                                                   {function, "execute_1/2"},
                                                   {line, ?LINE}, {body, Cause}]),
                            throw("unexpected_error happened at backend-db")
                    end;
                false ->
                    void
            end,
            %% Goto next object
            {ok, State_1} = output_accumulated_errors(State, NextOffset),
            {ok, {next, State_1#state{
                          error_pos  = 0,
                          set_errors = sets:new(),
                          compaction_prms =
                              CompactionPrms#compaction_prms{
                                key_bin     = NewKeyValue,
                                body_bin    = NewBodyValue,
                                metadata    = NewMetadata,
                                next_offset = NewNextOffset}
                         }}};

        %% Reached tail of the object-container
        {error, eof = Cause} ->
            {ok, State_1} = output_accumulated_errors(State, NextOffset),
            NumOfAcriveObjs  = CompactionPrms#compaction_prms.num_of_active_objs,
            SizeOfActiveObjs = CompactionPrms#compaction_prms.size_of_active_objs,
            {ok, {Cause, State_1#state{
                           error_pos  = 0,
                           set_errors = sets:new(),
                           compaction_prms =
                               CompactionPrms#compaction_prms{
                                 num_of_active_objs  = NumOfAcriveObjs,
                                 size_of_active_objs = SizeOfActiveObjs,
                                 next_offset = Cause}
                          }}};

        %% It found this object is broken,
        %% then it seeks a regular object,
        %% finally it reports a collapsed object to the error-log
        {_, Cause} ->
            ErrorPosCur = State#state.error_pos,
            ErrorPosNew = case (State#state.error_pos == 0) of
                              true ->
                                  NextOffset;
                              false ->
                                  ErrorPosCur
                          end,
            SetErrors = sets:add_element(Cause, State#state.set_errors),
            execute_1(ok,
                      State#state{error_pos  = ErrorPosNew,
                                  set_errors = SetErrors,
                                  compaction_prms =
                                      CompactionPrms#compaction_prms{
                                        next_offset = NextOffset + 1}
                                 })
    end;
execute_1(Error, State) ->
    {Error, State}.


%% @doc Calculate num of objects and total size of objects
%% @private
execute_2(Ret, CompactionPrms, Metadata, NumOfActiveObjs, ActiveSize, State) ->
    ObjectSize = leo_object_storage_haystack:calc_obj_size(Metadata),
    {Ret,
     State#state{compaction_prms =
                     CompactionPrms#compaction_prms{
                       metadata = Metadata,
                       num_of_active_objs  = NumOfActiveObjs + 1,
                       size_of_active_objs = ActiveSize + ObjectSize}}}.


%% @private
finish(#state{obj_storage_id   = ObjStorageId,
              compact_cntl_pid = CntlPid} = State) ->
    %% Generate the compaction report
    {ok, Report} = gen_compaction_report(State),

    %% Notify a message to the compaction-manager
    erlang:send(CntlPid, {finish, {ObjStorageId, Report}}),

    %% Unlock handling request
    ok = leo_object_storage_server:unlock(ObjStorageId),
    {ok, State#state{start_datetime = 0,
                     error_pos  = 0,
                     set_errors = sets:new(),
                     acc_errors = [],
                     obj_storage_info = #backend_info{},
                     result = undefined}}.

%% @private
after_execute(Ret, #state{obj_storage_info = StorageInfo,
                          is_diagnosing    = IsDiagnosing,
                          is_recovering    = _IsRecovering,
                          diagnosis_log_id = LoggerId} = State) ->
    %% Close file handlers
    ReadHandler  = StorageInfo#backend_info.read_handler,
    WriteHandler = StorageInfo#backend_info.write_handler,
    catch leo_object_storage_haystack:close(WriteHandler, ReadHandler),

    %% rotate the diagnosis-log
    case IsDiagnosing of
        true when LoggerId /= undefined ->
            leo_logger_client_base:force_rotation(LoggerId);
        _ ->
            void
    end,

    %% Finish compaction
    ok = after_execute_1({Ret, State}),
    finish(State).


%% @doc Reduce objects from the object-container.
%% @private
after_execute_1({_, #state{is_diagnosing = true,
                           compaction_prms =
                               #compaction_prms{
                                  num_of_active_objs  = _NumActiveObjs,
                                  size_of_active_objs = _SizeActiveObjs}}}) ->
    ok;

after_execute_1({ok, #state{meta_db_id       = MetaDBId,
                            obj_storage_id   = ObjStorageId,
                            obj_storage_info = StorageInfo,
                            compaction_prms =
                                #compaction_prms{
                                   num_of_active_objs  = NumActiveObjs,
                                   size_of_active_objs = SizeActiveObjs}}}) ->
    %% Unlink the symbol
    LinkedPath = StorageInfo#backend_info.linked_path,
    FilePath   = StorageInfo#backend_info.file_path,
    catch file:delete(LinkedPath),

    %% Migrate the filepath
    case file:make_symlink(FilePath, LinkedPath) of
        ok ->
            ok = leo_object_storage_server:switch_container(
                   ObjStorageId, FilePath, NumActiveObjs, SizeActiveObjs),
            ok = leo_backend_db_api:finish_compaction(MetaDBId, true),
            ok;
        {error,_Cause} ->
            leo_backend_db_api:finish_compaction(MetaDBId, false),
            ok
    end;

%% @doc Rollback - delete the tmp-files
%% @private
after_execute_1({_Error, #state{meta_db_id       = MetaDBId,
                                obj_storage_info = StorageInfo}}) ->
    %% must reopen the original file when handling at another process:
    catch file:delete(StorageInfo#backend_info.file_path),
    leo_backend_db_api:finish_compaction(MetaDBId, false),
    ok.


%% @doc Output the diagnosis log
%% @private
output_diagnosis_log(undefined,_Metadata) ->
    ok;
output_diagnosis_log(LoggerId, Metadata) ->
    ?output_diagnosis_log(Metadata).


%% @doc Output accumulated errors to logger
%% @private
-spec(output_accumulated_errors(State, ErrorPosEnd) ->
             {ok, State} when State::#state{},
                              ErrorPosEnd::non_neg_integer()).
output_accumulated_errors(#state{obj_storage_id = ObjStorageId,
                                 error_pos  = ErrorPosStart,
                                 set_errors = SetErrors,
                                 acc_errors = AccErrors} = State, ErrorPosEnd) ->
    case sets:size(SetErrors) of
        0 ->
            {ok, State};
        _ ->
            {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
            Errors = sets:to_list(SetErrors),
            error_logger:warning_msg(
              "~p,~p,~p,~p~n",
              [{module, ?MODULE_STRING},
               {function, "execute_1/4"},
               {line, ?LINE},
               {body, [{obj_container_path, StorageInfo#backend_info.file_path},
                       {error_pos_start, ErrorPosStart},
                       {error_pos_end,   ErrorPosEnd},
                       {errors,          Errors}]}
              ]),
            {ok, State#state{acc_errors = [{ErrorPosStart, ErrorPosEnd}|AccErrors]}}
    end.


%% @doc Is deleted a record ?
%% @private
-spec(is_removed_obj(MetaDBId, StorageInfo, Metadata, IsRecovering) ->
             boolean() when MetaDBId::atom(),
                            StorageInfo::#backend_info{},
                            Metadata::#?METADATA{},
                            IsRecovering::boolean()).
is_removed_obj(_MetaDBId,_StorageInfo,_Metdata, true) ->
    false;
is_removed_obj(MetaDBId, #backend_info{avs_ver_prv = AVSVsnBinPrv} = StorageInfo,
               #?METADATA{key = Key,
                          addr_id = AddrId} = MetaFromAvs,_IsRecovering) ->
    KeyOfMetadata = ?gen_backend_key(AVSVsnBinPrv, AddrId, Key),
    case leo_backend_db_api:get(MetaDBId, KeyOfMetadata) of
        {ok, MetaBin} ->
            case binary_to_term(MetaBin) of
                #?METADATA{del = ?DEL_TRUE} ->
                    true;
                Metadata ->
                    is_removed_obj_1(MetaDBId, StorageInfo, MetaFromAvs, Metadata)
            end;
        not_found ->
            true;
        _Other ->
            false
    end.

%% @private
-spec(is_removed_obj_1(MetaDBId, StorageInfo, Metadata_1, Metadata_2) ->
             boolean() when MetaDBId::atom(),
                            StorageInfo::#backend_info{},
                            Metadata_1::#?METADATA{},
                            Metadata_2::#?METADATA{}).
is_removed_obj_1(_MetaDBId,_StorageInfo,
                 #?METADATA{offset = Offset_1},
                 #?METADATA{offset = Offset_2}) when Offset_1 /= Offset_2 ->
    true;
is_removed_obj_1(_MetaDBId,_StorageInfo,_Meta_1,_Meta_2) ->
    false.


%% @doc Generate compaction report
%% @private
gen_compaction_report(State) ->
    #state{obj_storage_id   = ObjStorageId,
           compaction_prms  = CompactionPrms,
           obj_storage_info = #backend_info{file_path   = FilePath,
                                            linked_path = LinkedPath,
                                            avs_ver_prv = AVSVerPrev,
                                            avs_ver_cur = AVSVerCur},
           start_datetime = StartDateTime,
           is_diagnosing  = IsDiagnosing,
           is_recovering  = _IsRecovering,
           acc_errors     = AccErrors,
           result = Ret} = State,

    %% Append the compaction history
    EndDateTime = leo_date:now(),
    Duration = EndDateTime - StartDateTime,
    StartDateTime_1 = lists:flatten(leo_date:date_format(StartDateTime)),
    EndDateTime_1   = lists:flatten(leo_date:date_format(EndDateTime)),
    ok = leo_object_storage_server:append_compaction_history(
           ObjStorageId, #compaction_hist{start_datetime = StartDateTime,
                                          end_datetime   = EndDateTime,
                                          duration       = Duration,
                                          result = Ret}),
    %% Generate a report of the results
    #compaction_prms{num_of_active_objs  = ActiveObjs,
                     size_of_active_objs = ActiveSize,
                     total_num_of_objs   = TotalObjs,
                     total_size_of_objs  = TotalSize} = CompactionPrms,

    CntnrVer  = case IsDiagnosing of
                    true  -> AVSVerPrev;
                    false -> AVSVerCur
                end,
    CntnrPath = case IsDiagnosing of
                    true  -> LinkedPath;
                    false -> FilePath
                end,
    TotalObjs_1 = case IsDiagnosing of
                      true  -> TotalObjs;
                      false -> ActiveObjs
                  end,
    TotalSize_1 = case IsDiagnosing of
                      true  -> TotalSize;
                      false -> ActiveSize
                  end,
    Report = #compaction_report{
                file_path = CntnrPath,
                avs_ver   = CntnrVer,
                num_of_active_objs  = ActiveObjs,
                size_of_active_objs = ActiveSize,
                total_num_of_objs   = TotalObjs_1,
                total_size_of_objs  = TotalSize_1,
                start_datetime = StartDateTime_1,
                end_datetime   = EndDateTime_1,
                errors   = lists:reverse(AccErrors),
                duration = Duration,
                result   = Ret
               },

    case leo_object_storage_server:get_stats(ObjStorageId) of
        {ok, #storage_stats{file_path = ObjDir} = CurStats} ->
            %% Update the storage-state
            ok = leo_object_storage_server:set_stats(ObjStorageId,
                                                     CurStats#storage_stats{
                                                       total_sizes  = TotalSize_1,
                                                       active_sizes = ActiveSize,
                                                       total_num    = TotalObjs_1,
                                                       active_num   = ActiveObjs}),
            %% Output report of the storage-container
            Tokens = string:tokens(ObjDir, "/"),
            case (length(Tokens) > 2) of
                true ->
                    LogFilePath = filename:join(["/"]
                                                ++ lists:sublist(Tokens, length(Tokens) - 2)
                                                ++ [?DEF_LOG_SUB_DIR
                                                    ++ atom_to_list(ObjStorageId)
                                                    ++ ?DIAGNOSIS_REP_SUFFIX
                                                    ++ "."
                                                    ++ integer_to_list(leo_date:now())
                                                   ]),
                    Report_1 = lists:zip(record_info(fields, compaction_report),
                                         tl(tuple_to_list(Report))),
                    catch leo_file:file_unconsult(LogFilePath, Report_1),
                    error_logger:info_msg("~p,~p,~p,~p~n",
                                          [{module, ?MODULE_STRING},
                                           {function, "gen_compaction_report/1"},
                                           {line, ?LINE}, {body, [Report_1]}]),
                    ok;
                false ->
                    void
            end;
        _ ->
            void
    end,
    {ok, Report}.


%% @doc Increase the waiting time
%% @private
-spec(incr_interval_fun(WaitingTime, MaxWaitingTime, StepWaitingTime) ->
             NewWaitingTime when WaitingTime::non_neg_integer(),
                                 MaxWaitingTime::non_neg_integer(),
                                 StepWaitingTime::non_neg_integer(),
                                 NewWaitingTime::non_neg_integer()).
incr_interval_fun(WaitingTime, MaxWaitingTime, StepWaitingTime) ->
    WaitingTime_1 = WaitingTime + StepWaitingTime,
    case (WaitingTime_1 > MaxWaitingTime) of
        true  -> MaxWaitingTime;
        false -> WaitingTime_1
    end.


%% @doc Decrease the waiting time
%% @private
-spec(decr_interval_fun(WaitingTime, MinWaitingTime, StepWaitingTime) ->
             NewWaitingTime when WaitingTime::non_neg_integer(),
                                 MinWaitingTime::non_neg_integer(),
                                 StepWaitingTime::non_neg_integer(),
                                 NewWaitingTime::non_neg_integer()).
decr_interval_fun(WaitingTime, MinWaitingTime, StepWaitingTime) ->
    WaitingTime_1 = WaitingTime - StepWaitingTime,
    case (WaitingTime_1 < MinWaitingTime) of
        true  -> MinWaitingTime;
        false -> WaitingTime_1
    end.


%% @doc Increase the batch procs
%% @private
-spec(incr_batch_of_msgs_fun(BatchProcs, MaxBatchProcs, StepBatchProcs) ->
             NewBatchProcs when BatchProcs::non_neg_integer(),
                                MaxBatchProcs::non_neg_integer(),
                                StepBatchProcs::non_neg_integer(),
                                NewBatchProcs::non_neg_integer()).
incr_batch_of_msgs_fun(BatchProcs, MaxBatchProcs, StepBatchProcs) ->
    BatchProcs_1 = BatchProcs + StepBatchProcs,
    case (BatchProcs_1 > MaxBatchProcs) of
        true  -> MaxBatchProcs;
        false -> BatchProcs_1
    end.

%% @doc Increase the batch procs
%% @private
-spec(decr_batch_of_msgs_fun(BatchProcs, MinBatchProcs, StepBatchProcs) ->
             NewBatchProcs when BatchProcs::non_neg_integer(),
                                MinBatchProcs::non_neg_integer(),
                                StepBatchProcs::non_neg_integer(),
                                NewBatchProcs::non_neg_integer()).
decr_batch_of_msgs_fun(BatchProcs, MinBatchProcs, StepBatchProcs) ->
    BatchProcs_1 = BatchProcs - StepBatchProcs,
    case (BatchProcs_1 < MinBatchProcs) of
        true  -> MinBatchProcs;
        false -> BatchProcs_1
    end.

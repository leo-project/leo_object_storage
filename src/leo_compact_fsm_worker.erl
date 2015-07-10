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
         forced_run/3,
         suspend/1,
         resume/1,
         state/2,
         increase/1,
         decrease/1
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

-define(DEF_TIMEOUT, timer:seconds(30)).


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
    gen_fsm:send_event(Id, #compaction_event_info{event = ?EVENT_RUN,
                                                  is_diagnosing = IsDiagnosing,
                                                  is_recovering = IsRecovering,
                                                  is_forced_run = false}).

-spec(run(Id, ControllerPid, IsDiagnosing, IsRecovering, CallbackFun) ->
             ok | {error, any()} when Id::atom(),
                                      ControllerPid::pid(),
                                      IsDiagnosing::boolean(),
                                      IsRecovering::boolean(),
                                      CallbackFun::function()).
run(Id, ControllerPid, IsDiagnosing, IsRecovering, CallbackFun) ->
    gen_fsm:sync_send_event(Id, #compaction_event_info{event = ?EVENT_RUN,
                                                       controller_pid = ControllerPid,
                                                       is_diagnosing  = IsDiagnosing,
                                                       is_recovering  = IsRecovering,
                                                       is_forced_run  = false,
                                                       callback = CallbackFun}, ?DEF_TIMEOUT).

-spec(forced_run(Id, IsDiagnosing, IsRecovering) ->
             ok | {error, any()} when Id::atom(),
                                      IsDiagnosing::boolean(),
                                      IsRecovering::boolean()).
forced_run(Id, IsDiagnosing, IsRecovering) ->
    gen_fsm:send_event(Id, #compaction_event_info{event = ?EVENT_RUN,
                                                  is_diagnosing = IsDiagnosing,
                                                  is_recovering = IsRecovering,
                                                  is_forced_run = true}).


%% @doc Retrieve an object from the object-storage
%%
-spec(suspend(Id) ->
             ok | {error, any()} when Id::atom()).
suspend(Id) ->
    gen_fsm:send_event(Id, #compaction_event_info{event = ?EVENT_SUSPEND}).


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(resume(Id) ->
             ok | {error, any()} when Id::atom()).
resume(Id) ->
    gen_fsm:sync_send_event(Id, #compaction_event_info{event = ?EVENT_RESUME}, ?DEF_TIMEOUT).


%% @doc Retrieve the storage stats specfied by Id
%%      which contains number of objects and so on.
%%
-spec(state(Id, Client) ->
             ok | {error, any()} when Id::atom(),
                                      Client::pid()).
state(Id, Client) ->
    gen_fsm:send_event(Id, #compaction_event_info{event = ?EVENT_STATE,
                                                  client_pid = Client}).


%% @doc Increase performance of the data-compaction processing
%%
-spec(increase(Id) ->
             ok when Id::atom()).
increase(Id) ->
    gen_fsm:send_event(Id, #compaction_event_info{event = ?EVENT_INCREASE}).


%% @doc Decrease performance of the data-compaction processing
%%
%%
-spec(decrease(Id) ->
             ok when Id::atom()).
decrease(Id) ->
    gen_fsm:send_event(Id, #compaction_event_info{event = ?EVENT_DECREASE}).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% @doc Initiates the server
%%
init([Id, ObjStorageId, MetaDBId, LoggerId]) ->
    {ok, ?ST_IDLING, #compaction_worker_state{
                        id = Id,
                        obj_storage_id     = ObjStorageId,
                        meta_db_id         = MetaDBId,
                        diagnosis_log_id   = LoggerId,
                        interval       = ?env_compaction_interval_reg(),
                        max_interval   = ?env_compaction_interval_max(),
                        num_of_batch_procs        = ?env_compaction_num_of_batch_procs_reg(),
                        max_num_of_batch_procs    = ?env_compaction_num_of_batch_procs_max(),
                        compaction_prms = #compaction_prms{
                                             key_bin  = <<>>,
                                             body_bin = <<>>,
                                             metadata = #?METADATA{},
                                             next_offset = 0,
                                             num_of_active_objs  = 0,
                                             size_of_active_objs = 0}}}.

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
             {next_state, ?ST_IDLING | ?ST_RUNNING, State}
                 when EventInfo::#compaction_event_info{},
                      From::{pid(),Tag::atom()},
                      State::#compaction_worker_state{}).
idling(#compaction_event_info{event = ?EVENT_RUN,
                              controller_pid = ControllerPid,
                              is_diagnosing  = IsDiagnosing,
                              is_recovering  = IsRecovering,
                              is_forced_run  = false,
                              callback = CallbackFun}, From,
       #compaction_worker_state{id = Id,
                                compaction_prms = CompactionPrms} = State) ->
    NextStatus = ?ST_RUNNING,
    State_1 = State#compaction_worker_state{
                compact_cntl_pid = ControllerPid,
                status        = NextStatus,
                is_diagnosing = IsDiagnosing,
                is_recovering = IsRecovering,
                error_pos     = 0,
                set_errors    = sets:new(),
                acc_errors    = [],
                interval      = ?env_compaction_interval_reg(),
                max_interval  = ?env_compaction_interval_max(),
                num_of_batch_procs     = ?env_compaction_num_of_batch_procs_reg(),
                max_num_of_batch_procs = ?env_compaction_num_of_batch_procs_max(),
                compaction_prms =
                    CompactionPrms#compaction_prms{
                      num_of_active_objs  = 0,
                      size_of_active_objs = 0,
                      next_offset = ?AVS_SUPER_BLOCK_LEN,
                      callback_fun = CallbackFun
                     },
                start_datetime = leo_date:now()},
    case prepare(State_1) of
        {ok, State_2} ->
            gen_fsm:reply(From, ok),
            ok = run(Id, IsDiagnosing, IsRecovering),
            {next_state, NextStatus, State_2};
        {{error, Cause}, #compaction_worker_state{obj_storage_info =
                                                      #backend_info{write_handler = WriteHandler,
                                                                    read_handler  = ReadHandler}}} ->
            catch leo_object_storage_haystack:close(WriteHandler, ReadHandler),
            gen_fsm:reply(From, {error, Cause}),
            {next_state, ?ST_IDLING, State_1}
    end;
idling(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}}.

-spec(idling(EventInfo, State) ->
             {next_state, ?ST_IDLING, State} when EventInfo::#compaction_event_info{},
                                                  State::#compaction_worker_state{}).
idling(#compaction_event_info{event = ?EVENT_STATE,
                              client_pid = Client}, State) ->
    NextStatus = ?ST_IDLING,
    erlang:send(Client, NextStatus),
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}};

idling(#compaction_event_info{event = ?EVENT_INCREASE}, State) ->
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}};

idling(#compaction_event_info{event = ?EVENT_DECREASE}, State) ->
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}};
idling(_, State) ->
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}}.


%% @doc State of 'running'
-spec(running(EventInfo, State) ->
             {next_state, ?ST_RUNNING, State} when EventInfo::#compaction_event_info{},
                                                   State::#compaction_worker_state{}).
running(#compaction_event_info{event = ?EVENT_RUN},
        #compaction_worker_state{obj_storage_id = #backend_info{linked_path = []}} = State) ->
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}};
running(#compaction_event_info{event = ?EVENT_RUN},
        #compaction_worker_state{obj_storage_id = #backend_info{file_path = []}} = State) ->
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}};
running(#compaction_event_info{event = ?EVENT_RUN,
                               is_diagnosing = IsDiagnosing,
                               is_recovering = IsRecovering},
        #compaction_worker_state{id = Id,
                                 obj_storage_id   = ObjStorageId,
                                 compact_cntl_pid = CompactCntlPid,
                                 interval  = Interval,
                                 count_procs = CountProcs,
                                 num_of_batch_procs = BatchProcs,
                                 compaction_prms =
                                     #compaction_prms{
                                        start_lock_offset = StartLockOffset}} = State) ->

    %% Temporally suspend the compaction
    %% in order to decrease i/o load
    CountProcs_1 = case (CountProcs < 1) of
                       true ->
                           Interval_1 = case (Interval < 1) of
                                            true  -> ?DEF_MAX_COMPACTION_WT;
                                            false -> Interval
                                        end,
                           timer:sleep(Interval_1),
                           BatchProcs;
                       false ->
                           CountProcs - 1
                   end,

    {NextStatus, State_3} =
        case catch execute(State#compaction_worker_state{
                             is_diagnosing = IsDiagnosing,
                             is_recovering = IsRecovering}) of
            %% Lock the object-storage in order to
            %%     reject requests during the data-compaction
            {ok, {next, #compaction_worker_state{
                           is_locked = IsLocked,
                           compaction_prms =
                               #compaction_prms{next_offset = NextOffset}
                          } = State_1}} when NextOffset > StartLockOffset ->
                State_2 = case IsLocked of
                              true ->
                                  erlang:send(CompactCntlPid, run),
                                  State_1;
                              false ->
                                  ok = leo_object_storage_server:lock(ObjStorageId),
                                  erlang:send(CompactCntlPid, {lock, Id}),
                                  State_1#compaction_worker_state{is_locked = true}
                          end,
                ok = run(Id, IsDiagnosing, IsRecovering),
                {?ST_RUNNING, State_2};
            %% Execute the data-compaction repeatedly
            {ok, {next, State_1}} ->
                ok = run(Id, IsDiagnosing, IsRecovering),
                erlang:send(CompactCntlPid, run),
                {?ST_RUNNING, State_1};

            %% An unxepected error has occured
            {'EXIT', Cause} ->
                error_logger:info_msg("~p,~p,~p,~p~n",
                                      [{module, ?MODULE_STRING}, {function, "running/2"},
                                       {line, ?LINE}, {body, {Id, Cause}}]),
                {ok, State_1} = after_execute({error, Cause},
                                              State#compaction_worker_state{result = ?RET_FAIL}),
                {?ST_IDLING, State_1};
            %% Reached end of the object-container
            {ok, {eof, State_1}} ->
                {ok, State_2} = after_execute(ok,
                                              State_1#compaction_worker_state{result = ?RET_SUCCESS}),
                {?ST_IDLING, State_2};
            %% An epected error has occured
            {{error, Cause}, State_1} ->
                error_logger:info_msg("~p,~p,~p,~p~n",
                                      [{module, ?MODULE_STRING}, {function, "running/2"},
                                       {line, ?LINE}, {body, {Id, Cause}}]),
                {ok, State_2} = after_execute({error, Cause},
                                              State_1#compaction_worker_state{result = ?RET_FAIL}),
                {?ST_IDLING, State_2}
        end,
    {next_state, NextStatus, State_3#compaction_worker_state{status = NextStatus,
                                                             count_procs = CountProcs_1}};

running(#compaction_event_info{event = ?EVENT_SUSPEND}, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus,
                                                           is_forced_suspending = true}};

running(#compaction_event_info{event = ?EVENT_STATE,
                               client_pid = Client}, State) ->
    NextStatus = ?ST_RUNNING,
    erlang:send(Client, NextStatus),
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}};

running(#compaction_event_info{event = ?EVENT_INCREASE},
        #compaction_worker_state{num_of_batch_procs = BatchProcs,
                                 max_num_of_batch_procs = MaxBatchProcs,
                                 interval = Interval,
                                 num_of_steps = NumOfSteps} = State) ->
    {ok, {StepBatchProcs, StepInterval}} =
        ?step_compaction_proc_values(?env_compaction_num_of_batch_procs_reg(),
                                     ?env_compaction_interval_reg(),
                                     NumOfSteps),
    BatchProcs_1 = incr_batch_of_msgs_fun(BatchProcs, MaxBatchProcs, StepBatchProcs),
    Interval_1 = decr_interval_fun(Interval, StepInterval),

    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#compaction_worker_state{
                               num_of_batch_procs = BatchProcs_1,
                               interval = Interval_1,
                               status = NextStatus}};

running(#compaction_event_info{event = ?EVENT_DECREASE},
        #compaction_worker_state{num_of_batch_procs = BatchProcs,
                                 interval = Interval,
                                 max_interval  = MaxInterval,
                                 num_of_steps = NumOfSteps} = State) ->
    {ok, {StepBatchProcs, StepInterval}} =
        ?step_compaction_proc_values(?env_compaction_num_of_batch_procs_reg(),
                                     ?env_compaction_interval_reg(),
                                     NumOfSteps),
    Interval_1 = incr_interval_fun(Interval, MaxInterval, StepInterval),

    {NextStatus, BatchProcs_1} =
        case (BatchProcs =< 0) of
            true ->
                {?ST_SUSPENDING, 0};
            false ->
                {?ST_RUNNING,
                 decr_batch_of_msgs_fun(BatchProcs, StepBatchProcs)}
        end,
    {next_state, NextStatus, State#compaction_worker_state{
                               num_of_batch_procs = BatchProcs_1,
                               interval = Interval_1,
                               status = NextStatus}};

running(_, State) ->
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}}.


-spec(running( _, _, #compaction_worker_state{}) ->
             {next_state, ?ST_SUSPENDING|?ST_RUNNING, #compaction_worker_state{}}).
running(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}}.


%% @doc State of 'suspend'
%%
-spec(suspending(EventInfo, State) ->
             {next_state, ?ST_SUSPENDING, State} when EventInfo::#compaction_event_info{},
                                                      State::#compaction_worker_state{}).
suspending(#compaction_event_info{event = ?EVENT_RUN}, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}};

suspending(#compaction_event_info{event = ?EVENT_STATE,
                                  client_pid = Client}, State) ->
    NextStatus = ?ST_SUSPENDING,
    erlang:send(Client, NextStatus),
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}};

suspending(#compaction_event_info{event = ?EVENT_INCREASE},
           #compaction_worker_state{is_forced_suspending = true} = State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}};
suspending(#compaction_event_info{event = ?EVENT_INCREASE},
           #compaction_worker_state{id = Id,
                                    is_diagnosing = IsDiagnosing,
                                    is_recovering = IsRecovering,
                                    num_of_batch_procs = BatchProcs,
                                    max_num_of_batch_procs = MaxBatchProcs,
                                    interval = Interval,
                                    num_of_steps = NumOfSteps} = State) ->
    {ok, {StepBatchProcs, StepInterval}} =
        ?step_compaction_proc_values(?env_compaction_num_of_batch_procs_reg(),
                                     ?env_compaction_interval_reg(),
                                     NumOfSteps),
    BatchProcs_1 = incr_batch_of_msgs_fun(BatchProcs, MaxBatchProcs, StepBatchProcs),
    Interval_1 = decr_interval_fun(Interval, StepInterval),

    NextStatus = ?ST_RUNNING,
    timer:apply_after(timer:seconds(1), ?MODULE, run, [Id, IsDiagnosing, IsRecovering]),
    {next_state, NextStatus, State#compaction_worker_state{
                               num_of_batch_procs = BatchProcs_1,
                               interval = Interval_1,
                               status = NextStatus}};
suspending(_, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}}.


-spec(suspending(EventInfo, From, State) ->
             {next_state, ?ST_SUSPENDING | ?ST_RUNNING, State}
                 when EventInfo::#compaction_event_info{},
                      From::{pid(),Tag::atom()},
                      State::#compaction_worker_state{}).
suspending(#compaction_event_info{event = ?EVENT_RESUME}, From,
           #compaction_worker_state{id = Id,
                                    is_diagnosing = IsDiagnosing,
                                    is_recovering = IsRecovering} = State) ->
    %% resume the data-compaction
    gen_fsm:reply(From, ok),

    NextStatus = ?ST_RUNNING,
    timer:apply_after(
      timer:seconds(1), ?MODULE, run, [Id, IsDiagnosing, IsRecovering]),
    {next_state, NextStatus, State#compaction_worker_state{
                               status = NextStatus,
                               is_forced_suspending = false}};

suspending(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#compaction_worker_state{status = NextStatus}}.


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
             {ok, State} | {{error, any()}, State} when State::#compaction_worker_state{}).
prepare(#compaction_worker_state{obj_storage_id = ObjStorageId,
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


prepare_1(LinkedPath, FilePath,
          #compaction_worker_state{compaction_prms = CompactionPrms,
                                   is_diagnosing   = true} = State) ->
    case leo_object_storage_haystack:open(FilePath, read) of
        {ok, [_, ReadHandler, AVSVsnBinPrv]} ->
            FileSize = filelib:file_size(FilePath),
            ok = file:advise(ReadHandler,  0, FileSize, sequential),
            {ok, State#compaction_worker_state{
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

prepare_1(LinkedPath, FilePath,
          #compaction_worker_state{meta_db_id = MetaDBId,
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
                            {ok, State#compaction_worker_state{
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
             {ok, State} | {{error, any()}, State} when State::#compaction_worker_state{}).
execute(#compaction_worker_state{meta_db_id       = MetaDBId,
                                 diagnosis_log_id = LoggerId,
                                 obj_storage_info = StorageInfo,
                                 is_diagnosing    = IsDiagnosing,
                                 is_recovering    = IsRecovering,
                                 compaction_prms  = CompactionPrms} = State) ->
    %% Initialize set-error property
    State_1 = State#compaction_worker_state{set_errors = sets:new()},
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
                    execute_1(State_1#compaction_worker_state{
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
                                 State_1#compaction_worker_state{compaction_prms =
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
                    CompactionPrms_1 = NewState#compaction_worker_state.compaction_prms,
                    execute_1(Ret_1,
                              NewState#compaction_worker_state{
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
             {{error, any()}, State} when State::#compaction_worker_state{}).
execute_1(State) ->
    execute_1(ok, State).
execute_1(Ret, State) ->
    execute_1(Ret, State, 1).

-spec(execute_1(Ret, State, RetryTimes) ->
             {ok, {next|eof, State}} |
             {{error, any()}, State} when Ret::ok|{error,any()},
                                          State::#compaction_worker_state{},
                                          RetryTimes::non_neg_integer()).
execute_1(ok = Ret, #compaction_worker_state{meta_db_id       = MetaDBId,
                                             obj_storage_info = StorageInfo,
                                             compaction_prms  = CompactionPrms,
                                             is_recovering    = IsRecovering} = State, RetryTimes) ->
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
                            error_logger:error_msg("~p,~p,~p,~p~n",
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
            {ok, {next, State_1#compaction_worker_state{
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
            {ok, {Cause, State_1#compaction_worker_state{
                           error_pos  = 0,
                           set_errors = sets:new(),
                           compaction_prms =
                               CompactionPrms#compaction_prms{
                                 num_of_active_objs  = NumOfAcriveObjs,
                                 size_of_active_objs = SizeOfActiveObjs,
                                 next_offset = Cause}
                          }}};

        %% Aan issue of unexpected length happened,
        %% then need to rollback the data-compaction
        {error, {abort, unexpected_len = Cause}} when RetryTimes == ?MAX_RETRY_TIMES ->
            erlang:error(Cause);
        {error, {abort, unexpected_len = Cause}} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "execute_1/3"},
                                    {line, ?LINE}, [{offset, NextOffset},
                                                    {body, Cause}]]),
            timer:sleep(?WAIT_TIME_AFTER_ERROR),
            execute_1(Ret, State, RetryTimes + 1);

        %% It found this object is broken,
        %% then it seeks a regular object,
        %% finally it reports a collapsed object to the error-log
        {_, Cause} ->
            ErrorPosCur = State#compaction_worker_state.error_pos,
            ErrorPosNew = case (State#compaction_worker_state.error_pos == 0) of
                              true ->
                                  NextOffset;
                              false ->
                                  ErrorPosCur
                          end,
            SetErrors = sets:add_element(Cause,
                                         State#compaction_worker_state.set_errors),
            execute_1(ok,
                      State#compaction_worker_state{
                        error_pos  = ErrorPosNew,
                        set_errors = SetErrors,
                        compaction_prms =
                            CompactionPrms#compaction_prms{
                              next_offset = NextOffset + 1}})
    end;
execute_1(Error, State,_) ->
    {Error, State}.


%% @doc Calculate num of objects and total size of objects
%% @private
execute_2(Ret, CompactionPrms, Metadata, NumOfActiveObjs, ActiveSize, State) ->
    ObjectSize = leo_object_storage_haystack:calc_obj_size(Metadata),
    {Ret,
     State#compaction_worker_state{
       compaction_prms = CompactionPrms#compaction_prms{
                           metadata = Metadata,
                           num_of_active_objs  = NumOfActiveObjs + 1,
                           size_of_active_objs = ActiveSize + ObjectSize}}}.


%% @private
finish(#compaction_worker_state{obj_storage_id   = ObjStorageId,
                                compact_cntl_pid = CntlPid} = State) ->
    %% Generate the compaction report
    {ok, Report} = gen_compaction_report(State),

    %% Notify a message to the compaction-manager
    erlang:send(CntlPid, {finish, {ObjStorageId, Report}}),

    %% Unlock handling request
    ok = leo_object_storage_server:unlock(ObjStorageId),
    {ok, State#compaction_worker_state{start_datetime = 0,
                                       error_pos  = 0,
                                       set_errors = sets:new(),
                                       acc_errors = [],
                                       obj_storage_info = #backend_info{},
                                       result = undefined}}.

%% @private
after_execute(Ret, #compaction_worker_state{obj_storage_info = StorageInfo,
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
after_execute_1({_, #compaction_worker_state{
                       is_diagnosing = true,
                       compaction_prms =
                           #compaction_prms{
                              num_of_active_objs  = _NumActiveObjs,
                              size_of_active_objs = _SizeActiveObjs}}}) ->
    ok;

after_execute_1({ok, #compaction_worker_state{
                        meta_db_id       = MetaDBId,
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
after_execute_1({_Error, #compaction_worker_state{
                            meta_db_id = MetaDBId,
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
             {ok, State} when State::#compaction_worker_state{},
                              ErrorPosEnd::non_neg_integer()).
output_accumulated_errors(#compaction_worker_state{
                             obj_storage_id = ObjStorageId,
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
            {ok, State#compaction_worker_state{
                   acc_errors = [{ErrorPosStart, ErrorPosEnd}|AccErrors]}}
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
    #compaction_worker_state{obj_storage_id   = ObjStorageId,
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
-spec(incr_interval_fun(Interval, MaxInterval, StepInterval) ->
             NewInterval when Interval::non_neg_integer(),
                              MaxInterval::non_neg_integer(),
                              StepInterval::non_neg_integer(),
                              NewInterval::non_neg_integer()).
incr_interval_fun(Interval, MaxInterval, StepInterval) ->
    Interval_1 = Interval + StepInterval,
    case (Interval_1 >= MaxInterval) of
        true ->
            MaxInterval;
        false ->
            Interval_1
    end.


%% @doc Decrease the waiting time
%% @private
-spec(decr_interval_fun(Interval, StepInterval) ->
             NewInterval when Interval::non_neg_integer(),
                              StepInterval::non_neg_integer(),
                              NewInterval::non_neg_integer()).
decr_interval_fun(Interval, StepInterval) ->
    Interval_1 = Interval - StepInterval,
    case (Interval_1 =< ?DEF_MIN_COMPACTION_WT) of
        true ->
            ?DEF_MIN_COMPACTION_WT;
        false ->
            Interval_1
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
        true ->
            MaxBatchProcs;
        false ->
            BatchProcs_1
    end.

%% @doc Increase the batch procs
%% @private
-spec(decr_batch_of_msgs_fun(BatchProcs, StepBatchProcs) ->
             NewBatchProcs when BatchProcs::non_neg_integer(),
                                StepBatchProcs::non_neg_integer(),
                                NewBatchProcs::non_neg_integer()).
decr_batch_of_msgs_fun(BatchProcs, StepBatchProcs) ->
    BatchProcs_1 = BatchProcs - StepBatchProcs,
    case (BatchProcs_1 < 0) of
        true ->
            ?DEF_MIN_COMPACTION_BP;
        false ->
            BatchProcs_1
    end.

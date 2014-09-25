%%======================================================================
%%
%% Leo Object Storage
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
%% @doc FSM of the data-compaction worker, which handles removing unnecessary objects from a object container.
%% @reference [https://github.com/leo-project/leo_object_storage/blob/master/src/leo_compact_fsm_controller.erl]
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
-export([run/2, run/4,
         suspend/1,
         resume/1,
         finish/1,
         state/2]).

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
          event = ?EVENT_RUN :: event_of_compaction(),
          controller_pid :: pid(),
          client_pid     :: pid(),
          is_diagnosing = false :: boolean(),
          callback :: function()
         }).

-record(compaction_prms, {
          key_bin  = <<>> :: binary(),
          body_bin = <<>> :: binary(),
          metadata = #?METADATA{} :: #?METADATA{},
          next_offset = 0         :: non_neg_integer()|eof,
          start_lock_offset = 0   :: non_neg_integer(),
          callback_fun            :: function(),
          num_of_active_objs = 0  :: integer(),
          size_of_active_objs = 0 :: integer()
         }).

-record(state, {
          id :: atom(),
          obj_storage_id :: atom(),
          meta_db_id     :: atom(),
          obj_storage_info = #backend_info{} :: #backend_info{},
          compact_cntl_pid      :: pid(),
          diagnosis_log_id      :: atom(),
          status = ?ST_IDLING   :: state_of_compaction(),
          is_locked = false     :: boolean(),
          is_diagnosing = false :: boolean(),
          waiting_time = 0      :: non_neg_integer(),
          compaction_prms = #compaction_prms{} :: #compaction_prms{},
          start_datetime = 0 :: non_neg_integer(),
          error_pos = 0      :: non_neg_integer(),
          set_errors         :: set(),
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
-spec(run(Id, IsDiagnose) ->
             ok | {error, any()} when Id::atom(),
                                      IsDiagnose::boolean()).
run(Id, IsDiagnose) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_RUN,
                                       is_diagnosing = IsDiagnose
                                      }).

-spec(run(Id, ControllerPid, IsDiagnose, CallbackFun) ->
             ok | {error, any()} when Id::atom(),
                                      ControllerPid::pid(),
                                      IsDiagnose::boolean(),
                                      CallbackFun::function()).
run(Id, ControllerPid, IsDiagnose, CallbackFun) ->
    gen_fsm:sync_send_event(Id, #event_info{event = ?EVENT_RUN,
                                            controller_pid = ControllerPid,
                                            is_diagnosing  = IsDiagnose,
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


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(finish(Id) ->
             ok | {error, any()} when Id::atom()).
finish(Id) ->
    gen_fsm:send_event(Id, #event_info{event = ?EVENT_FINISH}).


%% @doc Retrieve the storage stats specfied by Id
%%      which contains number of objects and so on.
%%
-spec(state(Id, Client) ->
             ok | {error, any()} when Id::atom(),
                                      Client::pid()).
state(Id, Client) ->
    gen_fsm:send_event(Id, #event_info{event = state,
                                       client_pid = Client}).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% @doc Initiates the server
%%
init([Id, ObjStorageId, MetaDBId, LoggerId]) ->
    {ok, ?ST_IDLING, #state{id = Id,
                            obj_storage_id   = ObjStorageId,
                            meta_db_id       = MetaDBId,
                            diagnosis_log_id = LoggerId,
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
                   is_diagnosing  = IsDiagnose,
                   callback = CallbackFun}, From, #state{id = Id,
                                                         compaction_prms =
                                                             CompactionPrms} = State) ->
    NextStatus = ?ST_RUNNING,
    State_1 = State#state{compact_cntl_pid = ControllerPid,
                          status        = NextStatus,
                          is_diagnosing = IsDiagnose,
                          error_pos     = 0,
                          set_errors    = sets:new(),
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
            ok = run(Id, IsDiagnose),
            {next_state, NextStatus, State_2};
        {{error, Cause},_State} ->
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
idling(_, State) ->
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


-spec(running(EventInfo, State) ->
             {next_state, ?ST_RUNNING, State} when EventInfo::#event_info{},
                                                   State::#state{}).
running(#event_info{event = ?EVENT_RUN,
                    is_diagnosing = IsDiagnose}, #state{id = Id,
                                                        obj_storage_id   = ObjStorageId,
                                                        compact_cntl_pid = CompactCntlPid,
                                                        compaction_prms  =
                                                            #compaction_prms{
                                                               start_lock_offset = StartLockOffset
                                                              }} = State) ->
    NextStatus = ?ST_RUNNING,
    State_3 =
        case catch execute(State#state{is_diagnosing = IsDiagnose}) of
            %% Lock the object-storage in order to
            %%     reject requests during the data-compaction
            {ok, {next, #state{is_locked = IsLocked,
                               compaction_prms =
                                   #compaction_prms{
                                      next_offset = NextOffset}
                              } = State_1}} when NextOffset > StartLockOffset ->
                State_2 = case IsLocked of
                              true ->
                                  State_1;
                              false ->
                                  ok = leo_object_storage_server:lock(ObjStorageId),
                                  erlang:send(CompactCntlPid, {lock, Id}),
                                  State_1#state{is_locked = true}
                          end,
                ok = run(Id, IsDiagnose),
                State_2;
            %% Execute the data-compaction repeatedly
            {ok, {next, State_1}} ->
                ok = run(Id, IsDiagnose),
                State_1;
            %% An unxepected error has occured
            {'EXIT', Cause} ->
                ok = finish(Id),
                {_,State_2} = after_execute({error, Cause}, State),
                State_2;
            %% Reached end of the object-container
            {ok, {eof, State_1}} ->
                ok = finish(Id),
                {_,State_2} = after_execute(ok, State_1),
                State_2;
            %% An epected error has occured
            {{error, Cause}, State_1} ->
                ok = finish(Id),
                {_,State_2} = after_execute({error, Cause}, State_1),
                State_2
        end,
    {next_state, NextStatus, State_3#state{status = NextStatus}};

running(#event_info{event = ?EVENT_SUSPEND}, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}};

running(#event_info{event = ?EVENT_FINISH}, #state{obj_storage_id   = ObjStorageId,
                                                   compact_cntl_pid = CntlPid,
                                                   obj_storage_info = #backend_info{file_path = FilePath},
                                                   start_datetime = StartDateTime,
                                                   result = Ret} = State) ->
    EndDateTime = leo_date:now(),
    Duration = EndDateTime - StartDateTime,
    StartDateTime_1 = lists:flatten(leo_date:date_format(StartDateTime)),
    EndDateTime_1   = lists:flatten(leo_date:date_format(EndDateTime)),
    ok = leo_object_storage_server:append_compaction_history(
           ObjStorageId, #compaction_hist{start_datetime = StartDateTime,
                                          end_datetime   = EndDateTime,
                                          duration       = Duration,
                                          result = Ret}),
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "running/2"},
                           {line, ?LINE}, {body, [{file_path, FilePath},
                                                  {start_datetime, StartDateTime_1},
                                                  {end_datetime,   EndDateTime_1},
                                                  {duration, Duration},
                                                  {result, Ret}
                                                 ]}]),
    erlang:send(CntlPid, finish),
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus,
                                         error_pos = 0,
                                         set_errors = sets:new(),
                                         obj_storage_info = #backend_info{},
                                         compaction_prms = #compaction_prms{},
                                         start_datetime = 0,
                                         result = undefined
                                        }};
running(#event_info{event = ?EVENT_STATE,
                    client_pid = Client}, State) ->
    NextStatus = ?ST_RUNNING,
    erlang:send(Client, NextStatus),
    {next_state, NextStatus, State#state{status = NextStatus}};
running(_, State) ->
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


-spec(running( _, _, #state{}) ->
             {next_state, ?ST_SUSPENDING|?ST_RUNNING, #state{}}).
running(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
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
suspending(_, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}}.

-spec(suspending(EventInfo, From, State) ->
             {next_state, ?ST_SUSPENDING | ?ST_RUNNING, State} when EventInfo::#event_info{},
                                                                    From::{pid(),Tag::atom()},
                                                                    State::#state{}).
suspending(#event_info{event = ?EVENT_RESUME}, From, #state{id = Id,
                                                            is_diagnosing = IsDiagnose} = State) ->
    gen_fsm:reply(From, ok),
    ok = run(Id, IsDiagnose),

    NextStatus = ?ST_RUNNING,
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
               waiting_time     = WaitingTime,
               is_diagnosing    = IsDiagnose,
               compaction_prms  = CompactionPrms} = State) ->
    %% Initialize set-error property
    State_1 = State#state{set_errors = sets:new()},

    Offset   = CompactionPrms#compaction_prms.next_offset,
    Metadata = CompactionPrms#compaction_prms.metadata,

    %% Temporally suspend the compaction in order to decrease i/o load
    case (WaitingTime == 0) of
        true  ->
            void;
        false ->
            timer:sleep(WaitingTime)
    end,

    %% Execute compaction
    case (Offset == ?AVS_SUPER_BLOCK_LEN) of
        true ->
            execute_1(State_1);
        false ->
            #compaction_prms{key_bin  = Key,
                             body_bin = Body,
                             callback_fun = CallbackFun,
                             num_of_active_objs  = NumOfActiveObjs,
                             size_of_active_objs = ActiveSize
                            } = CompactionPrms,
            NumOfReplicas   = Metadata#?METADATA.num_of_replicas,
            HasChargeOfNode = case (CallbackFun == undefined) of
                                  true ->
                                      true;
                                  false ->
                                      CallbackFun(Key, NumOfReplicas)
                              end,

            case (is_deleted_rec(MetaDBId, StorageInfo, Metadata)
                  orelse HasChargeOfNode == false) of
                true ->
                    execute_1(State_1);
                %%
                %% For data-compaction processing
                %%
                false when IsDiagnose == false ->
                    %% Insert into the temporary object-container.
                    {Ret_1, NewState} =
                        case leo_object_storage_haystack:put_obj_to_new_cntnr(
                               StorageInfo#backend_info.write_handler, Metadata, Key, Body) of
                            {ok, Offset_1} ->
                                Metadata_1 = Metadata#?METADATA{offset = Offset_1},
                                KeyOfMeta  = ?gen_backend_key(
                                                StorageInfo#backend_info.avs_ver_cur,
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
                false when IsDiagnose == true ->
                    case LoggerId of
                        undefined ->
                            void;
                        _ ->
                            leo_logger_client_base:append(
                              {LoggerId, #message_log{format  = "~s\t~w\t~w\t~s\t~w~n",
                                                      message = [leo_date:date_format(),
                                                                 Metadata#?METADATA.offset,
                                                                 Metadata#?METADATA.addr_id,
                                                                 binary_to_list(Metadata#?METADATA.key),
                                                                 Metadata#?METADATA.dsize
                                                                ]}
                              })
                    end,
                    {Ret_1, NewState} = execute_2(
                                          ok, CompactionPrms, Metadata,
                                          NumOfActiveObjs, ActiveSize, State_1),
                    execute_1(Ret_1, NewState)
            end
    end.


%% @doc Reduce unnecessary objects from object-container.
%% @private
execute_1(State) ->
    execute_1(ok, State).

execute_1(ok, #state{obj_storage_info = StorageInfo,
                     compaction_prms  = CompactionPrms} = State) ->
    erlang:garbage_collect(self()),
    ReadHandler = StorageInfo#backend_info.read_handler,
    NextOffset  = CompactionPrms#compaction_prms.next_offset,

    case leo_object_storage_haystack:get_obj_for_new_cntnr(
           ReadHandler, NextOffset) of
        {ok, NewMetadata, [_HeaderValue, NewKeyValue,
                           NewBodyValue, NewNextOffset]} ->
            ok = output_accumulated_errors(State, NextOffset),
            {ok, {next, State#state{
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
            ok = output_accumulated_errors(State, NextOffset),
            NumOfAcriveObjs  = CompactionPrms#compaction_prms.num_of_active_objs,
            SizeOfActiveObjs = CompactionPrms#compaction_prms.size_of_active_objs,
            {ok, {Cause, State#state{
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
after_execute(Ret, #state{obj_storage_info = StorageInfo} = State) ->
    %% Close file handlers
    ReadHandler  = StorageInfo#backend_info.read_handler,
    WriteHandler = StorageInfo#backend_info.write_handler,
    catch leo_object_storage_haystack:close(WriteHandler, ReadHandler),

    %% Finish compaction
    after_execute_1({Ret, State}).


%% @doc Reduce objects from the object-container.
%% @private
after_execute_1({ok, #state{is_diagnosing = true,
                            compaction_prms =
                                #compaction_prms{
                                   num_of_active_objs  = _NumActiveObjs,
                                   size_of_active_objs = _SizeActiveObjs}} = State}) ->
    %% @TODO
    {ok, State#state{result = ?RET_SUCCESS}};

after_execute_1({ok, #state{meta_db_id       = MetaDBId,
                            obj_storage_id   = ObjStorageId,
                            obj_storage_info = StorageInfo,
                            compaction_prms =
                                #compaction_prms{
                                   num_of_active_objs  = NumActiveObjs,
                                   size_of_active_objs = SizeActiveObjs}} = State}) ->
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

            {ok, State#state{result = ?RET_SUCCESS}};
        {error, Cause} ->
            leo_backend_db_api:finish_compaction(MetaDBId, false),
            {{error, Cause}, State#state{result = ?RET_FAIL}}
    end;

%% @doc Rollback - delete the tmp-files
after_execute_1({_Error, #state{meta_db_id       = MetaDBId,
                                obj_storage_info = StorageInfo} = State}) ->
    %% must reopen the original file when handling at another process:
    catch file:delete(StorageInfo#backend_info.file_path),
    leo_backend_db_api:finish_compaction(MetaDBId, false),
    {ok, State#state{result = ?RET_FAIL}}.


%% @doc Output accumulated errors to logger
%% @private
-spec(output_accumulated_errors(State, ErrorPosEnd) ->
             ok when State::#state{},
                     ErrorPosEnd::non_neg_integer()).
output_accumulated_errors(#state{obj_storage_id = ObjStorageId,
                                 error_pos  = ErrorPosStart,
                                 set_errors = SetErrors}, ErrorPosEnd) ->
    case sets:size(SetErrors) of
        0 ->
            ok;
        _ ->
            {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
            error_logger:warning_msg(
              "~p,~p,~p,~p~n",
              [{module, ?MODULE_STRING},
               {function, "execute_1/4"},
               {line, ?LINE},
               {body, [{obj_container_path, StorageInfo#backend_info.file_path},
                       {error_pos_start, ErrorPosStart},
                       {error_pos_end,   ErrorPosEnd},
                       {errors,          sets:to_list(SetErrors)}]}
              ]),
            ok
    end.


%% @doc Is deleted a record ?
%% @private
-spec(is_deleted_rec(MetaDBId, StorageInfo, Metadata) ->
             boolean() when MetaDBId::atom(),
                            StorageInfo::#backend_info{},
                            Metadata::#?METADATA{}).
is_deleted_rec(_MetaDBId, _StorageInfo, #?METADATA{del = ?DEL_TRUE}) ->
    true;
is_deleted_rec(MetaDBId, #backend_info{avs_ver_prv = AVSVsnBinPrv} = StorageInfo,
               #?METADATA{key      = Key,
                          addr_id  = AddrId} = MetaFromAvs) ->
    KeyOfMetadata = ?gen_backend_key(AVSVsnBinPrv, AddrId, Key),
    case leo_backend_db_api:get(MetaDBId, KeyOfMetadata) of
        {ok, MetaBin} ->
            case binary_to_term(MetaBin) of
                #?METADATA{del = ?DEL_TRUE} ->
                    true;
                Metadata ->
                    is_deleted_rec(MetaDBId, StorageInfo, MetaFromAvs, Metadata)
            end;
        not_found ->
            true;
        _Other ->
            false
    end.

%% @private
-spec(is_deleted_rec(MetaDBId, StorageInfo, Metadata_1, Metadata_2) ->
             boolean() when MetaDBId::atom(),
                            StorageInfo::#backend_info{},
                            Metadata_1::#?METADATA{},
                            Metadata_2::#?METADATA{}).
is_deleted_rec(_MetaDBId,_StorageInfo,
               #?METADATA{offset = Offset_1},
               #?METADATA{offset = Offset_2}) when Offset_1 /= Offset_2 ->
    true;
is_deleted_rec(_MetaDBId,_StorageInfo,_Meta_1,_Meta_2) ->
    false.

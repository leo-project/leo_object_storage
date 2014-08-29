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
%%======================================================================
-module(leo_compact_fsm_worker).

-author('Yosuke Hara').

-behaviour(gen_fsm).

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/4, stop/1]).
-export([run/1, run/2,
         suspend/1,
         resume/1,
         finish/1,
         state/1]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4,
         format_status/2]).

-export([idling/3,
         running/2, running/3,
         suspending/2, suspending/3]).

-compile(nowarn_deprecated_type).

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
          id               :: atom(),
          obj_storage_id   :: atom(),
          meta_db_id       :: atom(),
          obj_storage_info = #backend_info{} :: #backend_info{},
          compact_cntl_pid :: pid(),
          status = ?ST_IDLING :: state_of_compaction(),
          error_pos = 0    :: non_neg_integer(),
          set_errors       :: set(),
          previous_times = [] :: [{non_neg_integer(),boolean()}],
          compaction_prms = #compaction_prms{} :: #compaction_prms{}
         }).

-define(MAX_COMPACT_HISTORIES, 10).
-define(DEF_TIMEOUT, timer:seconds(30)).


%%====================================================================
%% API
%%====================================================================
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
-spec(start_link(atom(), atom(), atom(), function()) ->
             {ok, pid()} | {error, any()}).
start_link(Id, ObjStorageId, MetaDBId, CallbackFun) ->
    gen_fsm:start_link({local, Id}, ?MODULE, [Id, ObjStorageId, MetaDBId, CallbackFun], []).

%% @doc Stop this server
%%
-spec(stop(atom()) -> ok).
stop(Id) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "stop/1"},
                           {line, ?LINE}, {body, Id}]),
    gen_fsm:sync_send_all_state_event(Id, stop, ?DEF_TIMEOUT).


%% @doc Run the process
%%
-spec(run(atom()) ->
             ok | {error, any()}).
run(Id) ->
    gen_fsm:send_event(Id, ?EVENT_RUN).

-spec(run(atom(), pid()) ->
             ok | {error, any()}).
run(Id, ControllerPid) ->
    gen_fsm:sync_send_event(Id, {?EVENT_RUN, ControllerPid}, ?DEF_TIMEOUT).


%% @doc Retrieve an object from the object-storage
%%
-spec(suspend(atom()) ->
             ok | {error, any()}).
suspend(Id) ->
    gen_fsm:send_event(Id, ?EVENT_SUSPEND).


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(resume(atom()) ->
             ok | {error, any()}).
resume(Id) ->
    gen_fsm:sync_send_event(Id, ?EVENT_RESUME, ?DEF_TIMEOUT).


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(finish(atom()) ->
             ok | {error, any()}).
finish(Id) ->
    gen_fsm:send_event(Id, ?EVENT_FINISH).


%% @doc Retrieve the storage stats specfied by Id
%%      which contains number of objects and so on.
%%
-spec(state(atom()) ->
             {ok, #storage_stats{}} |
             {error, any()}).
state(Id) ->
    gen_fsm:sync_send_all_state_event(Id, state, ?DEF_TIMEOUT).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Id, ObjStorageId, MetaDBId, CallbackFun]) ->
    {ok, ?ST_IDLING, #state{id = Id,
                            obj_storage_id = ObjStorageId,
                            meta_db_id     = MetaDBId,
                            compaction_prms = #compaction_prms{
                                                 key_bin  = <<>>,
                                                 body_bin = <<>>,
                                                 metadata = #?METADATA{},
                                                 next_offset = 0,
                                                 num_of_active_objs  = 0,
                                                 size_of_active_objs = 0,
                                                 callback_fun = CallbackFun}
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
%% CALLBACKS
%%====================================================================
%% @doc State of 'idle'
%%
-spec(idling({?EVENT_RUN, pid()} | _, _, #state{}) ->
             {next_state, ?ST_IDLING | ?ST_RUNNING, #state{}}).
idling({?EVENT_RUN, ControllerPid}, From,#state{id = Id,
                                                compaction_prms =
                                                    CompactionPrms} = State) ->
    NextStatus = ?ST_RUNNING,
    State_1 = State#state{compact_cntl_pid = ControllerPid,
                          status     = NextStatus,
                          error_pos  = 0,
                          set_errors = sets:new(),
                          compaction_prms =
                              CompactionPrms#compaction_prms{
                                num_of_active_objs  = 0,
                                size_of_active_objs = 0,
                                next_offset = ?AVS_SUPER_BLOCK_LEN}
                         },
    case prepare(State_1) of
        {ok, State_2} ->
            gen_fsm:reply(From, ok),
            %% @TODO
            %% NewHist = compact_add_history(
            %%             start, StorageStats#storage_stats.compaction_histories),
            ok = run(Id),
            {next_state, NextStatus, State_2};
        {{error, Cause},_State} ->
            gen_fsm:reply(From, {error, Cause}),
            {next_state, ?ST_IDLING, State_1}
    end;
idling(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


-spec(running(?EVENT_RUN, #state{}) ->
             {next_state, ?ST_RUNNING, #state{}}).
running(?EVENT_RUN, #state{id = Id} = State) ->
    NextStatus = ?ST_RUNNING,
    State_3 =
        case catch execute(State) of
            {ok, {next, State_1}} ->
                ok = run(Id),
                State_1;
            {'EXIT', Cause} ->
                ok = finish(Id),
                {_,State_2} = after_execute({error, Cause}, State),
                State_2;
            {ok, {eof, State_1}} ->
                ok = finish(Id),
                {_,State_2} = after_execute(ok, State_1),
                State_2;
            {{error, Cause}, State_1} ->
                ok = finish(Id),
                {_,State_2} = after_execute({error, Cause}, State_1),
                State_2
        end,
    {next_state, NextStatus, State_3#state{status = NextStatus}};

running(?EVENT_SUSPEND, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}};

running(?EVENT_FINISH, #state{compact_cntl_pid = CntlPid}= State) ->
    erlang:send(CntlPid, finish),
    NextStatus = ?ST_IDLING,
    {next_state, NextStatus, State#state{status = NextStatus}};

running(_, State) ->
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


-spec(running( _, _, #state{}) ->
             {next_state, 'suspend' | 'running', #state{}}).
running(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?ST_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


%% @doc State of 'suspend'
%%
suspending(?EVENT_RUN, State) ->
    NextStatus = ?ST_SUSPENDING,
    {next_state, NextStatus, State#state{status = NextStatus}}.

-spec(suspending(?EVENT_RESUME|_, _, #state{}) ->
             {next_state, ?ST_SUSPENDING | ?ST_RUNNING, #state{}}).
suspending(?EVENT_RESUME, From, #state{id = Id} = State) ->
    gen_fsm:reply(From, ok),
    ok = run(Id),

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
-spec(calc_remain_disksize(atom(), string()) ->
             {ok, integer()} | {error, any()}).
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
-spec(prepare(#state{}) ->
             {ok, #state{}} | {{error, any()}, #state{}}).
prepare(#state{obj_storage_id = ObjStorageId,
               meta_db_id = MetaDBId} = State) ->
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


%% @doc Open the current object-container and a new object-container
%% @private
prepare_1(LinkedPath, FilePath, #state{meta_db_id = MetaDBId,
                                       compaction_prms = CompactionPrms
                                      } = State) ->
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

                    case leo_backend_db_api:compact_start(MetaDBId) of
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
-spec(execute(#state{}) ->
             {ok, #state{}} | {{error, any()}, #state{}}).
execute(#state{meta_db_id       = MetaDBId,
               obj_storage_info = StorageInfo,
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
                             callback_fun = FunHasChargeOfNode,
                             num_of_active_objs  = NumOfActiveObjs,
                             size_of_active_objs = SizeActive
                            } = CompactionPrms,
            NumOfReplicas = Metadata#?METADATA.num_of_replicas,
            HasChargeOfNode = FunHasChargeOfNode(Key, NumOfReplicas),

            case (is_deleted_rec(MetaDBId, StorageInfo, Metadata)
                  orelse HasChargeOfNode == false) of
                true ->
                    execute_1(State_1);
                false ->
                    %% Insert into the temporary object-container.
                    {Ret_1, NewState} =
                        case leo_object_storage_haystack:compact_put(
                               StorageInfo#backend_info.write_handler, Metadata, Key, Body) of
                            {ok, Offset_1} ->
                                Metadata_1 = Metadata#?METADATA{offset = Offset_1},
                                KeyOfMeta  = ?gen_backend_key(
                                                StorageInfo#backend_info.avs_ver_cur,
                                                Metadata#?METADATA.addr_id,
                                                Metadata#?METADATA.key),
                                Ret = leo_backend_db_api:compact_put(
                                        MetaDBId, KeyOfMeta, term_to_binary(Metadata_1)),

                                %% Calculate num of objects and total size of objects
                                ObjectSize = leo_object_storage_haystack:calc_obj_size(Metadata_1),
                                {Ret,
                                 State_1#state{compaction_prms =
                                                   CompactionPrms#compaction_prms{
                                                     metadata = Metadata_1,
                                                     num_of_active_objs  = NumOfActiveObjs + 1,
                                                     size_of_active_objs = SizeActive + ObjectSize}}};
                            Error ->
                                {Error,
                                 State_1#state{compaction_prms =
                                                   CompactionPrms#compaction_prms{
                                                     metadata = Metadata}}}
                        end,
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

    case leo_object_storage_haystack:compact_get(
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


%% @private
after_execute(Ret, #state{obj_storage_info = StorageInfo} = State) ->
    %% Close file handlers
    ReadHandler  = StorageInfo#backend_info.read_handler,
    WriteHandler = StorageInfo#backend_info.write_handler,
    catch leo_object_storage_haystack:close(WriteHandler, ReadHandler),

    %% Finish compaction
    %% @TODO add history(end datetime)
    %% NewHist2 = compact_add_history(finish, NewHist),
    after_execute_1({Ret, State}).


%% @doc Reduce objects from the object-container.
%% @private
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
            ok = leo_backend_db_api:compact_end(MetaDBId, true),

            {ok, State#state{obj_storage_info = #backend_info{}}};
        {error, Cause} ->
            leo_backend_db_api:compact_end(MetaDBId, false),
            %% @TODO
            %% NewState = State#state{
            %%              storage_stats = StorageStats#storage_stats{
            %%                                has_error = true}},
            {{error, Cause}, State}
    end;

%% @doc Rollback - delete the tmp-files
after_execute_1({_Error, #state{meta_db_id       = MetaDBId,
                                obj_storage_info = StorageInfo} = State}) ->
    %% @TODO
    %% NewState = State#state{storage_stats = StorageStats#storage_stats{
    %%                                          has_error = true}},

    %% must reopen the original file when handling at another process:
    catch file:delete(StorageInfo#backend_info.file_path),
    leo_backend_db_api:compact_end(MetaDBId, false),
    {ok, State}.


%% @doc Output accumulated errors to logger
%% @private
-spec(output_accumulated_errors(#state{}, integer()) ->
             ok).
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
-spec(is_deleted_rec(atom(), #backend_info{}, #?METADATA{}) ->
             boolean()).
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
-spec(is_deleted_rec(atom(), #backend_info{}, #?METADATA{}, #?METADATA{}) ->
             boolean()).
is_deleted_rec(_MetaDBId,_StorageInfo,
               #?METADATA{offset = Offset_1},
               #?METADATA{offset = Offset_2}) when Offset_1 /= Offset_2 ->
    true;
is_deleted_rec(_MetaDBId,_StorageInfo,_Meta_1,_Meta_2) ->
    false.

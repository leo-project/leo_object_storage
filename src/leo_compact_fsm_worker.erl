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

-export([idle/3,
         running/2, running/3,
         suspend/2, suspend/3]).

-compile(nowarn_deprecated_type).

-record(compaction_prms, {
          key_bin  = <<>> :: binary(),
          body_bin = <<>> :: binary(),
          metadata = #?METADATA{} :: #?METADATA{},
          next_offset = 0 :: integer()|eof,
          callback_fun    :: function(),
          num_of_active_objs = 0  :: integer(),
          size_of_active_objs = 0 :: integer()
         }).

-record(state, {
          id               :: atom(),
          obj_storage_id   :: atom(),
          meta_db_id       :: atom(),
          compact_cntl_pid :: pid(),
          status = ?COMPACTION_STATUS_IDLE :: compaction_status(),
          error_pos = 0    :: non_neg_integer(),
          set_errors       :: set(),
          previous_times = [] :: [{non_neg_integer(),boolean()}],
          compaction_prms = #compaction_prms{} :: #compaction_prms{}
         }).

-define(NUM_OF_COMPACTION_OBJS,    100).
-define(MAX_COMPACT_HISTORIES,      10).
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
    gen_fsm:send_event(Id, run).

-spec(run(atom(), pid()) ->
             ok | {error, any()}).
run(Id, ControllerPid) ->
    gen_fsm:sync_send_event(Id, {run, ControllerPid}, ?DEF_TIMEOUT).


%% @doc Retrieve an object from the object-storage
%%
-spec(suspend(atom()) ->
             ok | {error, any()}).
suspend(Id) ->
    gen_fsm:send_event(Id, suspend).


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(resume(atom()) ->
             ok | {error, any()}).
resume(Id) ->
    gen_fsm:sync_send_event(Id, resume, ?DEF_TIMEOUT).


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(finish(atom()) ->
             ok | {error, any()}).
finish(Id) ->
    gen_fsm:sync_send_event(Id, finish, ?DEF_TIMEOUT).


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
    {ok, idle, #state{id = Id,
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
handle_sync_event(status, _From, StateName, #state{} = State) ->
    {reply, {ok, []}, StateName, State};

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
%%
%%====================================================================
%% @doc State of 'idle'
%%
-spec(idle({'run', pid()} | 'finish' | _, _, #state{}) ->
             {next_state, 'idle' | 'running', #state{}}).
idle({'run', ControllerPid}, From, #state{id = Id,
                                          compaction_prms = CompactionPrms} = State) ->
    NextStatus = ?COMPACTION_STATUS_RUNNING,
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
        ok ->
            gen_fsm:reply(From, ok),
            %% @TODO
            %% NewHist = compact_add_history(
            %%             start, StorageStats#storage_stats.compaction_histories),
            ok = run(Id),
            {next_state, NextStatus, State_1};
        {error, Cause} ->
            gen_fsm:reply(From, {error, Cause}),
            {next_state, ?COMPACTION_STATUS_IDLE, State}
    end;

idle('finish', From, #state{compact_cntl_pid = CntlPid}= State) ->
    gen_fsm:reply(From, ok),

    ok = finish_fun(State),
    erlang:send(CntlPid, finish),
    NextStatus = ?COMPACTION_STATUS_IDLE,
    {next_state, NextStatus, State#state{status = NextStatus}};

idle(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?COMPACTION_STATUS_IDLE,
    {next_state, NextStatus, State#state{status = NextStatus}}.


-spec(running('run', #state{}) ->
             {next_state, 'running', #state{}}).
running('run', #state{id = Id} = State) ->
    {NextStatus, State_3} =
        case catch execute(State) of
            {ok, {next, State_1}} ->
                ok = run(Id),
                {?COMPACTION_STATUS_RUNNING, State_1};
            {'EXIT', Cause} ->
                _ = timer:apply_after(timer:seconds(1),
                                      ?MODULE, finish, [Id]),
                {_,State_2} = after_execute({error, Cause}, State),
                {?COMPACTION_STATUS_IDLE, State_2};
            {ok, {eof, State_1}} ->
                _ = timer:apply_after(timer:seconds(1),
                                      ?MODULE, finish, [Id]),
                {_,State_2} = after_execute(ok, State_1),
                {?COMPACTION_STATUS_IDLE, State_2};
            {{error, Cause}, State_1} ->
                _ = timer:apply_after(timer:seconds(1),
                                      ?MODULE, finish, [Id]),
                {_,State_2} = after_execute({error, Cause}, State_1),
                {?COMPACTION_STATUS_IDLE, State_2}
        end,
    {next_state, NextStatus, State_3#state{status = NextStatus}};

running('suspend', State) ->
    NextStatus = ?COMPACTION_STATUS_SUSPEND,
    {next_state, NextStatus, State#state{status = NextStatus}};

running(_, State) ->
    NextStatus = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}}.



-spec(running({'run', pid()} | _, _, #state{}) ->
             {next_state, 'suspend' | 'running', #state{}}).
running({'run',_Pid}, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}};

running(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}}.


%% @doc State of 'suspend'
%%
suspend('run', State) ->
    NextStatus = ?COMPACTION_STATUS_SUSPEND,
    {next_state, NextStatus, State#state{status = NextStatus}}.

-spec(suspend('resume'|_, _, #state{}) ->
             {next_state, 'suspend' | 'running', #state{}}).
suspend('resume', From, #state{id = Id} = State) ->
    gen_fsm:reply(From, ok),
    ok = run(Id),

    NextStatus = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}};

suspend(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?COMPACTION_STATUS_SUSPEND,
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
             ok | {error, any()}).
prepare(#state{obj_storage_id = ObjStorageId,
               meta_db_id = MetaDBId} = State) ->
    {ok, #backend_info{
            file_path     = FilePath,
            file_path_raw = OrgFilePath}} = ?get_obj_storage_info(ObjStorageId),

    %% Retrieve the remain disk-size
    case calc_remain_disksize(MetaDBId, FilePath) of
        {ok, RemainSize} ->
            case (RemainSize > 0) of
                true ->
                    %% Open the current object-container
                    %% and a new object-container
                    prepare_1(FilePath, OrgFilePath, State);
                false ->
                    {error, system_limit}
            end;
        Error ->
            Error
    end.


%% @doc Open the current object-container and a new object-container
%% @private
prepare_1(FilePath, OrgFilePath, #state{obj_storage_id = ObjStorageId,
                                        meta_db_id = MetaDBId}) ->
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
    TmpPath = ?gen_raw_file_path(FilePath),

    %% must reopen the original file when handling at another process
    case leo_object_storage_haystack:open(TmpPath) of
        {ok, [TmpWriteHandler, TmpReadHandler, AVSVsnBinCur]} ->
            case leo_object_storage_haystack:open(OrgFilePath) of
                {ok, [WriteHandler, ReadHandler, AVSVsnBinPrv]} ->
                    FileSize = filelib:file_size(OrgFilePath),
                    ok = file:advise(TmpWriteHandler, 0, FileSize, dont_need),
                    ok = file:advise(ReadHandler, 0, FileSize, sequential),
                    ok = leo_object_storage_server:set_backend_info(
                           ObjStorageId, ?SERVER_OBJ_STORAGE,
                           StorageInfo#backend_info{
                             avs_version_bin_cur = AVSVsnBinCur,
                             avs_version_bin_prv = AVSVsnBinPrv,
                             tmp_file_path_raw   = TmpPath,
                             write_handler       = WriteHandler,
                             read_handler        = ReadHandler,
                             tmp_write_handler   = TmpWriteHandler,
                             tmp_read_handler    = TmpReadHandler}),
                    %%
                    leo_backend_db_api:compact_start(MetaDBId);
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @doc Reduce unnecessary objects from object-container.
%% @private
-spec(execute(#state{}) ->
             {ok, #state{}} | {{error, any()}, #state{}}).
execute(#state{obj_storage_id  = ObjStorageId,
               meta_db_id      = MetaDBId,
               compaction_prms = CompactionPrms} = State) ->
    %% Initialize set-error property
    State_1 = State#state{set_errors = sets:new()},
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),

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
                               StorageInfo#backend_info.tmp_write_handler,
                               Metadata, Key, Body) of
                            {ok, Offset_1} ->
                                Metadata_1 = Metadata#?METADATA{offset = Offset_1},
                                KeyOfMeta  = ?gen_backend_key(
                                                StorageInfo#backend_info.avs_version_bin_cur,
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

execute_1(ok, #state{obj_storage_id  = ObjStorageId,
                     compaction_prms = CompactionPrms} = State) ->
    erlang:garbage_collect(self()),
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
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
after_execute(Ret, #state{obj_storage_id = ObjStorageId} = State) ->
    %% Close file handlers
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
    ReadHandler     = StorageInfo#backend_info.read_handler,
    WriteHandler    = StorageInfo#backend_info.write_handler,
    TmpReadHandler  = StorageInfo#backend_info.tmp_read_handler,
    TmpWriteHandler = StorageInfo#backend_info.tmp_write_handler,

    catch leo_object_storage_haystack:close(WriteHandler,    ReadHandler),
    catch leo_object_storage_haystack:close(TmpWriteHandler, TmpReadHandler),

    %% Finish compaction
    %% @TODO add history(end datetime)
    %% NewHist2 = compact_add_history(finish, NewHist),
    after_execute_1({Ret, State}).


%% @doc Reduce objects from the object-container.
%% @private
after_execute_1({ok, #state{meta_db_id      = MetaDBId,
                            obj_storage_id  = ObjStorageId,
                            compaction_prms =
                                #compaction_prms{
                                   num_of_active_objs  = NumActiveObjs,
                                   size_of_active_objs = SizeActiveObjs}} = State}) ->
    %% Unlink the symbol
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
    RootPath       = StorageInfo#backend_info.file_path,
    TmpFilePathRaw = StorageInfo#backend_info.tmp_file_path_raw,
    catch file:delete(RootPath),

    %% Retrieve the before storage-state
    {ok, StorageStats} = leo_object_storage_server:get_stats(ObjStorageId),

    %% Migrate the filepath
    case file:make_symlink(TmpFilePathRaw, RootPath) of
        ok ->
            %% Must reopen the original file when handling at another process
            ok = file:delete(StorageInfo#backend_info.file_path_raw),
            ok = leo_backend_db_api:compact_end(MetaDBId, true),
            ok = leo_object_storage_server:set_backend_info(
                   ObjStorageId, ?SERVER_OBJ_STORAGE,
                   StorageInfo#backend_info{file_path_raw = TmpFilePathRaw}),
            ok = leo_object_storage_server:set_backend_info(
                   ObjStorageId, ?SERVER_OBJ_STORAGE,
                   StorageInfo#backend_info{tmp_file_path_raw = TmpFilePathRaw}),

            %% Update the stotrage state
            ok = leo_object_storage_server:set_stats(
                   ObjStorageId, StorageStats#storage_stats{
                                   total_num    = NumActiveObjs,
                                   active_num   = NumActiveObjs,
                                   total_sizes  = SizeActiveObjs,
                                   active_sizes = SizeActiveObjs}),
            ok = leo_object_storage_server:set_backend_info(
                   ObjStorageId, ?SERVER_OBJ_STORAGE,
                   StorageInfo#backend_info{read_handler      = undefined,
                                            write_handler     = undefined,
                                            tmp_read_handler  = undefined,
                                            tmp_write_handler = undefined,
                                            file_path_raw     = TmpFilePathRaw
                                           }),
            {ok, State};
        {error, Cause} ->
            leo_backend_db_api:compact_end(MetaDBId, false),
            %% @TODO
            %% NewState = State#state{
            %%              storage_stats = StorageStats#storage_stats{
            %%                                has_error = true}},
            {{error, Cause}, State}
    end;

%% @doc Rollback - delete the tmp-files
after_execute_1({_Error, #state{meta_db_id     = MetaDBId,
                                obj_storage_id = ObjStorageId} = State}) ->
    %% @TODO
    %% NewState = State#state{storage_stats = StorageStats#storage_stats{
    %%                                          has_error = true}},

    %% must reopen the original file when handling at another process:
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
    catch file:delete(StorageInfo#backend_info.tmp_file_path_raw),
    leo_backend_db_api:compact_end(MetaDBId, false),
    {ok, State}.


%% @doc Finish the data-compaction
%% @private
-spec(finish_fun(#state{}) ->
             ok).
finish_fun(#state{obj_storage_id = ObjStorageId}) ->
    %% Close the old object-container
    {ok, #backend_info{
            read_handler  = ReadHandler,
            write_handler = WriteHandler
           } = StorageInfo} = ?get_obj_storage_info(ObjStorageId),
    ok = leo_object_storage_haystack:close(WriteHandler, ReadHandler),

    %% Open the new object-container
    timer:sleep(timer:seconds(1)),
    ok = leo_object_storage_server:set_backend_info(
           ObjStorageId, ?SERVER_OBJ_STORAGE, StorageInfo),
    ok = leo_object_storage_server:open(ObjStorageId),
    ok.


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
               {body, [{obj_container_path, StorageInfo#backend_info.file_path_raw},
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
is_deleted_rec(MetaDBId, #backend_info{avs_version_bin_prv = AVSVsnBinPrv} = StorageInfo,
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

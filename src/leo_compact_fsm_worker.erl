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
%% ---------------------------------------------------------------------
%% Leo Object Storage - Server
%% @doc
%% @end
%%======================================================================
-module(leo_compact_fsm_worker).

-author('Yosuke Hara').

-behaviour(gen_fsm).

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/4, stop/1]).
-export([run/2,
         suspend/1,
         resume/1,
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
         running/3,
         suspend/3]).

-compile(nowarn_deprecated_type).

-record(state, {
          id               :: atom(),
          obj_storage_id   :: atom(),
          meta_db_id       :: atom(),
          compact_pid      :: pid(),
          compact_cntl_pid :: pid(),
          status = ?COMPACTION_STATUS_IDLE :: compaction_status(),
          callback_fun     :: function(),
          error_pos = 0    :: non_neg_integer(),
          set_errors       :: set(),
          previous_times = [] :: [{non_neg_integer(),boolean()}]
         }).

-record(compact_params, {
          key_bin                :: binary(),
          body_bin               :: binary(),
          next_offset            :: integer(),
          fun_has_charge_of_node :: function(),
          num_of_active_objects  :: integer(),
          size_of_active_object  :: integer()
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
-spec(run(atom(), pid()) ->
             ok | {error, any()}).
run(Id, ControllerPid) ->
    gen_fsm:sync_send_event(Id, {run, ControllerPid}, ?DEF_TIMEOUT).


%% @doc Retrieve an object from the object-storage
%%
-spec(suspend(atom()) ->
             ok | {error, any()}).
suspend(Id) ->
    gen_fsm:sync_send_event(Id, suspend, ?DEF_TIMEOUT).


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
                      callback_fun = CallbackFun}}.

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
-spec(idle({'run', pid()} | 'suspend' | 'resume', _, #state{}) ->
             {next_state, 'idle' | 'running', #state{}}).
idle({run, ControllerPid}, From, State) ->
    NextStatus = ?COMPACTION_STATUS_RUNNING,
    State_1 = State#state{compact_cntl_pid = ControllerPid,
                          status = NextStatus},
    {ok, State_2} = execute(State_1),

    gen_fsm:reply(From, ok),
    {next_state, NextStatus, State_2};

idle(suspend, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?COMPACTION_STATUS_IDLE,
    {next_state, NextStatus, State#state{status = NextStatus}};

idle(resume, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?COMPACTION_STATUS_IDLE,
    {next_state, NextStatus, State#state{status = NextStatus}}.


-spec(running({'run', pid()} | 'suspend' | 'resume', _, #state{}) ->
             {next_state, 'suspend' | 'running', #state{}}).
running({run,_Pid}, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}};

running(suspend, From, #state{id = Id,
                              compact_pid = Pid} = State) ->
    erlang:send(Pid, {Id, 'suspend'}),
    gen_fsm:reply(From, ok),
    NextStatus = ?COMPACTION_STATUS_SUSPEND,
    {next_state, NextStatus, State#state{status = NextStatus}};

running(resume, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextStatus, State#state{status = NextStatus}};

running(finish, From, #state{compact_cntl_pid = CntlPid}= State) ->
    gen_fsm:reply(From, ok),
    NextStatus = ?COMPACTION_STATUS_IDLE,
    erlang:send(CntlPid, done),
    {next_state, NextStatus, State#state{status = NextStatus}}.


%% @doc State of 'suspend'
%%
-spec(suspend('suspend'|_, _, #state{}) ->
             {next_state, 'suspend' | 'running', #state{}}).
suspend(resume, From, #state{id = Id,
                             compact_pid = Pid} = State) ->
    gen_fsm:reply(From, ok),
    erlang:send(Pid, {Id, 'resume'}),
    NextStatus = ?COMPACTION_STATUS_RUNNING,
    {next_state, NextStatus, State};

suspend(_, From, State) ->
    gen_fsm:reply(From, {error, badstate}),
    NextStatus = ?COMPACTION_STATUS_SUSPEND,
    {next_state, NextStatus, State#state{status = NextStatus}}.


%%--------------------------------------------------------------------
%% Inner Functions
%%--------------------------------------------------------------------
%% @private
-spec(execute(#state{}) ->
             {ok, #state{}}).
execute(#state{id = Id} = State) ->
    Pid = spawn_link(fun() ->
                             loop(State)
                     end),
    erlang:send(Pid, {Id, run, Pid}),
    {ok, State#state{compact_pid = Pid}}.


%% @private
-spec(loop(#state{}) ->
             ok | {error, any()}).
loop(#state{id = Id} = State) ->
    receive
        {Id, run, CompactPid} ->
            {ok, _State} = compact_fun(State#state{compact_pid = CompactPid}),
            loop(State);
        {Id, suspend} ->
            loop(State);
        {Id, resume} ->
            {ok, _} = compact_fun(State),
            loop(State);
        {_, done} ->
            loop(State);
        {Id, finish} ->
            finish(Id),
            ok;
        _ ->
            {error, unknown_message}
    end.


%% @doc compact objects from the object-container on a independent process.
%% @private
-spec(compact_fun(#state{}) ->
             {ok, #state{}}).
compact_fun(State) ->
    #state{obj_storage_id = OBjStorageId,
           meta_db_id = MetaDBId} = State,
    {ok, #backend_info{file_path = FilePath,
                       file_path_raw = OrgFilePath}} =
        leo_object_storage_server:get_info(OBjStorageId, ?SERVER_OBJ_STORAGE),

    Res = case calc_remain_disksize(MetaDBId, FilePath) of
              {ok, RemainSize} ->
                  case (RemainSize > 0) of
                      true ->
                          compact_fun_1(FilePath, OrgFilePath, State);
                      false ->
                          {error, system_limit}
                  end;
              Error ->
                  Error
          end,

    State_2 = try compact_fun_2(Res, State) of
                  {_, #state{} = State_1} ->
                      State_1
              catch
                  _:Reason ->
                      error_logger:error_msg(
                        "~p,~p,~p,~p~n",
                        [{module, ?MODULE_STRING},
                         {function, "compact_fun/2"},
                         {line, ?LINE},
                         {body, {MetaDBId, Reason}}]),
                      State
              end,
    finish_compaction(State_2).


%% @private
compact_fun_1(FilePath, OrgFilePath, #state{obj_storage_id = ObjStorageId}) ->
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
                    ok = leo_object_storage_server:set_info(
                           ObjStorageId, ?SERVER_OBJ_STORAGE,
                           StorageInfo#backend_info{
                             avs_version_bin_cur = AVSVsnBinCur,
                             avs_version_bin_prv = AVSVsnBinPrv,
                             tmp_file_path_raw   = TmpPath,
                             write_handler       = WriteHandler,
                             read_handler        = ReadHandler,
                             tmp_write_handler   = TmpWriteHandler,
                             tmp_read_handler    = TmpReadHandler}),
                    ok;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @doc Reduce objects from the object-container.
%% @private
compact_fun_2(ok, #state{meta_db_id = MetaDBId,
                         obj_storage_id = ObjStorageId,
                         %% storage_stats  = StorageStats,
                         callback_fun = FunHasChargeOfNode} = State) ->
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
    ReadHandler     = StorageInfo#backend_info.read_handler,
    WriteHandler    = StorageInfo#backend_info.write_handler,
    TmpReadHandler  = StorageInfo#backend_info.tmp_read_handler,
    TmpWriteHandler = StorageInfo#backend_info.tmp_write_handler,

    %% Start compaction
    %% @TODO
    %% NewHist = compact_add_history(
    %%             start, StorageStats#storage_stats.compaction_histories),
    Res = case leo_backend_db_api:compact_start(MetaDBId) of
              ok ->
                  try do_compact(#metadata{}, #compact_params{
                                                 next_offset = ?AVS_SUPER_BLOCK_LEN,
                                                 num_of_active_objects = 0,
                                                 size_of_active_object = 0,
                                                 fun_has_charge_of_node = FunHasChargeOfNode},
                                 State) of
                      Ret ->
                          Ret
                  catch
                      _:Reason ->
                          error_logger:error_msg("~p,~p,~p,~p~n",
                                                 [{module, ?MODULE_STRING},
                                                  {function, "compact_fun/2"},
                                                  {line, ?LINE},
                                                  {body, {MetaDBId, Reason}}]),
                          {error, Reason}
                  end;
              Error ->
                  Error
          end,

    %% Close file handlers
    catch leo_object_storage_haystack:close(WriteHandler,    ReadHandler),
    catch leo_object_storage_haystack:close(TmpWriteHandler, TmpReadHandler),

    %% Finish compaction
    %% @TODO add history(end datetime)
    %% NewHist2 = compact_add_history(finish, NewHist),
    %% NewState = State#state{storage_stats  = StorageStats#storage_stats{
    %%                                           compaction_histories = NewHist2},
    %%                        object_storage = StorageInfo#backend_info{
    %%                                           read_handler      = undefined,
    %%                                           write_handler     = undefined,
    %%                                           tmp_read_handler  = undefined,
    %%                                           tmp_write_handler = undefined}},
    NewState = State,
    compact_fun_3({Res, NewState});

compact_fun_2(Error,_State) ->
    {Error,_State}.


%% @doc Reduce objects from the object-container.
%% @private
compact_fun_3({{ok,_NumActive,_SizeActive},
               #state{meta_db_id     = MetaDBId,
                      obj_storage_id = ObjStorageId} = State}) ->
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
    RootPath       = StorageInfo#backend_info.file_path,
    TmpFilePathRaw = StorageInfo#backend_info.tmp_file_path_raw,

    catch file:delete(RootPath),
    case file:make_symlink(TmpFilePathRaw, RootPath) of
        ok ->
            ok = file:delete(StorageInfo#backend_info.file_path_raw),

            %% must reopen the original file when handling at another process
            _ = leo_backend_db_api:compact_end(MetaDBId, true),
            ok = leo_object_storage_server:set_info(ObjStorageId, ?SERVER_OBJ_STORAGE,
                                                    StorageInfo#backend_info{
                                                      file_path_raw = TmpFilePathRaw
                                                     }),
            ok = leo_object_storage_server:set_info(
                   ObjStorageId, ?SERVER_OBJ_STORAGE,
                   StorageInfo#backend_info{
                     tmp_file_path_raw = TmpFilePathRaw}),
            %% @TODO
            %% NewState    = State#state{storage_stats =
            %%                               StorageStats#storage_stats{
            %%                                 total_num    = NumActive,
            %%                                 active_num   = NumActive,
            %%                                 total_sizes  = SizeActive,
            %%                                 active_sizes = SizeActive},
            %%                           object_storage =
            %%                               BackendInfo#backend_info{
            %%                                 file_path_raw = TmpFilePathRaw
            %%                                }},
            NewState = State,
            {ok, NewState};
        {error, Cause} ->
            leo_backend_db_api:compact_end(MetaDBId, false),
            %% @TODO
            %% NewState = State#state{
            %%              storage_stats = StorageStats#storage_stats{
            %%                                has_error = true}},
            NewState = State,
            {{error, Cause}, NewState}
    end;

%% @doc Rollback - delete the tmp-files
compact_fun_3({_Error, #state{obj_storage_id = ObjStorageId,
                              meta_db_id     = MetaDBId
                              %% storage_stats  = StorageStats,
                              %% object_storage = StorageInfo
                             } = State}) ->
    %% @TODO
    %% NewState = State#state{storage_stats = StorageStats#storage_stats{
    %%                                          has_error = true}},
    NewState = State,

    %% must reopen the original file when handling at another process:
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
    catch file:delete(StorageInfo#backend_info.tmp_file_path_raw),
    leo_backend_db_api:compact_end(MetaDBId, false),
    {ok, NewState}.


%% @doc
%% @private
-spec(finish_compaction(#state{}) ->
             {ok, #state{}}).
finish_compaction(#state{id = Id,
                         obj_storage_id = ObjStorageId,
                         compact_pid = Pid} = State) ->
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
    FilePath     = StorageInfo#backend_info.file_path,
    ReadHandler  = StorageInfo#backend_info.read_handler,
    WriteHandler = StorageInfo#backend_info.write_handler,
    ok = leo_object_storage_haystack:close(WriteHandler, ReadHandler),

    case leo_object_storage_haystack:open(FilePath) of
        {ok, [NewWriteHandler, NewReadHandler, AVSVsnBin]} ->
            ok = leo_object_storage_server:set_info(
                   ObjStorageId, ?SERVER_OBJ_STORAGE,
                   StorageInfo#backend_info{
                     avs_version_bin_cur = AVSVsnBin,
                     write_handler       = NewWriteHandler,
                     read_handler        = NewReadHandler});
        {error, _} ->
            void
    end,
    erlang:send(Pid, {Id, finish}),
    {ok, State#state{compact_pid = undefined}}.


%% @doc Reduce unnecessary objects from object-container.
%% @private
-spec(do_compact(#?METADATA{}, #compact_params{}, #state{}) ->
             ok | {error, any()}).
do_compact(Metadata, CompactParams, #state{obj_storage_id = ObjStorageId,
                                           meta_db_id     = MetaDBId} = State) ->
    %% initialize set-error property
    State_1 = State#state{set_errors = sets:new()},
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),

    %% execute compaction
    case (CompactParams#compact_params.next_offset == ?AVS_SUPER_BLOCK_LEN) of
        true ->
            do_compact_1(Metadata, CompactParams, State_1);
        false ->
            %% retrieve value
            #compact_params{key_bin  = Key,
                            body_bin = Body,
                            fun_has_charge_of_node = FunHasChargeOfNode,
                            num_of_active_objects  = NumOfActiveObjs,
                            size_of_active_object  = SizeActive
                           } = CompactParams,

            %% set a flag of object of compaction
            NumOfReplicas = Metadata#?METADATA.num_of_replicas,
            HasChargeOfNode = FunHasChargeOfNode(Key, NumOfReplicas),

            %% execute compaction
            case (is_deleted_rec(MetaDBId, StorageInfo, Metadata)
                  orelse HasChargeOfNode == false) of
                true ->
                    do_compact_1(Metadata, CompactParams, State_1);
                false ->
                    %% insert into the temporary object-container.
                    {Ret_1, NewMetadata, NewCompactParams} =
                        case leo_object_storage_haystack:compact_put(
                               StorageInfo#backend_info.tmp_write_handler,
                               Metadata, Key, Body) of
                            {ok, Offset} ->
                                Metadata_1 = Metadata#?METADATA{offset = Offset},
                                KeyOfMeta  = ?gen_backend_key(
                                                StorageInfo#backend_info.avs_version_bin_cur,
                                                Metadata#?METADATA.addr_id,
                                                Metadata#?METADATA.key),
                                Ret = leo_backend_db_api:compact_put(
                                        MetaDBId, KeyOfMeta, term_to_binary(Metadata_1)),

                                %% calculate num of objects and total size of objects
                                ObjectSize = leo_object_storage_haystack:calc_obj_size(Metadata_1),
                                {Ret, Metadata_1,
                                 CompactParams#compact_params{
                                   num_of_active_objects = NumOfActiveObjs + 1,
                                   size_of_active_object = SizeActive + ObjectSize}};
                            Error ->
                                {Error, Metadata, CompactParams}
                        end,
                    do_compact_1(Ret_1, NewMetadata, NewCompactParams, State_1)
            end
    end.


%% @doc Reduce unnecessary objects from object-container.
%% @private
do_compact_1(Metadata, CompactParams, State) ->
    do_compact_1(ok, Metadata, CompactParams, State).

do_compact_1(ok, Metadata, CompactParams,
             #state{obj_storage_id = ObjStorageId} = State) ->
    erlang:garbage_collect(self()),
    {ok, StorageInfo} = ?get_obj_storage_info(ObjStorageId),
    ReadHandler = StorageInfo#backend_info.read_handler,
    NextOffset  = CompactParams#compact_params.next_offset,

    case leo_object_storage_haystack:compact_get(
           ReadHandler, NextOffset) of
        {ok, NewMetadata, [_HeaderValue, NewKeyValue,
                           NewBodyValue, NewNextOffset]} ->
            ok = output_accumulated_errors(State, NextOffset),
            do_compact(NewMetadata,
                       CompactParams#compact_params{
                         key_bin     = NewKeyValue,
                         body_bin    = NewBodyValue,
                         next_offset = NewNextOffset},
                       State#state{error_pos  = 0,
                                   set_errors = sets:new()});
        {error, eof} ->
            ok = output_accumulated_errors(State, NextOffset),
            NumOfAcriveObjs  = CompactParams#compact_params.num_of_active_objects,
            SizeOfActiveObjs = CompactParams#compact_params.size_of_active_object,
            {ok, NumOfAcriveObjs, SizeOfActiveObjs};
        {_, Cause} ->
            ErrorPosCur = State#state.error_pos,
            ErrorPosNew = case (State#state.error_pos == 0) of
                              true ->
                                  NextOffset;
                              false ->
                                  ErrorPosCur
                          end,
            SetErrors = sets:add_element(Cause, State#state.set_errors),
            do_compact_1(ok, Metadata,
                         CompactParams#compact_params{next_offset = NextOffset + 1},
                         State#state{error_pos  = ErrorPosNew,
                                     set_errors = SetErrors})
    end;
do_compact_1(Error,_,_,_) ->
    Error.


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
               {function, "do_compact_1/4"},
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

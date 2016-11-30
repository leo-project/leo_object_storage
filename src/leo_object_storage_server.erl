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
%% ---------------------------------------------------------------------
%% Leo Object Storage - Server
%%
%% @doc The object storage server
%% @reference https://github.com/leo-project/leo_object_storage/blob/master/src/leo_object_storage_server.erl
%% @end
%%======================================================================
-module(leo_object_storage_server).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-behaviour(gen_server).

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1,
         stop/1]).
-export([put/2, get/5, delete/2, head/2, fetch/4, store/3,
         get_stats/1, set_stats/2,
         get_avs_version_bin/1,
         head_with_calc_md5/3,
         close/1,
         get_backend_info/2,
         lock/1, block_del/1, unlock/1,
         switch_container/4,
         append_compaction_history/2,
         get_compaction_worker/1,
         get_eof_offset/1
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-compile(nowarn_deprecated_type).

-ifdef(TEST).
-export([add_incorrect_data/2]).
-endif.

-define(DEF_TIMEOUT, timer:seconds(30)).

-ifdef(TEST).
-define(add_incorrect_data(_StorageInfo,_Bin),
        leo_object_storage_haystack:add_incorrect_data(_StorageInfo,_Bin)).
-else.
-define(add_incorrect_data(_StorageInfo,_Bin), ok).
-endif.


%%====================================================================
%% API
%%====================================================================
%% @doc Starts the server with strict-check
%%
-spec(start_link(ObjServerState) ->
             {ok, pid()} | {error, any()} when ObjServerState::#obj_server_state{}).
start_link(#obj_server_state{id = Id} = ObjServerState) ->
    gen_server:start_link({local, Id}, ?MODULE, [ObjServerState], []).


%% @doc Stop this server
%%
-spec(stop(Id) ->
             ok when Id::atom()).
stop(Id) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "stop/1"},
                           {line, ?LINE}, {body, Id}]),
    gen_server:call(Id, stop, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% API - object operations.
%%--------------------------------------------------------------------
%% @doc Insert an object and an object's metadata into the object-storage
%%
-spec(put(Id, Object) ->
             ok | {error, any()} when Id::atom(),
                                      Object::#?OBJECT{}).
put(Id, #?OBJECT{del = ?DEL_FALSE} = Object) ->
    gen_server:call(Id, {put, Object,
                         ?begin_statistics_wallclock()}, ?DEF_TIMEOUT);
put(Id, #?OBJECT{del = ?DEL_TRUE} = Object) ->
    gen_server:call(Id, {delete, Object,
                         ?begin_statistics_wallclock()}, ?DEF_TIMEOUT).


%% @doc Retrieve an object from the object-storage
%%
-spec(get(Id, AddrIdAndKey, StartPos, EndPos, IsForcedCheck) ->
             {ok, #?METADATA{}, #?OBJECT{}} |
             not_found |
             {error, any()} when Id::atom(),
                                 AddrIdAndKey::addrid_and_key(),
                                 StartPos::non_neg_integer(),
                                 EndPos::non_neg_integer(),
                                 IsForcedCheck::boolean()).
get(Id, AddrIdAndKey, StartPos, EndPos, IsForcedCheck) ->
    gen_server:call(Id, {get, AddrIdAndKey, StartPos, EndPos,
                         IsForcedCheck, ?begin_statistics_wallclock()}, ?DEF_TIMEOUT).


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(delete(Id, Object) ->
             ok | {error, any()} when Id::atom(),
                                      Object::#?OBJECT{}).
delete(Id, Object) ->
    gen_server:call(Id, {delete, Object,
                         ?begin_statistics_wallclock()}, ?DEF_TIMEOUT).


%% @doc Retrieve an object's metadata from the object-storage
%%
-spec(head(Id, AddrIdAndKey) ->
             {ok, binary()} |
             not_found |
             {error, any()} when Id::atom(),
                                 AddrIdAndKey::addrid_and_key()).
head(Id, Key) ->
    gen_server:call(Id, {head, Key,
                         ?begin_statistics_wallclock()}, ?DEF_TIMEOUT).


%% @doc Retrieve objects from the object-storage by Key and Function
%%
-spec(fetch(Id, Key, Fun, MaxKeys) ->
             {ok, list()} | {error, any()} when Id::atom(),
                                                Key::any(),
                                                Fun::function(),
                                                MaxKeys::non_neg_integer()|undefined).
fetch(Id, Key, Fun, MaxKeys) ->
    gen_server:call(Id, {fetch, Key, Fun, MaxKeys,
                         ?begin_statistics_wallclock()}, ?DEF_TIMEOUT).


%% @doc Store metadata and data
%%
-spec(store(Id, Metadata, Bin) ->
             ok | {error, any()} when Id::atom(),
                                      Metadata::#?METADATA{},
                                      Bin::binary()).
store(Id, Metadata, Bin) ->
    gen_server:call(Id, {store, Metadata, Bin,
                         ?begin_statistics_wallclock()}, ?DEF_TIMEOUT).


%% @doc Retrieve the storage stats specfied by Id
%%      which contains number of objects and so on.
%%
-spec(get_stats(Id) ->
             {ok, #storage_stats{}} |
             {error, any()} when Id::atom()).
get_stats(Id) ->
    gen_server:call(Id, get_stats, ?DEF_TIMEOUT).


%% @doc Retrieve the storage stats specfied by Id
%%      which contains number of objects and so on.
%%
-spec(set_stats(Id, StorageStats) ->
             ok when Id::atom(),
                     StorageStats::#storage_stats{}).
set_stats(Id, StorageStats) ->
    gen_server:call(Id, {set_stats, StorageStats}, ?DEF_TIMEOUT).


%% @doc Get AVS format version binary like "LeoFS AVS-2.2"
-spec(get_avs_version_bin(Id) ->
             ok when Id::atom()).
get_avs_version_bin(Id) ->
    gen_server:call(Id, get_avs_version_bin, ?DEF_TIMEOUT).


%% @doc Retrieve a metada/data from backend_db/object-storage
%%      AND calc MD5 based on the body data
%%
-spec(head_with_calc_md5(Id, Key, MD5Context) ->
             {ok, #?METADATA{}, any()} | {error, any()} when Id::atom(),
                                                             Key::tuple(),
                                                             MD5Context::any()).
head_with_calc_md5(Id, Key, MD5Context) ->
    gen_server:call(Id, {head_with_calc_md5, Key, MD5Context,
                         ?begin_statistics_wallclock()}, ?DEF_TIMEOUT).


%% @doc Close the object-container
%%
-spec(close(Id) ->
             ok when Id::atom()).
close(Id) ->
    gen_server:call(Id, close, ?DEF_TIMEOUT).


%% @doc Retrieve object-storage/metadata-storage info
%%
-spec(get_backend_info(Id, ServerType) ->
             {ok, #backend_info{}} when Id::atom(),
                                        ServerType::?SERVER_OBJ_STORAGE).
get_backend_info(Id, ServerType) ->
    gen_server:call(Id, {get_backend_info, ServerType}, ?DEF_TIMEOUT).


%% @doc Lock handling objects for put/delete/store
%%
-spec(lock(Id) ->
             ok when Id::atom()).
lock(undefined) ->
    ok;
lock(Id) ->
    gen_server:call(Id, lock, ?DEF_TIMEOUT).


%% @doc Lock handling objects for delete
%%
-spec(block_del(Id) ->
             ok when Id::atom()).
block_del(undefined) ->
    ok;
block_del(Id) ->
    gen_server:call(Id, block_del, ?DEF_TIMEOUT).


%% @doc Unlock handling objects for put/delete/store
%%
-spec(unlock(Id) ->
             ok when Id::atom()).
unlock(undefined) ->
    ok;
unlock(Id) ->
    gen_server:call(Id, unlock, ?DEF_TIMEOUT).


%% @doc Open the object-container
%%
-spec(switch_container(Id, FilePath, NumOfActiveObjs, SizeOfActiveObjs) ->
             ok when Id::atom(),
                     FilePath::string(),
                     NumOfActiveObjs::non_neg_integer(),
                     SizeOfActiveObjs::non_neg_integer()).
switch_container(Id, FilePath, NumOfActiveObjs, SizeOfActiveObjs) ->
    gen_server:call(Id, {switch_container, FilePath,
                         NumOfActiveObjs, SizeOfActiveObjs}, infinity).


%% @doc Append the history in the state
%%
-spec(append_compaction_history(Id, History) ->
             ok when Id::atom(),
                     History::tuple()).
append_compaction_history(Id, History) ->
    gen_server:call(Id, {append_compaction_history, History}, ?DEF_TIMEOUT).


%% @doc Retrieve the compaction worker
%%
-spec(get_compaction_worker(Id) ->
             {ok, CompactionWorkerId} when Id::atom(),
                                           CompactionWorkerId::atom()).
get_compaction_worker(Id) ->
    gen_server:call(Id, get_compaction_worker, ?DEF_TIMEOUT).

%% @doc Get the EOF offset
%%
-spec(get_eof_offset(Id) ->
             {ok, Offset} when Id::atom(),
                               Offset::non_neg_integer()).
get_eof_offset(Id) ->
    gen_server:call(Id, get_eof_offset, ?DEF_TIMEOUT).


-ifdef(TEST).
%% @doc Store metadata and data
%%
-spec(add_incorrect_data(atom(), binary()) ->
             ok | {error, any()}).
add_incorrect_data(Id, Bin) ->
    gen_server:call(Id, {add_incorrect_data, Bin}, ?DEF_TIMEOUT).
-endif.


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% @doc Initiates the server
init([ObjServerState]) ->
    #obj_server_state{id = Id,
                      seq_num = SeqNo,
                      privilege = Privilege,
                      diagnosis_logger_id = DiagnosisLogId,
                      root_path = RootPath} = ObjServerState,
    ObjectStorageDir = lists:append([RootPath, ?DEF_OBJECT_STORAGE_SUB_DIR]),
    ObjectStoragePath = lists:append([ObjectStorageDir, integer_to_list(SeqNo), ?AVS_FILE_EXT]),
    StateFilePath = lists:append([RootPath, ?DEF_STATE_SUB_DIR, atom_to_list(Id)]),
    LogFilePath = lists:append([RootPath, ?DEF_LOG_SUB_DIR]),
    StorageStats =
        case file:consult(StateFilePath) of
            {ok, Props} ->
                #storage_stats{
                   file_path = ObjectStoragePath,
                   total_sizes = leo_misc:get_value('total_sizes', Props, 0),
                   active_sizes = leo_misc:get_value('active_sizes', Props, 0),
                   total_num = leo_misc:get_value('total_num', Props, 0),
                   active_num = leo_misc:get_value('active_num', Props, 0),
                   compaction_hist = leo_misc:get_value('compaction_hist', Props, [])
                  };
            _ ->
                #storage_stats{file_path = ObjectStoragePath}
        end,

    %% open object-storage.
    case get_raw_path(object, ObjectStorageDir, ObjectStoragePath) of
        {ok, ObjectStorageRawPath} ->
            case leo_object_storage_haystack:open(
                   ObjectStorageRawPath, Privilege) of
                {ok, [ObjectWriteHandler, ObjectReadHandler, AVSVsnBin]} ->
                    StorageInfo = #backend_info{
                                     linked_path = ObjectStoragePath,
                                     file_path = ObjectStorageRawPath,
                                     write_handler = ObjectWriteHandler,
                                     read_handler = ObjectReadHandler,
                                     avs_ver_cur = AVSVsnBin},

                    %% Launch the diagnosis logger
                    case (Privilege == ?OBJ_PRV_READ_WRITE andalso
                          ?env_enable_diagnosis_log()) of
                        true ->
                            _ = filelib:ensure_dir(LogFilePath),
                            ok = leo_logger_client_base:new(
                                   ?LOG_GROUP_ID_DIAGNOSIS,
                                   DiagnosisLogId,
                                   LogFilePath,
                                   ?LOG_FILENAME_DIAGNOSIS ++ integer_to_list(SeqNo));
                        _ ->
                            void
                    end,
                    {ok, ObjServerState#obj_server_state{
                           object_storage = StorageInfo,
                           storage_stats = StorageStats,
                           state_filepath = StateFilePath,
                           is_locked = false,
                           threshold_slow_processing = ?env_threshold_slow_processing()
                          }};
                {error, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "init/4"},
                                            {line, ?LINE},
                                            {body, Cause}]),
                    {stop, Cause}
            end;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "init/4"},
                                    {line, ?LINE},
                                    {body, Cause}]),
            {stop, Cause}
    end.


%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State};


%% Insert an object
handle_call({put,_,_}, _From, #obj_server_state{is_locked = true} = State) ->
    {reply, {error, ?ERROR_LOCKED_CONTAINER}, State};
handle_call({put,_,_}, _From, #obj_server_state{privilege = ?OBJ_PRV_READ_ONLY} = State) ->
    {reply, {error, ?ERROR_NOT_ALLOWED_ACCESS}, State};
handle_call({put = Method, #?OBJECT{addr_id = AddrId,
                                    key = Key} = Object, InTime}, _From,
            #obj_server_state{object_storage = StorageInfo} = State) ->
    Key_1 = ?gen_backend_key(StorageInfo#backend_info.avs_ver_cur,
                             AddrId, Key),
    {Reply, State_1} = put_1(Key_1, Object, State),

    erlang:garbage_collect(self()),
    reply(Method, Key, Reply, InTime, State_1);

%% Retrieve an object
handle_call({get = Method, {AddrId, Key}, StartPos, EndPos, IsForcedCheck, InTime},
            _From, #obj_server_state{meta_db_id      = MetaDBId,
                                     object_storage  = StorageInfo,
                                     is_strict_check = IsStrictCheck} = State) ->
    IsStrictCheck_1 = case IsForcedCheck of
                          true  ->
                              IsForcedCheck;
                          false ->
                              IsStrictCheck
                      end,
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_ver_cur, AddrId, Key),
    Reply = leo_object_storage_haystack:get(
              MetaDBId, StorageInfo, BackendKey, StartPos, EndPos, IsStrictCheck_1),

    State_1 = after_proc(Reply, State),
    erlang:garbage_collect(self()),
    reply(Method, Key, Reply, InTime, State_1);


%% Remove an object
handle_call({delete,_,_}, _From, #obj_server_state{is_locked = true} = State) ->
    {reply, {error, ?ERROR_LOCKED_CONTAINER}, State};
handle_call({delete,_,_}, _From, #obj_server_state{is_del_blocked = true} = State) ->
    {reply, {error, ?ERROR_LOCKED_CONTAINER}, State};
handle_call({delete,_,_}, _From, #obj_server_state{privilege = ?OBJ_PRV_READ_ONLY} = State) ->
    {reply, {error, ?ERROR_NOT_ALLOWED_ACCESS}, State};
handle_call({delete = Method, #?OBJECT{addr_id = AddrId,
                                       key = Key} = Object, InTime}, _From,
            #obj_server_state{object_storage = StorageInfo} = State) ->
    Key_1 = ?gen_backend_key(StorageInfo#backend_info.avs_ver_cur, AddrId, Key),
    {Reply, State_1} = delete_1(Key_1, Object, State),
    reply(Method, Key, Reply, InTime, State_1);


%% Head an object
handle_call({head = Method, {AddrId, Key}, InTime},
            _From, #obj_server_state{meta_db_id = MetaDBId,
                                     object_storage = StorageInfo} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_ver_cur,
                                  AddrId, Key),
    Reply = leo_object_storage_haystack:head(MetaDBId, StorageInfo, BackendKey),
    reply(Method, Key, Reply, InTime, State);


%% Fetch objects with address-id and key to maximum numbers of keys
handle_call({fetch = Method, {AddrId, Key}, Fun, MaxKeys, InTime},
            _From, #obj_server_state{meta_db_id     = MetaDBId,
                                     object_storage = StorageInfo} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_ver_cur, AddrId, Key),
    Reply = case catch leo_object_storage_haystack:fetch(
                         MetaDBId, BackendKey, Fun, MaxKeys) of
                {'EXIT', Cause} ->
                    {error, Cause};
                not_found = Ret ->
                    Ret;
                {ok, RetL} ->
                    {ok, RetL};
                Other ->
                    {error, Other}
            end,
    reply(Method, Key, Reply, InTime, State);


%% Store an object
handle_call({store,_,_,_}, _From, #obj_server_state{is_locked = true} = State) ->
    {reply, {error, ?ERROR_LOCKED_CONTAINER}, State};
handle_call({store,_,_,_}, _From, #obj_server_state{privilege = ?OBJ_PRV_READ_ONLY} = State) ->
    {reply, {error, ?ERROR_NOT_ALLOWED_ACCESS}, State};
handle_call({store = Method, Metadata, Bin, InTime}, _From,
            #obj_server_state{object_storage = StorageInfo,
                              is_del_blocked = IsDelBlocked} = State) ->
    Metadata_1 = leo_object_storage_transformer:transform_metadata(Metadata),
    Key = ?gen_backend_key(StorageInfo#backend_info.avs_ver_cur,
                           Metadata_1#?METADATA.addr_id,
                           Metadata_1#?METADATA.key),
    Object = leo_object_storage_transformer:metadata_to_object(Bin, Metadata),
    {Reply, State_1} =
        case Metadata_1#?METADATA.del of
            ?DEL_TRUE when IsDelBlocked == true ->
                {{error, ?ERROR_LOCKED_CONTAINER}, State};
            ?DEL_TRUE ->
                delete_1(Key, Object, State);
            ?DEL_FALSE ->
                put_1(Key, Object, State)
        end,
    Reply_1 = case Reply of
                  ok ->
                      ok;
                  {ok, _} ->
                      ok;
                  Other ->
                      Other
              end,
    erlang:garbage_collect(self()),
    reply(Method, Metadata_1#?METADATA.key, Reply_1, InTime, State_1);


%% Retrieve the current status
handle_call(get_stats, _From, #obj_server_state{storage_stats = StorageStats} = State) ->
    {reply, {ok, StorageStats}, State};

%% Set the current status
handle_call({set_stats, StorageStats}, _From, State) ->
    {reply, ok, State#obj_server_state{storage_stats = StorageStats}};

%% Retrieve the avs version
handle_call(get_avs_version_bin, _From, #obj_server_state{object_storage = StorageInfo} = State) ->
    Reply = {ok, StorageInfo#backend_info.avs_ver_cur},
    {reply, Reply, State};

%% Retrieve hash of the object with head-verb
handle_call({head_with_calc_md5 = Method, {AddrId, Key}, MD5Context, InTime},
            _From, #obj_server_state{meta_db_id      = MetaDBId,
                                     object_storage  = StorageInfo} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_ver_cur,
                                  AddrId, Key),
    Reply = leo_object_storage_haystack:head_with_calc_md5(
              MetaDBId, StorageInfo, BackendKey, MD5Context),

    State_1 = after_proc(Reply, State),
    erlang:garbage_collect(self()),
    reply(Method, Key, Reply, InTime, State_1);


%% Close the object-container
handle_call(close, _From,
            #obj_server_state{id = Id,
                              meta_db_id = MetaDBId,
                              state_filepath = StateFilePath,
                              storage_stats  = StorageStats,
                              object_storage = #backend_info{write_handler = WriteHandler,
                                                             read_handler  = ReadHandler}} = State) ->
    ok = close_storage(Id, MetaDBId, StateFilePath,
                       StorageStats, WriteHandler, ReadHandler),
    {reply, ok, State};

%% Retrieve the backend info/configuration
handle_call({get_backend_info, ?SERVER_OBJ_STORAGE}, _From,
            #obj_server_state{object_storage = ObjectStorage} = State) ->
    {reply, {ok, ObjectStorage}, State};

%% Lock the object-container
handle_call(lock, _From, State) ->
    {reply, ok, State#obj_server_state{is_locked = true}};

%% Lock the object-container
handle_call(block_del, _From, State) ->
    {reply, ok, State#obj_server_state{is_del_blocked = true}};

%% Unlock the object-container
handle_call(unlock, _From, State) ->
    {reply, ok, State#obj_server_state{is_locked = false,
                                       is_del_blocked = false}};

%% Open the object-container
handle_call({switch_container, FilePath,
             NumOfActiveObjs, SizeOfActiveObjs}, _From,
            #obj_server_state{object_storage = ObjectStorage,
                              storage_stats  = StorageStats} = State) ->
    %% Close the handlers
    #backend_info{
       write_handler = WriteHandler,
       read_handler  = ReadHandler} = ObjectStorage,
    catch leo_object_storage_haystack:close(WriteHandler, ReadHandler),

    %% Delete the old container
    case file:delete(ObjectStorage#backend_info.file_path) of
        ok ->
            ok;
        {error, enoent} ->
            ok;
        {error, Cause} ->
            error_logger:info_msg("~p,~p,~p,~p~n",
                                  [{module, ?MODULE_STRING}, {function, "handle_call/3"},
                                   {line, ?LINE}, {body, Cause}])
    end,
    State_1 = State#obj_server_state{object_storage =
                                         ObjectStorage#backend_info{
                                           file_path = FilePath},
                                     storage_stats =
                                         StorageStats#storage_stats{
                                           total_sizes  = SizeOfActiveObjs,
                                           active_sizes = SizeOfActiveObjs,
                                           total_num    = NumOfActiveObjs,
                                           active_num   = NumOfActiveObjs
                                          }
                                    },
    %% Open the new container
    State_2 = open_container(State_1),
    {reply, ok, State_2};

%% Append the history in the state
handle_call({append_compaction_history, History}, _From,
            #obj_server_state{storage_stats = StorageStats} = State) ->
    %% Retrieve the current compaciton-histories
    CurHist = StorageStats#storage_stats.compaction_hist,
    Len = length(CurHist),

    NewHist = case CurHist of
                  [] ->
                      [History];
                  [_|_] when Len > ?MAX_LEN_HIST ->
                      [History|lists:sublist(CurHist, (?MAX_LEN_HIST - 1))];
                  _ ->
                      [History|CurHist]
              end,
    {reply, ok, State#obj_server_state{
                  storage_stats =
                      StorageStats#storage_stats{
                        compaction_hist = NewHist}
                 }};

%% Retrieve the compaction worker
handle_call(get_compaction_worker, _From,
            #obj_server_state{compaction_worker_id = CompactionWorkerId} = State) ->
    {reply, {ok, CompactionWorkerId}, State};

%% Get the EOF offset
handle_call(get_eof_offset,
            _From, #obj_server_state{object_storage = StorageInfo} = State) ->
    Reply = leo_object_storage_haystack:get_eof_offset(StorageInfo),
    {reply, Reply, State};

%% Put incorrect data for the unit-test
handle_call({add_incorrect_data,_Bin},
            _From, #obj_server_state{object_storage =_StorageInfo} = State) ->
    ?add_incorrect_data(_StorageInfo,_Bin),
    {reply, ok, State}.


%% @doc Handling cast message
%% <p>
%% gen_server callback - Module:handle_cast(Request, State) -> Result.
%% </p>
handle_cast(_Msg, State) ->
    {noreply, State}.


%% @doc Handling all non call/cast messages
%% <p>
%% gen_server callback - Module:handle_info(Info, State) -> Result.
%% </p>
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc This function is called by a gen_server when it is about to
%%      terminate. It should be the opposite of Module:init/1 and do any necessary
%%      cleaning up. When it returns, the gen_server terminates with Reason.
terminate(_Reason, #obj_server_state{id = Id,
                                     meta_db_id = MetaDBId,
                                     state_filepath = StateFilePath,
                                     storage_stats  = StorageStats,
                                     object_storage = #backend_info{write_handler = WriteHandler,
                                                                    read_handler  = ReadHandler}}) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "terminate/2"},
                           {line, ?LINE}, {body, Id}]),
    ok = close_storage(Id, MetaDBId, StateFilePath,
                       StorageStats, WriteHandler, ReadHandler),
    ok.

%% @doc Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%====================================================================
%% INNER FUNCTIONS
%%====================================================================
%%--------------------------------------------------------------------
%% object operations.
%%--------------------------------------------------------------------
%% @doc Open the conatainer
%% @private
open_container(#obj_server_state{privilege = Privilege,
                                 object_storage =
                                     #backend_info{
                                        linked_path = FilePath}} = State) ->
    case leo_object_storage_haystack:open(FilePath, Privilege) of
        {ok, [NewWriteHandler, NewReadHandler, AVSVsnBin]} ->
            BackendInfo = State#obj_server_state.object_storage,
            State#obj_server_state{
              object_storage =
                  BackendInfo#backend_info{
                    avs_ver_cur   = AVSVsnBin,
                    write_handler = NewWriteHandler,
                    read_handler  = NewReadHandler},
              is_locked = false};
        {error, _} ->
            State
    end.



%% @doc Check the processing time and then reply the result
%% @private
-spec(reply(Method, Key, Reply, InTime, State) ->
             Result when Method::put|get|delete|head|fetch|store|head_with_calc_md5,
                         Key::binary(),
                         Reply::term(),
                         InTime::non_neg_integer(),
                         State::#obj_server_state{},
                         Result::term()).
reply(Method, Key, Reply, InTime,
      #obj_server_state{threshold_slow_processing = ThresholdSlowProcessing} = State) ->
    %% Check the request whether slow-operation or not
    OutTime = erlang:element(1, erlang:statistics(wall_clock)),
    Time = OutTime - InTime,

    case (Time > ThresholdSlowProcessing) of
        true ->
            leo_object_storage_event_notifier:notify(
              ?ERROR_MSG_SLOW_OPERATION, Method, Key, Time);
        false ->
            void
    end,
    {reply, Reply, State}.

%% @doc After object-operations
%% @private
after_proc(Ret, State) ->
    case Ret of
        {error, ?ERROR_FD_CLOSED} ->
            open_container(State);
        _Other ->
            State
    end.


%% @doc Put an object
%% @private
put_1(Key, Object, #obj_server_state{meta_db_id     = MetaDBId,
                                     object_storage = StorageInfo,
                                     storage_stats  = StorageStats} = State) ->
    {Ret, DiffRec, OldSize} =
        case leo_object_storage_haystack:head(MetaDBId, Key) of
            not_found ->
                {ok, 1, 0};
            {ok, MetaBin} ->
                Meta1 = binary_to_term(MetaBin),
                Meta2 = leo_object_storage_transformer:transform_metadata(Meta1),
                #?METADATA{del = DelFlag} = Meta2,
                case DelFlag of
                    ?DEL_FALSE ->
                        {ok, 0, leo_object_storage_haystack:calc_obj_size(Meta2)};
                    ?DEL_TRUE ->
                        {ok, 1, 0}
                end;
            _Error ->
                {_Error, 0, 0}
        end,
    case Ret of
        ok ->
            NewSize = leo_object_storage_haystack:calc_obj_size(Object),
            Reply   = leo_object_storage_haystack:put(MetaDBId, StorageInfo, Object),
            State_1 = after_proc(Reply, State),
            {Reply, State_1#obj_server_state{
                      storage_stats = StorageStats#storage_stats{
                                        total_sizes  = StorageStats#storage_stats.total_sizes  + NewSize,
                                        active_sizes = StorageStats#storage_stats.active_sizes + (NewSize - OldSize),
                                        total_num    = StorageStats#storage_stats.total_num    + 1,
                                        active_num   = StorageStats#storage_stats.active_num   + DiffRec}}};
        Error ->
            {Error, State}
    end.


%% @doc Remove an object
%% @private
delete_1(Key, Object, #obj_server_state{meta_db_id     = MetaDBId,
                                        object_storage = StorageInfo,
                                        storage_stats  = StorageStats} = State) ->
    {Reply, DiffRec, OldSize, State_1} =
        case leo_object_storage_haystack:head(
               MetaDBId, Key) of
            not_found ->
                {ok, 0, 0, State};
            {ok, MetaBin} ->
                Meta = binary_to_term(MetaBin),
                #?METADATA{del = DelFlag} = Meta,
                case DelFlag of
                    ?DEL_FALSE ->
                        case leo_object_storage_haystack:delete(
                               MetaDBId, StorageInfo, Object) of
                            ok ->
                                {ok, 1, leo_object_storage_haystack:calc_obj_size(Meta), State};
                            {error, Cause} ->
                                NewState = after_proc({error, Cause}, State),
                                {{error, Cause}, 0, 0, NewState}
                        end;
                    ?DEL_TRUE ->
                        {ok, 0, 0, State}
                end;
            _Error ->
                {_Error, 0, 0, State}
        end,

    case Reply of
        ok ->
            NewStorageStats =
                StorageStats#storage_stats{
                  total_sizes  = StorageStats#storage_stats.total_sizes,
                  active_sizes = StorageStats#storage_stats.active_sizes - OldSize,
                  total_num    = StorageStats#storage_stats.total_num,
                  active_num   = StorageStats#storage_stats.active_num - DiffRec},
            {Reply, State_1#obj_server_state{storage_stats = NewStorageStats}};
        _ ->
            {Reply, State_1}
    end.


%%--------------------------------------------------------------------
%% data-compaction.
%%--------------------------------------------------------------------
%% @doc create symbolic link and directory.
%% @private
-spec(get_raw_path(object, string(), string()) ->
             {ok, string()} | {error, any()}).
get_raw_path(object, ObjectStorageRootDir, SymLinkPath) ->
    case filelib:ensure_dir(ObjectStorageRootDir) of
        ok ->
            case file:read_link(SymLinkPath) of
                {ok, FileName} ->
                    {ok, FileName};
                {error, enoent} ->
                    RawPath = ?gen_raw_file_path(SymLinkPath),

                    case leo_file:file_touch(RawPath) of
                        ok ->
                            case file:make_symlink(RawPath, SymLinkPath) of
                                ok ->
                                    {ok, RawPath};
                                Error ->
                                    Error
                            end;
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @doc Close a storage
%% @private
close_storage(Id, MetaDBId, StateFilePath,
              StorageStats, WriteHandler, ReadHandler) when is_list(StateFilePath) ->
    _ = filelib:ensure_dir(StateFilePath),
    _ = leo_file:file_unconsult(
          StateFilePath,
          [{id, Id},
           {total_sizes,     StorageStats#storage_stats.total_sizes},
           {active_sizes,    StorageStats#storage_stats.active_sizes},
           {total_num,       StorageStats#storage_stats.total_num},
           {active_num,      StorageStats#storage_stats.active_num},
           {compaction_hist, StorageStats#storage_stats.compaction_hist}
          ]),
    catch leo_object_storage_haystack:close(WriteHandler, ReadHandler),
    catch leo_backend_db_server:close(MetaDBId),
    ok;
close_storage(_,_,_,_,_,_) ->
    ok.

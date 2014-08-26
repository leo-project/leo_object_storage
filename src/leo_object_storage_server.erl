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
-module(leo_object_storage_server).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-behaviour(gen_server).

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/5, start_link/6, stop/1]).
-export([put/2, get/4, delete/2, head/2, fetch/4, store/3,
         get_stats/1, set_stats/2,
         get_avs_version_bin/1,
         head_with_calc_md5/3, close/1,
         get_backend_info/2, set_backend_info/3,
         get_compaction_worker/1
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

-record(state, {
          id                   :: atom(),
          meta_db_id           :: atom(),
          compaction_worker_id :: atom(),
          object_storage       :: #backend_info{},
          storage_stats        :: #storage_stats{},
          state_filepath       :: string(),
          is_strict_check      :: boolean()
         }).

-define(DEF_TIMEOUT, 30000).


-ifdef(TEST).
-define(add_incorrect_data(_StorageInfo,_Bin),
        leo_object_storage_haystack:add_incorrect_data(_StorageInfo,_Bin)).
-else.
-define(add_incorrect_data(_StorageInfo,_Bin), ok).
-endif.


%%====================================================================
%% API
%%====================================================================
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
-spec(start_link(atom(), non_neg_integer(), atom(), atom(), string()) ->
             {ok, pid()} | {error, any()}).
start_link(Id, SeqNo, MetaDBId, CompactionWorkerId, RootPath) ->
    start_link(Id, SeqNo, MetaDBId, CompactionWorkerId, RootPath, false).

-spec(start_link(atom(), non_neg_integer(), atom(), atom(), string(), boolean()) ->
             {ok, pid()} | {error, any()}).
start_link(Id, SeqNo, MetaDBId, CompactionWorkerId, RootPath, IsStrictCheck) ->
    gen_server:start_link({local, Id}, ?MODULE,
                          [Id, SeqNo, MetaDBId, CompactionWorkerId, RootPath, IsStrictCheck], []).

%% @doc Stop this server
%%
-spec(stop(atom()) -> ok).
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
-spec(put(atom(), #?OBJECT{}) ->
             ok | {error, any()}).
put(Id, Object) ->
    gen_server:call(Id, {put, Object}, ?DEF_TIMEOUT).


%% @doc Retrieve an object from the object-storage
%%
-spec(get(atom(), tuple(), integer(), integer()) ->
             {ok, #?METADATA{}, #?OBJECT{}} | not_found | {error, any()}).
get(Id, Key, StartPos, EndPos) ->
    gen_server:call(Id, {get, Key, StartPos, EndPos}, ?DEF_TIMEOUT).


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(delete(atom(), #?OBJECT{}) ->
             ok | {error, any()}).
delete(Id, Object) ->
    gen_server:call(Id, {delete, Object}, ?DEF_TIMEOUT).


%% @doc Retrieve an object's metadata from the object-storage
%%
-spec(head(atom(), tuple()) ->
             {ok, binary()} | not_found | {error, any()}).
head(Id, Key) ->
    gen_server:call(Id, {head, Key}, ?DEF_TIMEOUT).


%% @doc Retrieve objects from the object-storage by Key and Function
%%
-spec(fetch(atom(), any(), fun(), non_neg_integer()|undefined) ->
             {ok, list()} | {error, any()}).
fetch(Id, Key, Fun, MaxKeys) ->
    gen_server:call(Id, {fetch, Key, Fun, MaxKeys}, ?DEF_TIMEOUT).


%% @doc Store metadata and data
%%
-spec(store(atom(), #?METADATA{}, binary()) ->
             ok | {error, any()}).
store(Id, Metadata, Bin) ->
    gen_server:call(Id, {store, Metadata, Bin}, ?DEF_TIMEOUT).


%% @doc Retrieve the storage stats specfied by Id
%%      which contains number of objects and so on.
%%
-spec(get_stats(atom()) ->
             {ok, #storage_stats{}} |
             {error, any()}).
get_stats(Id) ->
    gen_server:call(Id, get_stats, ?DEF_TIMEOUT).


%% @doc Retrieve the storage stats specfied by Id
%%      which contains number of objects and so on.
%%
-spec(set_stats(atom(), #storage_stats{}) ->
             ok).
set_stats(Id, StorageStats) ->
    gen_server:call(Id, {set_stats, StorageStats}, ?DEF_TIMEOUT).


%% @doc Get AVS format version binary like <<"LeoFS AVS-2.2">>
-spec(get_avs_version_bin(atom()) -> ok).
get_avs_version_bin(Id) ->
    gen_server:call(Id, get_avs_version_bin, ?DEF_TIMEOUT).


%% @doc Retrieve a metada/data from backend_db/object-storage
%%      AND calc MD5 based on the body data
%%
-spec(head_with_calc_md5(atom(), tuple(), any()) ->
             {ok, #?METADATA{}, any()} | {error, any()}).
head_with_calc_md5(Id, Key, MD5Context) ->
    gen_server:call(Id, {head_with_calc_md5, Key, MD5Context}, ?DEF_TIMEOUT).


%% @doc Close a storage
%%
close(Id) ->
    gen_server:call(Id, close, ?DEF_TIMEOUT).


%% @doc Retrieve object-storage/metadata-storage info
%%
get_backend_info(Id, ServerType) ->
    gen_server:call(Id, {get_backend_info, ServerType}, ?DEF_TIMEOUT).


%% @doc Retrieve object-storage/metadata-storage info
%%
set_backend_info(Id, ServerType, BackendInfo) ->
    gen_server:call(Id, {set_backend_info, ServerType, BackendInfo}, ?DEF_TIMEOUT).

%% @doc Retrieve the compaction worker
%%
get_compaction_worker(Id) ->
    gen_server:call(Id, get_compaction_worker, ?DEF_TIMEOUT).


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
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Id, SeqNo, MetaDBId, CompactionWorkerId, RootPath, IsStrictCheck]) ->
    ObjectStorageDir  = lists:append([RootPath, ?DEF_OBJECT_STORAGE_SUB_DIR]),
    ObjectStoragePath = lists:append([ObjectStorageDir, integer_to_list(SeqNo), ?AVS_FILE_EXT]),
    StateFilePath     = lists:append([RootPath, ?DEF_STATE_SUB_DIR, atom_to_list(Id)]),

    StorageStats =
        case file:consult(StateFilePath) of
            {ok, Props} ->
                #storage_stats{
                   file_path    = ObjectStoragePath,
                   total_sizes  = leo_misc:get_value('total_sizes',  Props, 0),
                   active_sizes = leo_misc:get_value('active_sizes', Props, 0),
                   total_num    = leo_misc:get_value('total_num',    Props, 0),
                   active_num   = leo_misc:get_value('active_num',   Props, 0),
                   %% compaction_histories = leo_misc:get_value('compaction_histories', Props, []),
                   has_error            = leo_misc:get_value('has_error', Props, false)};
            _ -> #storage_stats{file_path = ObjectStoragePath}
        end,

    %% open object-storage.
    case get_raw_path(object, ObjectStorageDir, ObjectStoragePath) of
        {ok, ObjectStorageRawPath} ->
            case leo_object_storage_haystack:open(ObjectStorageRawPath) of
                {ok, [ObjectWriteHandler, ObjectReadHandler, AVSVsnBin]} ->
                    StorageInfo = #backend_info{
                                     file_path           = ObjectStoragePath,
                                     file_path_raw       = ObjectStorageRawPath,
                                     write_handler       = ObjectWriteHandler,
                                     read_handler        = ObjectReadHandler,
                                     avs_version_bin_cur = AVSVsnBin},
                    {ok, #state{id = Id,
                                meta_db_id           = MetaDBId,
                                compaction_worker_id = CompactionWorkerId,
                                object_storage       = StorageInfo,
                                storage_stats        = StorageStats,
                                state_filepath       = StateFilePath,
                                is_strict_check      = IsStrictCheck
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


handle_call(stop, _From, State) ->
    {stop, shutdown, ok, State};


%% Insert an object
handle_call({put, Object}, _From, #state{meta_db_id     = MetaDBId,
                                         object_storage = StorageInfo,
                                         storage_stats  = StorageStats} = State) ->
    Key = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                           Object#?OBJECT.addr_id,
                           Object#?OBJECT.key),
    {DiffRec, OldSize} =
        case leo_object_storage_haystack:head(MetaDBId, Key) of
            not_found ->
                {1, 0};
            {ok, MetaBin} ->
                Metadata_1 = binary_to_term(MetaBin),
                Metadata_2 = leo_object_storage_transformer:transform_metadata(Metadata_1),
                {0, leo_object_storage_haystack:calc_obj_size(Metadata_2)};
            _ ->
                {1, 0}
        end,

    NewSize  = leo_object_storage_haystack:calc_obj_size(Object),
    Reply    = leo_object_storage_haystack:put(MetaDBId, StorageInfo, Object),
    NewState = after_proc(Reply, State),
    _ = erlang:garbage_collect(self()),

    NewStorageStats =
        StorageStats#storage_stats{
          total_sizes  = StorageStats#storage_stats.total_sizes  + NewSize,
          active_sizes = StorageStats#storage_stats.active_sizes + (NewSize - OldSize),
          total_num    = StorageStats#storage_stats.total_num    + 1,
          active_num   = StorageStats#storage_stats.active_num   + DiffRec},
    {reply, Reply, NewState#state{storage_stats = NewStorageStats}};

%% Retrieve an object
handle_call({get, {AddrId, Key}, StartPos, EndPos},
            _From, #state{meta_db_id      = MetaDBId,
                          object_storage  = StorageInfo,
                          is_strict_check = IsStrictCheck} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                                  AddrId, Key),
    Reply = leo_object_storage_haystack:get(
              MetaDBId, StorageInfo, BackendKey, StartPos, EndPos, IsStrictCheck),

    NewState = after_proc(Reply, State),
    erlang:garbage_collect(self()),

    {reply, Reply, NewState};

%% Remove an object
handle_call({delete, Object}, _From, #state{meta_db_id     = MetaDBId,
                                            object_storage = StorageInfo,
                                            storage_stats  = StorageStats} = State) ->
    Key = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                           Object#?OBJECT.addr_id,
                           Object#?OBJECT.key),
    {DiffRec, OldSize} =
        case leo_object_storage_haystack:head(
               MetaDBId, Key) of
            not_found ->
                {0, 0};
            {ok, MetaBin} ->
                Meta = binary_to_term(MetaBin),
                {-1, leo_object_storage_haystack:calc_obj_size(Meta)};
            _ ->
                {0, 0}
        end,

    NewSize = leo_object_storage_haystack:calc_obj_size(Object),
    Reply   = leo_object_storage_haystack:delete(MetaDBId, StorageInfo, Object),

    NewState = after_proc(Reply, State),
    NewStorageStats =
        StorageStats#storage_stats{
          total_sizes  = StorageStats#storage_stats.total_sizes  + NewSize,
          active_sizes = StorageStats#storage_stats.active_sizes - (NewSize - OldSize),
          total_num    = StorageStats#storage_stats.total_num    + 1,
          active_num   = StorageStats#storage_stats.active_num   + DiffRec},
    {reply, Reply, NewState#state{storage_stats = NewStorageStats}};

%% Head an object
handle_call({head, {AddrId, Key}},
            _From, #state{meta_db_id = MetaDBId,
                          object_storage = StorageInfo} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                                  AddrId, Key),
    Reply = leo_object_storage_haystack:head(MetaDBId, BackendKey),
    {reply, Reply, State};

%% Fetch objects with address-id and key to maximum numbers of keys
handle_call({fetch, {AddrId, Key}, Fun, MaxKeys},
            _From, #state{meta_db_id     = MetaDBId,
                          object_storage = StorageInfo} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                                  AddrId, Key),
    Reply = leo_object_storage_haystack:fetch(MetaDBId, BackendKey, Fun, MaxKeys),
    {reply, Reply, State};

%% Store an object
handle_call({store, Metadata, Bin}, _From, #state{meta_db_id     = MetaDBId,
                                                  object_storage = StorageInfo,
                                                  storage_stats  = StorageStats} = State) ->
    Metadata_1 = leo_object_storage_transformer:transform_metadata(Metadata),
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                                  Metadata_1#?METADATA.addr_id,
                                  Metadata_1#?METADATA.key),
    {DiffRec, OldSize} =
        case leo_object_storage_haystack:head(MetaDBId, BackendKey) of
            not_found ->
                {1, 0};
            {ok, MetaBin} ->
                Metadata_2 = binary_to_term(MetaBin),
                {0, leo_object_storage_haystack:calc_obj_size(Metadata_2)};
            _ ->
                {1, 0}
        end,

    NewSize = leo_object_storage_haystack:calc_obj_size(Metadata_1),
    Reply   = leo_object_storage_haystack:store(MetaDBId, StorageInfo, Metadata_1, Bin),
    NewStorageStats =
        StorageStats#storage_stats{
          total_sizes  = StorageStats#storage_stats.total_sizes  + NewSize,
          active_sizes = StorageStats#storage_stats.active_sizes + (NewSize - OldSize),
          total_num    = StorageStats#storage_stats.total_num    + 1,
          active_num   = StorageStats#storage_stats.active_num   + DiffRec},
    {reply, Reply, State#state{storage_stats = NewStorageStats}};

%% Retrieve the current status
handle_call(get_stats, _From, #state{storage_stats = StorageStats} = State) ->
    {reply, {ok, StorageStats}, State};

%% Set the current status
handle_call({set_stats, StorageStats}, _From, State) ->
    {reply, ok, State#state{storage_stats = StorageStats}};

%% Retrieve the avs version
handle_call(get_avs_version_bin, _From, #state{object_storage = StorageInfo} = State) ->
    Reply = {ok, StorageInfo#backend_info.avs_version_bin_cur},
    {reply, Reply, State};

%% Retrieve hash of the object with head-verb
handle_call({head_with_calc_md5, {AddrId, Key}, MD5Context},
            _From, #state{meta_db_id      = MetaDBId,
                          object_storage  = StorageInfo} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                                  AddrId, Key),
    Reply = leo_object_storage_haystack:head_with_calc_md5(
              MetaDBId, StorageInfo, BackendKey, MD5Context),

    NewState = after_proc(Reply, State),
    erlang:garbage_collect(self()),
    {reply, Reply, NewState};

%% Close the object-container
handle_call(close, _From,
            #state{id = Id,
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
            #state{object_storage = ObjectStorage} = State) ->
    {reply, {ok, ObjectStorage}, State};

%% Set the backend info/configuration
handle_call({set_backend_info, ?SERVER_OBJ_STORAGE, BackendInfo}, _From, State) ->
    {reply, ok, State#state{object_storage = BackendInfo}};

%% Retrieve the compaction worker
handle_call(get_compaction_worker, _From,
            #state{compaction_worker_id = CompactionWorkerId} = State) ->
    {reply, {ok, CompactionWorkerId}, State};

%% Put incorrect data for the unit-test
handle_call({add_incorrect_data,_Bin},
            _From, #state{object_storage =_StorageInfo} = State) ->
    ?add_incorrect_data(_StorageInfo,_Bin),
    {reply, ok, State}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast message
handle_cast(_Msg, State) ->
    {noreply, State}.


%% Function: handle_info(Info, State) -> {noreply, State}          |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
handle_info(_Info, State) ->
    {noreply, State}.

%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
terminate(_Reason, #state{id = Id,
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

%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%====================================================================
%% INNER FUNCTIONS
%%====================================================================
%%--------------------------------------------------------------------
%% object operations.
%%--------------------------------------------------------------------
%% @doc
%% @private
after_proc(Ret, #state{object_storage = #backend_info{file_path = FilePath}} = State) ->
    case Ret of
        {error, ?ERROR_FD_CLOSED} ->
            case leo_object_storage_haystack:open(FilePath) of
                {ok, [NewWriteHandler, NewReadHandler, AVSVsnBin]} ->
                    BackendInfo = State#state.object_storage,
                    State#state{
                      object_storage = BackendInfo#backend_info{
                                         avs_version_bin_cur = AVSVsnBin,
                                         write_handler       = NewWriteHandler,
                                         read_handler        = NewReadHandler}};
                {error, _} ->
                    State
            end;
        _Other ->
            State
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
           {total_sizes,  StorageStats#storage_stats.total_sizes},
           {active_sizes, StorageStats#storage_stats.active_sizes},
           {total_num,    StorageStats#storage_stats.total_num},
           {active_num,   StorageStats#storage_stats.active_num},
           %% {compaction_histories, StorageStats#storage_stats.compaction_histories},
           {has_error,            StorageStats#storage_stats.has_error}]),
    ok = leo_object_storage_haystack:close(WriteHandler, ReadHandler),
    ok = leo_backend_db_server:close(MetaDBId),
    ok;
close_storage(_,_,_,_,_,_) ->
    ok.

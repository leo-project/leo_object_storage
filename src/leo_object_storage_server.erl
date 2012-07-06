%%======================================================================
%%
%% Leo Object Storage
%%
%% Copyright (c) 2012 Rakuten, Inc.
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
-vsn('0.9.0').

-behaviour(gen_server).

-include("leo_object_storage.hrl").

%% API
-export([start_link/6, stop/1]).
-export([put/2, get/2, delete/2, head/2, fetch/3]).
-export([datasync/1, compact/1, stats/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {id             :: atom(),
                meta_db_id     :: atom(),
                device_num     :: integer(),
                storage_num    :: integer(),
                object_storage :: #backend_info{},
                storage_stats  :: #storage_stats{}
               }).

-define(AVS_FILE_EXT, ".avs").

%%====================================================================
%% API
%%====================================================================
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
-spec(start_link(atom(), atom(), integer(), integer(), atom(), string()) ->
             ok | {error, any()}).
start_link(Id, MetaDBId, DeviceNumber, StorageNumber, ObjectStorageMod, RootPath) ->
    io:format("id:~p, meda-db-id:~p, dn:~p, sn:~p, mod:~p, path:~p~n",
              [Id,  MetaDBId, DeviceNumber, StorageNumber, ObjectStorageMod, RootPath]),
    gen_server:start_link({local, Id}, ?MODULE, [Id, MetaDBId, DeviceNumber, StorageNumber, ObjectStorageMod, RootPath], []).

%% @doc stop this server.
%%
-spec(stop(Id::atom()) -> ok).
stop(Id) ->
    gen_server:call(Id, stop).

%%--------------------------------------------------------------------
%% API - object operations.
%%--------------------------------------------------------------------
%% @doc put an object and an object's metadata.
%%
-spec(put(Id::atom(), ObjectPool::pid()) ->
             ok | {error, any()}).
put(Id, ObjectPool) ->
    gen_server:call(Id, {put, ObjectPool}).


%% @doc get an object.
%%
-spec(get(Id::atom(), KeyBin::binary()) ->
             {ok, #metadata{}, list()} | not_found | {error, any()}).
get(Id, KeyBin) ->
    gen_server:call(Id, {get, KeyBin}).


%% @doc logical-delete.
%%
-spec(delete(Id::atom(), ObjectPool::pid()) ->
             ok | {error, any()}).
delete(Id, ObjectPool) ->
    gen_server:call(Id, {delete, ObjectPool}).


%% @doc get an object's metadata.
%%
-spec(head(Id::atom(), KeyBin::binary()) ->
             {ok, #metadata{}} | {error, any()}).
head(Id, KeyBin) ->
    gen_server:call(Id, {head, KeyBin}).


%% @doc
%%
-spec(fetch(atom(), binary(), function()) ->
             {ok, list()} | {error, any()}).
fetch(Id, KeyBin, Fun) ->
    gen_server:call(Id, {fetch, KeyBin, Fun}).


%%--------------------------------------------------------------------
%% API - data-compaction.
%%--------------------------------------------------------------------
%% @doc compaction/start prepare(check disk usage, mk temporary file...)
%%
-spec(compact(Id::atom()) ->
             ok |
             {error, any()}).
compact(Id) ->
    gen_server:call(Id, compact).

%%--------------------------------------------------------------------
%% API - get the storage stats
%%--------------------------------------------------------------------
%% @doc get the storage stats specfied by Id which contains number of (active)object and so on.
%%
-spec(stats(Id::atom()) ->
             {ok, #storage_stats{}} |
             {error, any()}).
stats(Id) ->
    gen_server:call(Id, stats).

%% @doc data synchronize.
%%
-spec(datasync(Id::atom()) -> ok).
datasync(Id) ->
    gen_server:cast(Id, {datasync, Id}).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Id, MetaDBId, DeviceNum, StorageNum, ObjectStorage, RootPath]) ->
    ObjectStorageDir  = RootPath ++ ?DEF_OBJECT_STORAGE_SUB_DIR,
    ObjectStoragePath = ObjectStorageDir ++ atom_to_list(Id) ++ ?AVS_FILE_EXT,

    %% open object-storage.
    case get_raw_path(object, ObjectStorageDir, ObjectStoragePath) of
        {ok, ObjectStorageRawPath} ->
            Obj = ObjectStorage:new([],[]),
            case Obj:open(ObjectStorageRawPath) of
                {ok, [ObjectWriteHandler, ObjectReadHandler]} ->
                    {ok, #state{id = Id,
                                meta_db_id  = MetaDBId,
                                device_num  = DeviceNum,
                                storage_num = StorageNum,
                                object_storage   = #backend_info{backend       = ObjectStorage,
                                                                 file_path     = ObjectStoragePath,
                                                                 file_path_raw = ObjectStorageRawPath,
                                                                 write_handler = ObjectWriteHandler,
                                                                 read_handler  = ObjectReadHandler}}};
                {error, Cause} ->
                    io:format("~w, cause:~p~n", [?LINE, Cause]),
                    {stop, Cause}
            end;
        {error, Cause} ->
            io:format("~w, cause:~p~n", [?LINE, Cause]),
            {stop, Cause}
    end.


handle_call(stop, _From, #state{meta_db_id     = MetaDBId,
                                object_storage = #backend_info{backend       = Module,
                                                               write_handler = WriteHandler,
                                                               read_handler  = ReadHandler} = StorageInfo} = State) ->
    Obj = Module:new(MetaDBId, StorageInfo),
    Obj:close(WriteHandler, ReadHandler),
    {stop, normal, ok, State};


handle_call({put, ObjectPool}, _From, #state{meta_db_id     = MetaDBId,
                                             object_storage = StorageInfo} = State) ->
    #backend_info{backend = Module} = StorageInfo,
    Obj = Module:new(MetaDBId, StorageInfo),
    Reply = Obj:put(ObjectPool),

    NewState = after_proc(Reply, State),
    erlang:garbage_collect(self()),

    {reply, Reply, NewState};

handle_call({get, KeyBin}, _From, #state{meta_db_id     = MetaDBId,
                                         object_storage = StorageInfo} = State) ->
    #backend_info{backend = Module} = StorageInfo,
    Obj = Module:new(MetaDBId, StorageInfo),
    Reply = Obj:get(KeyBin),

    NewState = after_proc(Reply, State),
    erlang:garbage_collect(self()),

    {reply, Reply, NewState};


handle_call({delete, ObjectPool}, _From, #state{meta_db_id     = MetaDBId,
                                                object_storage = StorageInfo} = State) ->
    #backend_info{backend = Module} = StorageInfo,
    Obj = Module:new(MetaDBId, StorageInfo),
    Reply = Obj:delete(ObjectPool),

    NewState = after_proc(Reply, State),
    {reply, Reply, NewState};


handle_call({head, KeyBin}, _From, #state{meta_db_id     = MetaDBId,
                                          object_storage = StorageInfo} = State) ->
    #backend_info{backend = Module} = StorageInfo,
    Obj = Module:new(MetaDBId, StorageInfo),
    Reply = Obj:head(KeyBin),

    {reply, Reply, State};


handle_call({fetch, KeyBin, Fun}, _From, #state{meta_db_id     = MetaDBId,
                                                object_storage = StorageInfo} = State) ->
    #backend_info{backend = Module} = StorageInfo,
    Obj = Module:new(MetaDBId, StorageInfo),
    Reply = Obj:fetch(KeyBin, Fun),

    {reply, Reply, State};


handle_call(stats, _From, #state{meta_db_id     = MetaDBId,
                                 object_storage = #backend_info{backend       = Module,
                                                                file_path     = RootPath,
                                                                read_handler  = ReadHandler} = StorageInfo} = State) ->
    Obj = Module:new(MetaDBId, StorageInfo),

    case Obj:compact_get(ReadHandler) of
        {ok, Metadata, [_HeaderValue, _KeyValue, _BodyValue, NextOffset]} ->
            case do_stats(MetaDBId, Obj, ReadHandler, Metadata, NextOffset, #storage_stats{}) of
                {ok, Stats} ->
                    {reply, {ok, Stats#storage_stats{file_path   = RootPath,
                                                     total_sizes = filelib:file_size(RootPath)}}, State};
                Error ->
                    {reply, Error, State}
            end;
        {error, eof} ->
            {reply, {ok, #storage_stats{file_path   = RootPath,
                                        total_sizes = filelib:file_size(RootPath)}}, State};
        Error ->
            {reply, Error, State}
    end;

handle_call(compact, _From, State) ->
    {Reply, NewState} = compact_fun(State),
    {reply, Reply, NewState}.

%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast message
handle_cast({datasync, _Id}, State) ->
    {noreply, State};

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
terminate(_Reason, _State) ->
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
after_proc(Ret, #state{meta_db_id = MetaDBId,
                       object_storage = #backend_info{backend   = Module,
                                                      file_path = FilePath} = StorageInfo} = State) ->
    case Ret of
        {error, ?ERROR_FD_CLOSED} ->
            Obj = Module:new(MetaDBId, StorageInfo),

            case Obj:open(FilePath) of
                {ok, [NewWriteHandler, NewReadHandler]} ->
                    BackendInfo = State#state.object_storage,
                    State#state{object_storage = BackendInfo#backend_info{
                                                   write_handler = NewWriteHandler,
                                                   read_handler  = NewReadHandler}};
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
-spec(get_raw_path(object, ObjectStorageRootDir::string(), SymLinkPath::string()) ->
             {ok, RawPath::string()} | {error, any()}).
get_raw_path(object, ObjectStorageRootDir, SymLinkPath) ->
    case filelib:ensure_dir(ObjectStorageRootDir) of
        ok ->
            case file:read_link(SymLinkPath) of
                {ok, FileName} ->
                    {ok, FileName};
                {error, enoent} ->
                    RawPath = gen_raw_file_path(SymLinkPath),

                    case leo_utils:file_touch(RawPath) of
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


%% @doc Reduce objects from the object-container.
%% @private
-spec(compact_fun(#state{}) ->
             {ok, #state{}} | {error, any(), #state{}}).
compact_fun(State) ->
    #state{meta_db_id     = MetaDBId,
           object_storage = StorageInfo} = State,
    #backend_info{backend       = Module,
                  file_path     = FilePath} = StorageInfo,

    Res = case calc_remain_disksize(MetaDBId, FilePath) of
              {ok, RemainSize} ->
                  case (RemainSize > 0) of
                      true ->
                          TmpPath = gen_raw_file_path(FilePath),
                          Obj = Module:new(MetaDBId, StorageInfo),

                          case Obj:open(TmpPath) of
                              {ok, [TmpWriteHandler, TmpReadHandler]} ->
                                  BackendInfo = State#state.object_storage,
                                  {ok, State#state{object_storage = BackendInfo#backend_info{
                                                                      tmp_file_path_raw = TmpPath,
                                                                      tmp_write_handler = TmpWriteHandler,
                                                                      tmp_read_handler  = TmpReadHandler}}};
                              Error ->
                                  {Error, State}
                          end;
                      false ->
                          {{error, system_limit}, State}
                  end;
              Error ->
                  {Error, State}
          end,
    compact_fun1(Res).


%% @doc Reduce objects from the object-container.
%% @private
compact_fun1({ok, State}) ->
    #state{meta_db_id     = MetaDBId,
           object_storage = StorageInfo} = State,
    #backend_info{backend       = Module,
                  read_handler  = ReadHandler,
                  write_handler = WriteHandler,
                  tmp_read_handler  = TmpReadHandler,
                  tmp_write_handler = TmpWriteHandler} = StorageInfo,

    Obj = Module:new(MetaDBId, StorageInfo),
    Res = case Obj:compact_get(ReadHandler) of
              {ok, Metadata, [_HeaderValue, KeyValue, BodyValue, NextOffset]} ->
                  case leo_backend_db_api:compact_start(MetaDBId) of
                      ok ->
                          Ret = do_compact(Metadata, [KeyValue, BodyValue, NextOffset], State),
                          Obj:close(WriteHandler,    ReadHandler),
                          Obj:close(TmpWriteHandler, TmpReadHandler),
                          Ret;
                      Error0 ->
                          Error0
                  end;
              Error1 ->
                  Error1
          end,
    compact_fun2({Res, State});

compact_fun1({Error,_State}) ->
    Error.


%% @doc Reduce objects from the object-container.
%% @private
compact_fun2({ok, State}) ->
    #state{meta_db_id     = MetaDBId,
           object_storage = StorageInfo} = State,
    #backend_info{backend           = Module,
                  file_path_raw     = RawPath,
                  file_path         = RootPath,
                  tmp_file_path_raw = TmpFilePathRaw} = StorageInfo,

    Obj = Module:new(MetaDBId, StorageInfo),
    catch file:delete(RootPath),

    case file:make_symlink(TmpFilePathRaw, RootPath) of
        ok ->
            catch file:delete(RawPath),

            case Obj:open(RootPath) of
                {ok, [NewWriteHandler, NewReadHandler]} ->
                    leo_backend_db_api:compact_end(MetaDBId, true),

                    BackendInfo = State#state.object_storage,
                    NewState    = State#state{object_storage = BackendInfo#backend_info{
                                                                 file_path_raw = TmpFilePathRaw,
                                                                 read_handler  = NewReadHandler,
                                                                 write_handler = NewWriteHandler}},
                    {ok, NewState};
                {error, Cause} ->
                    {{error, Cause}, State}
            end;
        {error, Cause} ->
            {{error, Cause}, State}
    end;

compact_fun2({_Error, State}) ->
    #state{meta_db_id     = MetaDBId,
           object_storage = StorageInfo} = State,
    #backend_info{tmp_file_path_raw = TmpFilePathRaw} = StorageInfo,

    %% rollback (delete tmp files)
    %%
    catch file:delete(TmpFilePathRaw),
    leo_backend_db_api:compact_end(MetaDBId, false),
    {ok, State}.


%% @doc Calculate remain disk-sizes.
%% @private
-spec(calc_remain_disksize(atom(), string()) ->
             {ok, integer()} | {error, any()}).
calc_remain_disksize(MetaDBId, FilePath) ->
    case leo_utils:file_get_mount_path(FilePath) of
        {ok, MountPath} ->
            {ok, MetaDir} = leo_backend_db_api:get_db_raw_filepath(MetaDBId),

            case catch leo_utils:file_get_total_size(MetaDir) of
                {'EXIT', Reason} ->
                    {error, Reason};
                MetaSize ->
                    AvsSize = filelib:file_size(FilePath),
                    Remain = leo_utils:file_get_remain_disk(MountPath),

                    %% ?info("handle_call/3",
                    %%       "(compact_start) mount_path: ~p, remain:~p meta_dir:~p meta_size:~p avs_size:~p rec:~p",
                    %%       [MountPath, Remain, MetaDir, MetaSize, AvsSize]),

                    {ok, Remain - (AvsSize + MetaSize) * 1.5}
            end;
        Error ->
            Error
    end.


%% @doc Is deleted a record ?
%% @private
-spec(is_deleted_rec(atom(), #metadata{}) ->
             boolean()).
is_deleted_rec(_MetaDBId, #metadata{del = Del}) when Del =/= 0 ->
    true;
is_deleted_rec(MetaDBId, #metadata{key      = Key,
                                   addr_id  = AddrId} = MetaFromAvs) ->
    case leo_backend_db_api:get(MetaDBId, term_to_binary({AddrId, Key})) of
        {ok, MetaOrg} ->
            MetaOrgTerm = binary_to_term(MetaOrg),
            is_deleted_rec(MetaDBId, MetaFromAvs, MetaOrgTerm);
        not_found ->
            true;
        _Other ->
            false
    end.

-spec(is_deleted_rec(atom(), #metadata{}, #metadata{}) ->
             boolean()).
is_deleted_rec(_MetaDBId,_Meta0, Meta1) when Meta1#metadata.del =/= 0 ->
    true;
is_deleted_rec(_MetaDBId, Meta0, Meta1) when Meta0#metadata.offset =/= Meta1#metadata.offset ->
    true;
is_deleted_rec(_MetaDBId,_Meta0,_Meta1) ->
    false.


%% @doc Execute getting status.
%% @private
-spec(do_stats(atom(), atom(), pid(), #metadata{}, integer(), #storage_stats{}) ->
             {ok, any()} | {error, any()}).
do_stats(MetaDBId, Obj, ReadHandler, Metadata, NextOffset, #storage_stats{total_num  = ObjTotal,
                                                                          active_num = ObjActive} = StorageStats) ->
    NewStorageStats =
        case is_deleted_rec(MetaDBId, Metadata) of
            true  -> StorageStats#storage_stats{total_num  = ObjTotal  + 1};
            false -> StorageStats#storage_stats{total_num  = ObjTotal  + 1,
                                                active_num = ObjActive + 1}
        end,

    case Obj:compact_get(ReadHandler, NextOffset) of
        {ok, NewMetadata, [_HeaderValue, _NewKeyValue, _NewBodyValue, NewNextOffset]} ->
            do_stats(MetaDBId, Obj, ReadHandler, NewMetadata, NewNextOffset, NewStorageStats);
        {error, eof} ->
            {ok, NewStorageStats};
        Error ->
            Error
    end.


%% @doc Reduce unnecessary objects from object-container.
%% @private
-spec(do_compact(#metadata{},#state{}, list()) ->
             ok | {error, any()}).
do_compact(Metadata, Props, State) ->
    #metadata{addr_id  = AddrId,
              key      = Key} = Metadata,
    [KeyValue, BodyValue, _] = Props,

    #state{meta_db_id     = MetaDBId,
           object_storage = StorageInfo} = State,
    #backend_info{backend           = Module,
                  tmp_write_handler = TmpWriteHandler} = StorageInfo,

    case is_deleted_rec(MetaDBId, Metadata) of
        true ->
            do_compact1(ok, Metadata, Props, State);
        false ->
            %% Insert into the temporary object-container.
            %%
            Obj = Module:new(MetaDBId, StorageInfo),

            case Obj:compact_put(TmpWriteHandler, Metadata, KeyValue, BodyValue) of
                {ok, Offset} ->
                    NewMeta = Metadata#metadata{offset = Offset},
                    Ret = leo_backend_db_api:compact_put(MetaDBId,
                                                         term_to_binary({AddrId, Key}),
                                                         term_to_binary(NewMeta)),
                    do_compact1(Ret, NewMeta, Props, State);
                Error ->
                    do_compact1(Error, Metadata, Props, State)
            end
    end.


%% @doc Reduce unnecessary objects from object-container.
%% @private
do_compact1(ok,_Metadata, Props, State) ->
    [_, _, NextOffset] = Props,

    #state{meta_db_id     = MetaDBId,
           object_storage = StorageInfo} = State,
    #backend_info{backend      = Module,
                  read_handler = ReadHandler} = StorageInfo,

    Obj = Module:new(MetaDBId, StorageInfo),

    case Obj:compact_get(ReadHandler, NextOffset) of
        {ok, NewMetadata, [_HeaderValue, NewKeyValue, NewBodyValue, NewNextOffset]} ->
            do_compact(NewMetadata, [NewKeyValue, NewBodyValue, NewNextOffset], State);
        {error, eof} ->
            ok;
        Error ->
            Error
    end;
do_compact1(Error,_Metadata,_Props,_State) ->
    Error.


%% @doc Generate a raw file path.
%% @private
-spec(gen_raw_file_path(string()) ->
             string()).
gen_raw_file_path(FilePath) ->
    FilePath ++ "_" ++ integer_to_list(leo_utils:now()).


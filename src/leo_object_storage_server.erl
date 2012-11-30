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

-behaviour(gen_server).

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/4, stop/1]).
-export([put/2, get/4, delete/2, head/2, fetch/3, store/3]).
-export([compact/2, stats/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          id                 :: atom(),
          meta_db_id         :: atom(),
          vnode_id           :: integer(),
          object_storage     :: #backend_info{},
          storage_stats      :: #storage_stats{},
          state_filepath     :: string(),
          num_of_objects = 0 :: integer()
         }).

-record(compact_params, {
          key_bin                :: binary(),
          body_bin               :: binary(),
          next_offset            :: integer(),
          fun_has_charge_of_node :: function()
         }).

-define(AVS_FILE_EXT, ".avs").
-define(DEF_TIMEOUT, 30000).

%%====================================================================
%% API
%%====================================================================
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
-spec(start_link(atom(), integer(), atom(), string()) ->
             ok | {error, any()}).
start_link(Id, SeqNo, MetaDBId, RootPath) ->
    gen_server:start_link({local, Id}, ?MODULE,
                          [Id, SeqNo, MetaDBId, RootPath], []).

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
-spec(put(atom(), #object{}) ->
             ok | {error, any()}).
put(Id, Object) ->
    gen_server:call(Id, {put, Object}, ?DEF_TIMEOUT).


%% @doc Retrieve an object from the object-storage
%%
-spec(get(atom(), binary(), integer(), integer()) ->
             {ok, #metadata{}, #object{}} | not_found | {error, any()}).
get(Id, Key, StartPos, EndPos) ->
    gen_server:call(Id, {get, Key, StartPos, EndPos}, ?DEF_TIMEOUT).


%% @doc Remove an object from the object-storage - (logical-delete)
%%
-spec(delete(atom(), #object{}) ->
             ok | {error, any()}).
delete(Id, Object) ->
    gen_server:call(Id, {delete, Object}, ?DEF_TIMEOUT).


%% @doc Retrieve an object's metadata from the object-storage
%%
-spec(head(atom(), binary()) ->
             {ok, #metadata{}} | {error, any()}).
head(Id, Key) ->
    gen_server:call(Id, {head, Key}, ?DEF_TIMEOUT).


%% @doc Retrieve objects from the object-storage by Key and Function
%%
-spec(fetch(atom(), binary(), function()) ->
             {ok, list()} | {error, any()}).
fetch(Id, Key, Fun) ->
    gen_server:call(Id, {fetch, Key, Fun}, ?DEF_TIMEOUT).


%% @doc Store metadata and data
%%
-spec(store(atom(), #metadata{}, binary()) ->
             ok | {error, any()}).
store(Id, Metadata, Bin) ->
    gen_server:call(Id, {store, Metadata, Bin}, ?DEF_TIMEOUT).


%%--------------------------------------------------------------------
%% API - data-compaction.
%%--------------------------------------------------------------------
%% @doc compaction/start prepare(check disk usage, mk temporary file...)
%%
-spec(compact(atom(), function()) ->
             ok | {error, any()}).
compact(Id, FunHasChargeOfNode) ->
    gen_server:call(Id, {compact, FunHasChargeOfNode}, ?DEF_TIMEOUT).

%%--------------------------------------------------------------------
%% API - get the storage stats
%%--------------------------------------------------------------------
%% @doc get the storage stats specfied by Id which contains number of (active)object and so on.
%%
-spec(stats(atom()) ->
             {ok, #storage_stats{}} |
             {error, any()}).
stats(Id) ->
    gen_server:call(Id, stats, ?DEF_TIMEOUT).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% Function: init(Args) -> {ok, State}          |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
init([Id, SeqNo, MetaDBId, RootPath]) ->
    ObjectStorageDir  = lists:append([RootPath, ?DEF_OBJECT_STORAGE_SUB_DIR]),
    ObjectStoragePath = lists:append([ObjectStorageDir, integer_to_list(SeqNo), ?AVS_FILE_EXT]),
    StateFilePath     = lists:append([RootPath, ?DEF_STATE_SUB_DIR, atom_to_list(Id)]),

    NumOfObjects =
        case file:consult(StateFilePath) of
            {ok, Props} ->
                leo_misc:get_value('num_of_objects', Props, 0);
            _ -> 0
        end,

    %% open object-storage.
    case get_raw_path(object, ObjectStorageDir, ObjectStoragePath) of
        {ok, ObjectStorageRawPath} ->
            case leo_object_storage_haystack:open(ObjectStorageRawPath) of
                {ok, [ObjectWriteHandler, ObjectReadHandler]} ->
                    StorageInfo = #backend_info{file_path     = ObjectStoragePath,
                                                file_path_raw = ObjectStorageRawPath,
                                                write_handler = ObjectWriteHandler,
                                                read_handler  = ObjectReadHandler},
                    {ok, #state{id = Id,
                                meta_db_id     = MetaDBId,
                                object_storage = StorageInfo,
                                state_filepath = StateFilePath,
                                num_of_objects = NumOfObjects
                               }};
                {error, Cause} ->
                    io:format("~w, cause:~p~n", [?LINE, Cause]),
                    {stop, Cause}
            end;
        {error, Cause} ->
            io:format("~w, cause:~p~n", [?LINE, Cause]),
            {stop, Cause}
    end.


handle_call(stop, _From, #state{id = Id,
                                state_filepath = StateFilePath,
                                num_of_objects = NumOfObjects,
                                object_storage = #backend_info{
                                  write_handler  = WriteHandler,
                                  read_handler   = ReadHandler}} = State) ->
    _ = filelib:ensure_dir(StateFilePath),
    _ = leo_file:file_unconsult(StateFilePath, [{id, Id},
                                                {num_of_objects, NumOfObjects}]),
    ok = leo_object_storage_haystack:close(WriteHandler, ReadHandler),
    {stop, shutdown, ok, State};


handle_call({put, Object}, _From, #state{meta_db_id     = MetaDBId,
                                         object_storage = StorageInfo,
                                         num_of_objects = NumOfObjs} = State) ->
    Reply = leo_object_storage_haystack:put(MetaDBId, StorageInfo, Object),

    NewState = after_proc(Reply, State),
    erlang:garbage_collect(self()),

    {reply, Reply, NewState#state{num_of_objects = NumOfObjs + 1}};


handle_call({get, Key, StartPos, EndPos}, _From, #state{meta_db_id     = MetaDBId,
                                                        object_storage = StorageInfo} = State) ->
    Reply = leo_object_storage_haystack:get(MetaDBId, StorageInfo, Key, StartPos, EndPos),

    NewState = after_proc(Reply, State),
    erlang:garbage_collect(self()),

    {reply, Reply, NewState};


handle_call({delete, Object}, _From, #state{meta_db_id     = MetaDBId,
                                            object_storage = StorageInfo,
                                            num_of_objects = NumOfObjs} = State) ->
    Reply = leo_object_storage_haystack:delete(MetaDBId, StorageInfo, Object),

    NewState = after_proc(Reply, State),
    {reply, Reply, NewState#state{num_of_objects = NumOfObjs - 1}};


handle_call({head, Key}, _From, #state{meta_db_id = MetaDBId} = State) ->
    Reply = leo_object_storage_haystack:head(MetaDBId, Key),

    {reply, Reply, State};


handle_call({fetch, Key, Fun}, _From, #state{meta_db_id = MetaDBId} = State) ->
    Reply = leo_object_storage_haystack:fetch(MetaDBId, Key, Fun),

    {reply, Reply, State};


handle_call({store, Metadata, Bin}, _From, #state{meta_db_id     = MetaDBId,
                                                  object_storage = StorageInfo,
                                                  num_of_objects = NumOfObjs} = State) ->
    Reply = leo_object_storage_haystack:store(MetaDBId, StorageInfo, Metadata, Bin),

    {reply, Reply, State#state{num_of_objects = NumOfObjs + 1}};


handle_call(stats, _From, #state{meta_db_id     = _MetaDBId,
                                 object_storage = StorageInfo,
                                 num_of_objects = NumOfObjs} = State) ->
    FilePath = StorageInfo#backend_info.file_path,
    Res = {ok, #storage_stats{file_path   = FilePath,
                              total_num   = NumOfObjs}},
    {reply, Res, State};


handle_call({compact, FunHasChargeOfNode},  _From, #state{meta_db_id = MetaDBId} = State) ->
    {Reply, State1} = compact_fun(State, FunHasChargeOfNode),

    State2 = case do_stats(MetaDBId, State1#state.object_storage) of
                 {ok, #storage_stats{active_num = ActiveObjs}} ->
                     State1#state{num_of_objects = ActiveObjs};
                 {error, _Cause} ->
                     State1#state{num_of_objects = 0}
             end,
    {reply, Reply, State2}.


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
                          object_storage = #backend_info{write_handler = WriteHandler,
                                                         read_handler  = ReadHandler}}) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "terminate/2"},
                           {line, ?LINE}, {body, Id}]),
    ok = leo_object_storage_haystack:close(WriteHandler, ReadHandler),
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
-spec(get_raw_path(object, string(), string()) ->
             {ok, string()} | {error, any()}).
get_raw_path(object, ObjectStorageRootDir, SymLinkPath) ->
    case filelib:ensure_dir(ObjectStorageRootDir) of
        ok ->
            case file:read_link(SymLinkPath) of
                {ok, FileName} ->
                    {ok, FileName};
                {error, enoent} ->
                    RawPath = gen_raw_file_path(SymLinkPath),

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


%% @doc Reduce objects from the object-container.
%% @private
-spec(compact_fun(#state{}, function()) ->
             {ok, #state{}} | {error, any(), #state{}}).
compact_fun(#state{meta_db_id       = MetaDBId,
                   object_storage   = StorageInfo} = State, FunHasChargeOfNode) ->
    FilePath = StorageInfo#backend_info.file_path,

    Res = case calc_remain_disksize(MetaDBId, FilePath) of
              {ok, RemainSize} ->
                  case (RemainSize > 0) of
                      true ->
                          TmpPath = gen_raw_file_path(FilePath),

                          case leo_object_storage_haystack:open(TmpPath) of
                              {ok, [TmpWriteHandler, TmpReadHandler]} ->

                                  case do_stats(MetaDBId, StorageInfo) of
                                      {ok, #storage_stats{active_num = ActiveObjs}} ->
                                          {ok, State#state{object_storage = StorageInfo#backend_info{
                                                                              tmp_file_path_raw = TmpPath,
                                                                              tmp_write_handler = TmpWriteHandler,
                                                                              tmp_read_handler  = TmpReadHandler},
                                                           num_of_objects = ActiveObjs}};
                                      Error ->
                                          {Error, State}
                                  end;
                              Error ->
                                  {Error, State}
                          end;
                      false ->
                          {{error, system_limit}, State}
                  end;
              Error ->
                  {Error, State}
          end,
    compact_fun1(Res, FunHasChargeOfNode).


%% @doc Reduce objects from the object-container.
%% @private
compact_fun1({ok, #state{meta_db_id     = MetaDBId,
                         object_storage = StorageInfo} = State}, FunHasChargeOfNode) ->
    ReadHandler     = StorageInfo#backend_info.read_handler,
    WriteHandler    = StorageInfo#backend_info.write_handler,
    TmpReadHandler  = StorageInfo#backend_info.tmp_read_handler,
    TmpWriteHandler = StorageInfo#backend_info.tmp_write_handler,

    Res = case leo_object_storage_haystack:compact_get(ReadHandler) of
              {ok, Metadata, [_HeaderValue, KeyValue, BodyValue, NextOffset]} ->
                  case leo_backend_db_api:compact_start(MetaDBId) of
                      ok ->
                          CompactParams = #compact_params{key_bin     = KeyValue,
                                                          body_bin    = BodyValue,
                                                          next_offset = NextOffset,
                                                          fun_has_charge_of_node = FunHasChargeOfNode},
                          Ret = do_compact(Metadata, CompactParams, State),
                          _ = leo_object_storage_haystack:close(WriteHandler,    ReadHandler),
                          _ = leo_object_storage_haystack:close(TmpWriteHandler, TmpReadHandler),
                          Ret;
                      Error0 ->
                          Error0
                  end;
              Error1 ->
                  Error1
          end,
    compact_fun2({Res, State});

compact_fun1({Error,_State}, _) ->
    Error.


%% @doc Reduce objects from the object-container.
%% @private
compact_fun2({ok, #state{meta_db_id     = MetaDBId,
                         object_storage = StorageInfo} = State}) ->
    RootPath       = StorageInfo#backend_info.file_path,
    TmpFilePathRaw = StorageInfo#backend_info.tmp_file_path_raw,

    catch file:delete(RootPath),
    case file:make_symlink(TmpFilePathRaw, RootPath) of
        ok ->
            catch file:delete(StorageInfo#backend_info.file_path_raw),

            case leo_object_storage_haystack:open(RootPath) of
                {ok, [NewWriteHandler, NewReadHandler]} ->
                    _ = leo_backend_db_api:compact_end(MetaDBId, true),

                    BackendInfo = State#state.object_storage,
                    NewState    = State#state{object_storage =
                                                  BackendInfo#backend_info{
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

compact_fun2({_Error, #state{meta_db_id     = MetaDBId,
                             object_storage = StorageInfo} = State}) ->
    %% rollback (delete tmp files)
    %%
    catch file:delete(StorageInfo#backend_info.tmp_file_path_raw),
    leo_backend_db_api:compact_end(MetaDBId, false),
    {ok, State}.


%% @doc Calculate remain disk-sizes.
%% @private
-spec(calc_remain_disksize(atom(), string()) ->
             {ok, integer()} | {error, any()}).
calc_remain_disksize(MetaDBId, FilePath) ->
    case leo_file:file_get_mount_path(FilePath) of
        {ok, MountPath} ->
            {ok, MetaDir} = leo_backend_db_api:get_db_raw_filepath(MetaDBId),

            case catch leo_file:file_get_total_size(MetaDir) of
                {'EXIT', Reason} ->
                    {error, Reason};
                MetaSize ->
                    AvsSize = filelib:file_size(FilePath),
                    Remain  = leo_file:file_get_remain_disk(MountPath),
                    {ok, Remain - (AvsSize + MetaSize) * 1.5}
            end;
        Error ->
            Error
    end.


%% @doc Is deleted a record ?
%% @private
-spec(is_deleted_rec(atom(), #metadata{}) ->
             boolean()).
is_deleted_rec(_MetaDBId, #metadata{del = Del}) when Del =/= ?DEL_FALSE ->
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
-spec(do_stats(atom(), #backend_info{}) ->
             {ok, #storage_stats{}} | {error, any()}).
do_stats(MetaDBId, #backend_info{file_path     = RootPath,
                                 read_handler  = ReadHandler}) ->
    case leo_object_storage_haystack:compact_get(ReadHandler) of
        {ok, Metadata, [_HeaderValue, _KeyValue, _BodyValue, NextOffset]} ->
            case do_stats(MetaDBId, ReadHandler, Metadata, NextOffset, #storage_stats{}) of
                {ok, Stats} ->
                    {ok, Stats#storage_stats{file_path   = RootPath,
                                             total_sizes = filelib:file_size(RootPath)}};
                Error ->
                    Error
            end;
        {error, eof} ->
            {ok, #storage_stats{file_path   = RootPath,
                                total_sizes = filelib:file_size(RootPath)}};
        Error ->
            Error
    end.

-spec(do_stats(atom(), pid(), #metadata{}, integer(), #storage_stats{}) ->
             {ok, any()} | {error, any()}).
do_stats(MetaDBId, ReadHandler, Metadata, NextOffset, #storage_stats{total_num  = ObjTotal,
                                                                     active_num = ObjActive} = StorageStats) ->
    NewStorageStats =
        case is_deleted_rec(MetaDBId, Metadata) of
            true  -> StorageStats#storage_stats{total_num  = ObjTotal  + 1};
            false -> StorageStats#storage_stats{total_num  = ObjTotal  + 1,
                                                active_num = ObjActive + 1}
        end,
    case leo_object_storage_haystack:compact_get(ReadHandler, NextOffset) of
        {ok, NewMetadata, [_HeaderValue, _NewKeyValue, _NewBodyValue, NewNextOffset]} ->
            do_stats(MetaDBId, ReadHandler, NewMetadata, NewNextOffset, NewStorageStats);
        {error, eof} ->
            {ok, NewStorageStats};
        Error ->
            Error
    end.


%% @doc Reduce unnecessary objects from object-container.
%% @private
-spec(do_compact(#metadata{}, #compact_params{}, #state{}) ->
             ok | {error, any()}).
do_compact(Metadata, CompactParams, #state{meta_db_id     = MetaDBId,
                                           object_storage = StorageInfo} = State) ->
    FunHasChargeOfNode = CompactParams#compact_params.fun_has_charge_of_node,
    HasChargeOfNode    = FunHasChargeOfNode(CompactParams#compact_params.key_bin),

    case (is_deleted_rec(MetaDBId, Metadata) orelse HasChargeOfNode == false) of
        true ->
            do_compact1(ok, Metadata, CompactParams, State);
        false ->
            %% Insert into the temporary object-container.
            %%
            TmpWriteHandler = StorageInfo#backend_info.tmp_write_handler,

            case leo_object_storage_haystack:compact_put(TmpWriteHandler, Metadata,
                                                         CompactParams#compact_params.key_bin,
                                                         CompactParams#compact_params.body_bin) of
                {ok, Offset} ->
                    NewMeta = Metadata#metadata{offset = Offset},
                    Ret = leo_backend_db_api:compact_put(
                            MetaDBId,
                            term_to_binary({Metadata#metadata.addr_id,
                                            Metadata#metadata.key}),
                            term_to_binary(NewMeta)),
                    do_compact1(Ret, NewMeta, CompactParams, State);
                Error ->
                    do_compact1(Error, Metadata, CompactParams, State)
            end
    end.


%% @doc Reduce unnecessary objects from object-container.
%% @private
do_compact1(ok,_Metadata, CompactParams, #state{object_storage = StorageInfo} = State) ->
    ReadHandler = StorageInfo#backend_info.read_handler,

    case leo_object_storage_haystack:compact_get(ReadHandler, CompactParams#compact_params.next_offset) of
        {ok, NewMetadata, [_HeaderValue, NewKeyValue, NewBodyValue, NewNextOffset]} ->
            do_compact(NewMetadata, CompactParams#compact_params{key_bin     = NewKeyValue,
                                                                 body_bin    = NewBodyValue,
                                                                 next_offset = NewNextOffset},
                       State);
        {error, eof} ->
            ok;
        Error ->
            Error
    end;
do_compact1(Error,_,_,_) ->
    Error.


%% @doc Generate a raw file path.
%% @private
-spec(gen_raw_file_path(string()) ->
             string()).
gen_raw_file_path(FilePath) ->
    lists:append([FilePath, "_", integer_to_list(leo_date:now())]).


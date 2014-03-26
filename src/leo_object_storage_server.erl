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
-export([start_link/4, start_link/5, stop/1]).
-export([put/2, get/4, delete/2, head/2, fetch/4, store/3]).
-export([compact/2, compact_suspend/1, compact_resume/1, stats/1]).
-export([get_avs_version_bin/1]).
-export([head_with_calc_md5/3]).

-ifdef(TEST).
-export([add_incorrect_data/2]).
-endif.

%% To be passed to spawn_link
-export([compact_fun/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          id                  :: atom(),
          meta_db_id          :: atom(),
          compaction_from_pid :: pid(),
          compaction_exec_pid :: pid(),
          object_storage      :: #backend_info{},
          storage_stats       :: #storage_stats{},
          state_filepath      :: string(),
          is_strict_check     :: boolean()
         }).

-record(compact_params, {
          key_bin                :: binary(),
          body_bin               :: binary(),
          next_offset            :: integer(),
          fun_has_charge_of_node :: function(),
          num_of_active_object   :: integer(),
          size_of_active_object  :: integer()
         }).

-define(MAX_COMPACT_HISTORIES, 7).
-define(AVS_FILE_EXT, ".avs").
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
-spec(start_link(atom(), integer(), atom(), string()) ->
             ok | {error, any()}).
start_link(Id, SeqNo, MetaDBId, RootPath) ->
    start_link(Id, SeqNo, MetaDBId, RootPath, false).

-spec(start_link(atom(), integer(), atom(), string(), boolean()) ->
             ok | {error, any()}).
start_link(Id, SeqNo, MetaDBId, RootPath, IsStrictCheck) ->
    gen_server:start_link({local, Id}, ?MODULE,
                          [Id, SeqNo, MetaDBId, RootPath, IsStrictCheck], []).

%% @doc Stop this server
%%
-spec(stop(atom()) -> ok).
stop(Id) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "stop/1"},
                           {line, ?LINE}, {body, Id}]),
    gen_server:call(Id, stop, ?DEF_TIMEOUT).

%% @doc Get AVS format version binary like <<"LeoFS AVS-2.2">>
-spec(get_avs_version_bin(atom()) -> ok).
get_avs_version_bin(Id) ->
    gen_server:call(Id, get_avs_version_bin, ?DEF_TIMEOUT).

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
             {ok, #?METADATA{}} | {error, any()}).
head(Id, Key) ->
    gen_server:call(Id, {head, Key}, ?DEF_TIMEOUT).

%% @doc Retrieve a metada/data from backend_db/object-storage
%%      AND calc MD5 based on the body data
%%
-spec(head_with_calc_md5(atom(), tuple(), any()) ->
             {ok, #?METADATA{}, any()} | {error, any()}).
head_with_calc_md5(Id, Key, MD5Context) ->
    gen_server:call(Id, {head_with_calc_md5, Key, MD5Context}, ?DEF_TIMEOUT).


%% @doc Retrieve objects from the object-storage by Key and Function
%%
-spec(fetch(atom(), binary(), function(), pos_integer()|undefined) ->
             {ok, list()} | {error, any()}).
fetch(Id, Key, Fun, MaxKeys) ->
    gen_server:call(Id, {fetch, Key, Fun, MaxKeys}, ?DEF_TIMEOUT).


%% @doc Store metadata and data
%%
-spec(store(atom(), #?METADATA{}, binary()) ->
             ok | {error, any()}).
store(Id, Metadata, Bin) ->
    gen_server:call(Id, {store, Metadata, Bin}, ?DEF_TIMEOUT).


-ifdef(TEST).
%% @doc Store metadata and data
%%
-spec(add_incorrect_data(atom(), binary()) ->
             ok | {error, any()}).
add_incorrect_data(Id, Bin) ->
    gen_server:call(Id, {add_incorrect_data, Bin}, ?DEF_TIMEOUT).
-endif.


%%--------------------------------------------------------------------
%% API - data-compaction.
%%--------------------------------------------------------------------
%% @doc compaction/start prepare(check disk usage, mk temporary file...)
%%
-spec(compact(atom(), function()) ->
             ok | {error, any()}).
compact(Id, FunHasChargeOfNode) ->
    gen_server:call(Id, {compact, FunHasChargeOfNode}, infinity).

%%--------------------------------------------------------------------
%% API - suspend data-compaction.
%%--------------------------------------------------------------------
%% @doc compaction/suspend
%%
-spec(compact_suspend(atom()) ->
             ok | {error, any()}).
compact_suspend(Id) ->
    gen_server:call(Id, compact_suspend, infinity).

compact_done(#state{id = Id} = NewState) ->
    gen_server:cast(Id, {compact_done, NewState}).

%%--------------------------------------------------------------------
%% API - resume data-compaction.
%%--------------------------------------------------------------------
%% @doc compaction/resume
%%
-spec(compact_resume(atom()) ->
             ok | {error, any()}).
compact_resume(Id) ->
    gen_server:call(Id, compact_resume, infinity).

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
init([Id, SeqNo, MetaDBId, RootPath, IsStrictCheck]) ->
    ObjectStorageDir  = lists:append([RootPath, ?DEF_OBJECT_STORAGE_SUB_DIR]),
    ObjectStoragePath = lists:append([ObjectStorageDir, integer_to_list(SeqNo), ?AVS_FILE_EXT]),
    StateFilePath     = lists:append([RootPath, ?DEF_STATE_SUB_DIR, atom_to_list(Id)]),

    StorageStats =
        case file:consult(StateFilePath) of
            {ok, Props} ->
                #storage_stats{file_path    = ObjectStoragePath,
                               total_sizes  = leo_misc:get_value('total_sizes',  Props, 0),
                               active_sizes = leo_misc:get_value('active_sizes', Props, 0),
                               total_num    = leo_misc:get_value('total_num',    Props, 0),
                               active_num   = leo_misc:get_value('active_num',   Props, 0),
                               compaction_histories = leo_misc:get_value('compaction_histories', Props, []),
                               has_error            = leo_misc:get_value('has_error', Props, false)};
            _ -> #storage_stats{file_path = ObjectStoragePath}
        end,

    %% open object-storage.
    case get_raw_path(object, ObjectStorageDir, ObjectStoragePath) of
        {ok, ObjectStorageRawPath} ->
            case leo_object_storage_haystack:open(ObjectStorageRawPath) of
                {ok, [ObjectWriteHandler, ObjectReadHandler, AVSVsnBin]} ->
                    StorageInfo = #backend_info{file_path           = ObjectStoragePath,
                                                file_path_raw       = ObjectStorageRawPath,
                                                write_handler       = ObjectWriteHandler,
                                                read_handler        = ObjectReadHandler,
                                                avs_version_bin_cur = AVSVsnBin},
                    {ok, #state{id = Id,
                                meta_db_id          = MetaDBId,
                                object_storage      = StorageInfo,
                                storage_stats       = StorageStats,
                                compaction_from_pid = undefined,
                                compaction_exec_pid = undefined,
                                state_filepath      = StateFilePath,
                                is_strict_check     = IsStrictCheck
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

handle_call(get_avs_version_bin, _From, #state{object_storage = StorageInfo} = State) ->
    Reply = {ok, StorageInfo#backend_info.avs_version_bin_cur},
    {reply, Reply, State};

handle_call({put, Object}, _From, #state{meta_db_id     = MetaDBId,
                                         object_storage = StorageInfo,
                                         storage_stats  = StorageStats} = State) ->
    Key = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                           Object#?OBJECT.addr_id,
                           Object#?OBJECT.key),
    {DiffRec, Oldsize} =
        case leo_object_storage_haystack:head(
               MetaDBId, Key) of
            not_found ->
                {1, 0};
            {ok, MetaBin} ->
                Meta = binary_to_term(MetaBin),
                {0, leo_object_storage_haystack:calc_obj_size(Meta)};
            _ ->
                {1, 0}
        end,

    NewSize = leo_object_storage_haystack:calc_obj_size(Object),
    Reply   = leo_object_storage_haystack:put(MetaDBId, StorageInfo, Object),

    NewState = after_proc(Reply, State),
    _ = erlang:garbage_collect(self()),

    NewStorageStats =
        StorageStats#storage_stats{total_sizes  = StorageStats#storage_stats.total_sizes  + NewSize,
                                   active_sizes = StorageStats#storage_stats.active_sizes + (NewSize - Oldsize),
                                   total_num    = StorageStats#storage_stats.total_num    + 1,
                                   active_num   = StorageStats#storage_stats.active_num   + DiffRec},
    {reply, Reply, NewState#state{storage_stats = NewStorageStats}};


handle_call({get, {AddrId, Key}, StartPos, EndPos}, _From, #state{meta_db_id      = MetaDBId,
                                                                  object_storage  = StorageInfo,
                                                                  is_strict_check = IsStrictCheck} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                                  AddrId, Key),
    Reply = leo_object_storage_haystack:get(
              MetaDBId, StorageInfo, BackendKey, StartPos, EndPos, IsStrictCheck),

    NewState = after_proc(Reply, State),
    erlang:garbage_collect(self()),

    {reply, Reply, NewState};


handle_call({delete, Object}, _From, #state{meta_db_id     = MetaDBId,
                                            object_storage = StorageInfo,
                                            storage_stats  = StorageStats} = State) ->
    Key = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                           Object#?OBJECT.addr_id,
                           Object#?OBJECT.key),
    {DiffRec, Oldsize} =
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
        StorageStats#storage_stats{total_sizes  = StorageStats#storage_stats.total_sizes  + NewSize,
                                   active_sizes = StorageStats#storage_stats.active_sizes - (NewSize - Oldsize),
                                   total_num    = StorageStats#storage_stats.total_num    + 1,
                                   active_num   = StorageStats#storage_stats.active_num   + DiffRec},
    {reply, Reply, NewState#state{storage_stats = NewStorageStats}};


handle_call({head, {AddrId, Key}}, _From, #state{meta_db_id = MetaDBId, object_storage = StorageInfo} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                                  AddrId, Key),
    Reply = leo_object_storage_haystack:head(MetaDBId, BackendKey),
    {reply, Reply, State};

handle_call({head_with_calc_md5, {AddrId, Key}, MD5Context}, _From, #state{meta_db_id      = MetaDBId,
                                                                           object_storage  = StorageInfo} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                                  AddrId, Key),
    Reply = leo_object_storage_haystack:head_with_calc_md5(
              MetaDBId, StorageInfo, BackendKey, MD5Context),

    NewState = after_proc(Reply, State),
    erlang:garbage_collect(self()),

    {reply, Reply, NewState};


handle_call({fetch, {AddrId, Key}, Fun, MaxKeys}, _From, #state{meta_db_id     = MetaDBId,
                                                                object_storage = StorageInfo} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                                  AddrId, Key),
    Reply = leo_object_storage_haystack:fetch(MetaDBId, BackendKey, Fun, MaxKeys),
    {reply, Reply, State};


handle_call({store, Metadata, Bin}, _From, #state{meta_db_id     = MetaDBId,
                                                  object_storage = StorageInfo,
                                                  storage_stats  = StorageStats} = State) ->
    BackendKey = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                                  Metadata#?METADATA.addr_id,
                                  Metadata#?METADATA.key),
    {DiffRec, Oldsize} =
        case leo_object_storage_haystack:head(
               MetaDBId, BackendKey) of
            not_found ->
                {1, 0};
            {ok, MetaBin} ->
                Meta = binary_to_term(MetaBin),
                {0, leo_object_storage_haystack:calc_obj_size(Meta)};
            _ ->
                {1, 0}
        end,

    NewSize = leo_object_storage_haystack:calc_obj_size(Metadata),
    Reply   = leo_object_storage_haystack:store(MetaDBId, StorageInfo, Metadata, Bin),
    NewStorageStats =
        StorageStats#storage_stats{
          total_sizes  = StorageStats#storage_stats.total_sizes  + NewSize,
          active_sizes = StorageStats#storage_stats.active_sizes + (NewSize - Oldsize),
          total_num    = StorageStats#storage_stats.total_num    + 1,
          active_num   = StorageStats#storage_stats.active_num   + DiffRec},
    {reply, Reply, State#state{storage_stats = NewStorageStats}};

handle_call(stats, _From, #state{storage_stats = StorageStats} = State) ->
    {reply, {ok, StorageStats}, State};

handle_call(compact_suspend,  _From, #state{compaction_exec_pid = undefined} = State) ->
    {reply, {error, ?ERROR_COMPACT_SUSPEND_FAILURE}, State};

handle_call(compact_suspend,  _From, #state{meta_db_id          = MetaDBId,
                                            compaction_exec_pid = Pid} = State) ->
    leo_backend_db_api:compact_suspend(MetaDBId),
    erlang:send(Pid, compact_suspend),
    {reply, ok, State};

handle_call(compact_resume,  _From, #state{compaction_exec_pid = undefined} = State) ->
    {reply, {error, ?ERROR_COMPACT_RESUME_FAILURE}, State};

handle_call(compact_resume,  _From, #state{meta_db_id          = MetaDBId,
                                           compaction_exec_pid = Pid} = State) ->
    leo_backend_db_api:compact_resume(MetaDBId),
    erlang:send(Pid, compact_resume),
    {reply, ok, State};

handle_call({compact, FunHasChargeOfNode}, {FromPid, _FromRef},
            #state{object_storage = StorageInfo} = State0) ->
    State1 = State0#state{compaction_from_pid = FromPid},
    Pid = spawn_link(?MODULE, compact_fun,
                     [State1#state{object_storage = StorageInfo#backend_info{
                                                      write_handler = undefined,
                                                      read_handler  = undefined
                                                     }}, FunHasChargeOfNode]),
    {reply, ok, State1#state{compaction_exec_pid = Pid}};


handle_call({add_incorrect_data,_Bin}, _From, #state{object_storage =_StorageInfo} = State) ->
    ?add_incorrect_data(_StorageInfo,_Bin),
    {reply, ok, State}.


%% Function: handle_cast(Msg, State) -> {noreply, State}          |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast message
handle_cast({compact_done, NewState},  #state{object_storage      = StorageInfo,
                                              compaction_from_pid = From} = State) ->
    FilePath     = StorageInfo#backend_info.file_path,
    ReadHandler  = StorageInfo#backend_info.read_handler,
    WriteHandler = StorageInfo#backend_info.write_handler,
    ok = leo_object_storage_haystack:close(WriteHandler, ReadHandler),
    NewState2 = case leo_object_storage_haystack:open(FilePath) of
                    {ok, [NewWriteHandler, NewReadHandler, AVSVsnBin]} ->
                        BackendInfo = NewState#state.object_storage,
                        NewState#state{object_storage = BackendInfo#backend_info{
                                                          avs_version_bin_cur = AVSVsnBin,
                                                          write_handler       = NewWriteHandler,
                                                          read_handler        = NewReadHandler}};
                    {error, _} ->
                        State
                end,
    erlang:send(From, done),
    {noreply, NewState2#state{compaction_from_pid = undefined, compaction_exec_pid = undefined}};
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
                          state_filepath = StateFilePath,
                          storage_stats  = StorageStats,
                          object_storage = #backend_info{write_handler = WriteHandler,
                                                         read_handler  = ReadHandler}}) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "terminate/2"},
                           {line, ?LINE}, {body, Id}]),

    _ = filelib:ensure_dir(StateFilePath),
    _ = leo_file:file_unconsult(
          StateFilePath,
          [{id, Id},
           {total_sizes,  StorageStats#storage_stats.total_sizes},
           {active_sizes, StorageStats#storage_stats.active_sizes},
           {total_num,    StorageStats#storage_stats.total_num},
           {active_num,   StorageStats#storage_stats.active_num},
           {compaction_histories, StorageStats#storage_stats.compaction_histories},
           {has_error,            StorageStats#storage_stats.has_error}]),
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
                {ok, [NewWriteHandler, NewReadHandler, AVSVsnBin]} ->
                    BackendInfo = State#state.object_storage,
                    State#state{object_storage = BackendInfo#backend_info{
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


%% @doc compact objects from the object-container on a independent process.
%% @private
-spec(compact_fun(#state{}, function()) ->
             ok).
compact_fun(#state{meta_db_id       = MetaDBId,
                   object_storage   = StorageInfo} = State, FunHasChargeOfNode) ->
    FilePath = StorageInfo#backend_info.file_path,
    OrigFilePath = StorageInfo#backend_info.file_path_raw,

    Res = case calc_remain_disksize(MetaDBId, FilePath) of
              {ok, RemainSize} ->
                  case (RemainSize > 0) of
                      true ->
                          TmpPath = gen_raw_file_path(FilePath),
                          %% must reopen the original file when handling at another process
                          case leo_object_storage_haystack:open(TmpPath) of
                              {ok, [TmpWriteHandler, TmpReadHandler, AVSVsnBinCur]} ->
                                  case leo_object_storage_haystack:open(OrigFilePath) of
                                      {ok, [WriteHandler, ReadHandler, AVSVsnBinPrv]} ->
                                          FileSize = filelib:file_size(OrigFilePath),
                                          ok = file:advise(TmpWriteHandler, 0, FileSize, dont_need),
                                          ok = file:advise(ReadHandler, 0, FileSize, sequential),
                                          {ok, State#state{
                                                 object_storage = StorageInfo#backend_info{
                                                                    avs_version_bin_cur = AVSVsnBinCur,
                                                                    avs_version_bin_prv = AVSVsnBinPrv,
                                                                    tmp_file_path_raw   = TmpPath,
                                                                    write_handler       = WriteHandler,
                                                                    read_handler        = ReadHandler,
                                                                    tmp_write_handler   = TmpWriteHandler,
                                                                    tmp_read_handler    = TmpReadHandler}}};
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
    {_Ret, NewState} = try compact_fun1(Res, FunHasChargeOfNode) of
                           {R, S} ->
                               {R, S};
                           Other ->
                               {Other, State}
                       catch _:Reason ->
                               error_logger:error_msg("~p,~p,~p,~p~n",
                                                      [{module, ?MODULE_STRING}, {function, "compact_fun/2"},
                                                       {line, ?LINE},
                                                       {body, {MetaDBId, Reason}}]),
                               {Reason, State}
                       end,
    compact_done(NewState).


%% @doc Reduce objects from the object-container.
%% @private
compact_fun1({ok, #state{meta_db_id     = MetaDBId,
                         storage_stats  = StorageStats,
                         object_storage = StorageInfo} = State}, FunHasChargeOfNode) ->
    ReadHandler     = StorageInfo#backend_info.read_handler,
    WriteHandler    = StorageInfo#backend_info.write_handler,
    TmpReadHandler  = StorageInfo#backend_info.tmp_read_handler,
    TmpWriteHandler = StorageInfo#backend_info.tmp_write_handler,

    NewHist = compact_add_history(start, StorageStats#storage_stats.compaction_histories),
    Res = case catch leo_object_storage_haystack:compact_get(ReadHandler) of
              {ok, Metadata, [_HeaderValue, KeyValue, BodyValue, NextOffset]} ->
                  case leo_backend_db_api:compact_start(MetaDBId) of
                      ok ->
                          CompactParams = #compact_params{key_bin     = KeyValue,
                                                          body_bin    = BodyValue,
                                                          next_offset = NextOffset,
                                                          num_of_active_object   = 0,
                                                          size_of_active_object  = 0,
                                                          fun_has_charge_of_node = FunHasChargeOfNode},
                          try do_compact(Metadata, CompactParams, State) of
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
                      Error0 ->
                          Error0
                  end;
              Error1 ->
                  Error1
          end,

    catch leo_object_storage_haystack:close(WriteHandler,    ReadHandler),
    catch leo_object_storage_haystack:close(TmpWriteHandler, TmpReadHandler),

    %% @TODO add history(end datetime)
    NewHist2 = compact_add_history(finish, NewHist),
    NewState = State#state{storage_stats = StorageStats#storage_stats{compaction_histories = NewHist2},
                           object_storage = StorageInfo#backend_info{
                                              read_handler      = undefined,
                                              write_handler     = undefined,
                                              tmp_read_handler  = undefined,
                                              tmp_write_handler = undefined
                                             }},
    compact_fun2({Res, NewState});

compact_fun1({Error,_State}, _) ->
    {Error,_State}.


%% @doc Reduce objects from the object-container.
%% @private
compact_fun2({{ok, NumActive, SizeActive},
              #state{meta_db_id     = MetaDBId,
                     storage_stats  = StorageStats,
                     object_storage = StorageInfo} = State}) ->
    RootPath       = StorageInfo#backend_info.file_path,
    TmpFilePathRaw = StorageInfo#backend_info.tmp_file_path_raw,

    catch file:delete(RootPath),
    case file:make_symlink(TmpFilePathRaw, RootPath) of
        ok ->
            ok = file:delete(StorageInfo#backend_info.file_path_raw),
            %% must reopen the original file when handling at another process
            %% so we don't open here
            _ = leo_backend_db_api:compact_end(MetaDBId, true),
            BackendInfo = State#state.object_storage,
            NewState    = State#state{storage_stats =
                                          StorageStats#storage_stats{
                                            total_num    = NumActive,
                                            active_num   = NumActive,
                                            total_sizes  = SizeActive,
                                            active_sizes = SizeActive},
                                      object_storage =
                                          BackendInfo#backend_info{
                                            file_path_raw = TmpFilePathRaw
                                           }},
            {ok, NewState};
        {error, Cause} ->
            leo_backend_db_api:compact_end(MetaDBId, false),
            NewState = State#state{storage_stats = StorageStats#storage_stats{
                                                     has_error = true}},
            {{error, Cause}, NewState}
    end;

compact_fun2({_Error, #state{meta_db_id     = MetaDBId,
                             storage_stats  = StorageStats,
                             object_storage = StorageInfo} = State}) ->
    %% rollback (delete tmp files)
    %%
    NewState = State#state{storage_stats = StorageStats#storage_stats{
                                             has_error = true}},
    %% must reopen the original file when handling at another process
    %% so we don't open here
    catch file:delete(StorageInfo#backend_info.tmp_file_path_raw),
    leo_backend_db_api:compact_end(MetaDBId, false),
    {ok, NewState}.


%% @doc add compaction history
-spec(compact_add_history(atom(), compaction_histories()) -> compaction_histories()).
compact_add_history(start, Histories) when is_list(Histories) ->
    NewHist = case length(Histories) < ?MAX_COMPACT_HISTORIES of
                  true -> Histories;
                  false ->
                      Last = lists:last(Histories),
                      lists:delete(Last, Histories)
              end,
    [{leo_date:now(), 0}|NewHist];
compact_add_history(finish, [{Start, _}|Histories]) ->
    [{Start, leo_date:now()}|Histories].


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
-spec(is_deleted_rec(atom(), #backend_info{}, #?METADATA{}) ->
             boolean()).
is_deleted_rec(_MetaDBId, _StorageInfo, #?METADATA{del = Del}) when Del =/= ?DEL_FALSE ->
    true;
is_deleted_rec(MetaDBId, #backend_info{avs_version_bin_prv = AVSVsnBinPrv} = StorageInfo,
               #?METADATA{key      = Key,
                          addr_id  = AddrId} = MetaFromAvs) ->
    KeyOfMetadata = ?gen_backend_key(AVSVsnBinPrv, AddrId, Key),
    case leo_backend_db_api:get(MetaDBId, KeyOfMetadata) of
        {ok, MetaOrg} ->
            MetaOrgTerm = binary_to_term(MetaOrg),
            is_deleted_rec(MetaDBId, StorageInfo, MetaFromAvs, MetaOrgTerm);
        not_found ->
            true;
        _Other ->
            false
    end.

%% @private
-spec(is_deleted_rec(atom(), #backend_info{}, #?METADATA{}, #?METADATA{}) ->
             boolean()).
is_deleted_rec(_MetaDBId,_StorageInfo,
               _Meta,
               #?METADATA{del = Del}) when Del /= 0 ->
    true;
is_deleted_rec(_MetaDBId,_StorageInfo,
               #?METADATA{offset = Offset_1},
               #?METADATA{offset = Offset_2}) when Offset_1 /= Offset_2 ->
    true;
is_deleted_rec(_MetaDBId,_StorageInfo,_Meta_1,_Meta_2) ->
    false.


%% @doc Reduce unnecessary objects from object-container.
%% @private
-spec(do_compact(#?METADATA{}, #compact_params{}, #state{}) ->
             ok | {error, any()}).
do_compact(Metadata, CompactParams, #state{meta_db_id     = MetaDBId,
                                           object_storage = StorageInfo} = State) ->
    %% check mailbox regularly
    receive
        compact_suspend ->
            receive
                compact_resume ->
                    void
            end
    after
        0 ->
            void
    end,

    %% retrieve value
    FunHasChargeOfNode = CompactParams#compact_params.fun_has_charge_of_node,
    NumActive          = CompactParams#compact_params.num_of_active_object,
    SizeActive         = CompactParams#compact_params.size_of_active_object,

    %% set a flag of object of compaction
    HasChargeOfNode = FunHasChargeOfNode(Metadata),

    %% execute compaction
    case (is_deleted_rec(MetaDBId, StorageInfo, Metadata)
          orelse HasChargeOfNode == false) of
        true ->
            do_comapct_1(ok, Metadata, CompactParams, State);
        false ->
            %% Insert into the temporary object-container.
            %%
            TmpWriteHandler = StorageInfo#backend_info.tmp_write_handler,

            case leo_object_storage_haystack:compact_put(TmpWriteHandler, Metadata,
                                                         CompactParams#compact_params.key_bin,
                                                         CompactParams#compact_params.body_bin) of
                {ok, Offset} ->
                    NewMeta = Metadata#?METADATA{offset = Offset},
                    KeyOfMetadata = ?gen_backend_key(StorageInfo#backend_info.avs_version_bin_cur,
                                                     Metadata#?METADATA.addr_id,
                                                     Metadata#?METADATA.key),
                    Ret = leo_backend_db_api:compact_put(
                            MetaDBId, KeyOfMetadata, term_to_binary(NewMeta)),

                    ObjectSize = leo_object_storage_haystack:calc_obj_size(NewMeta),
                    NewCompactParams = CompactParams#compact_params{
                                         num_of_active_object  = NumActive  + 1,
                                         size_of_active_object = SizeActive + ObjectSize},

                    do_comapct_1(Ret, NewMeta, NewCompactParams, State);
                Error ->
                    do_comapct_1(Error, Metadata, CompactParams, State)
            end
    end.


%% @doc Reduce unnecessary objects from object-container.
%% @private
do_comapct_1(ok, Metadata, CompactParams, #state{object_storage = StorageInfo} = State) ->
    ReadHandler = StorageInfo#backend_info.read_handler,

    case leo_object_storage_haystack:compact_get(ReadHandler, CompactParams#compact_params.next_offset) of
        {ok, NewMetadata, [_HeaderValue, NewKeyValue, NewBodyValue, NewNextOffset]} ->
            do_compact(NewMetadata,
                       CompactParams#compact_params{key_bin     = NewKeyValue,
                                                    body_bin    = NewBodyValue,
                                                    next_offset = NewNextOffset},
                       State);
        {error, eof} ->
            NumOfAcriveObjs  = CompactParams#compact_params.num_of_active_object,
            SizeOfActiveObjs = CompactParams#compact_params.size_of_active_object,
            {ok, NumOfAcriveObjs, SizeOfActiveObjs};
        {error, Cause} when Cause =:= ?ERROR_INVALID_DATA orelse
                            Cause =:= ?ERROR_DATA_SIZE_DID_NOT_MATCH ->
            %% retry until eof
            OldOffset = CompactParams#compact_params.next_offset,
            do_comapct_1(ok, Metadata,
                         CompactParams#compact_params{next_offset = OldOffset + 1},
                         State);
        Error ->
            Error
    end;
do_comapct_1(Error,_,_,_) ->
    Error.


%% @doc Generate a raw file path.
%% @private
-spec(gen_raw_file_path(string()) ->
             string()).
gen_raw_file_path(FilePath) ->
    lists:append([FilePath, "_", integer_to_list(leo_date:now())]).

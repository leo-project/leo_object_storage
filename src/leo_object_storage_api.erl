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
%% Leo Object Storage - API
%% @doc
%% @end
%%======================================================================
-module(leo_object_storage_api).

-author('Yosuke Hara').

-include("leo_object_storage.hrl").

-export([start/2,
         put/2, get/1, get/3, delete/2, head/1,
         fetch_by_addr_id/2, fetch_by_key/2,
         compact/0, stats/0,
         add_container/1, remove_container/1
        ]).


-define(ETS_CONTAINERS_TABLE, 'leo_object_storage_containers').
-define(ETS_INFO_TABLE,       'leo_object_storage_info').
-define(SERVER_MODULE,        'leo_object_storage_server').


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Create object-storage processes
%%
-spec(start(list(), string()) ->
             ok | {error, any()}).
start([], []) ->
    {error, badarg};
start(_, []) ->
    {error, badarg};
start([], _) ->
    {error, badarg};

start(NumOfContainers, Path0) ->
    start_app(),

    Path1     = get_path(Path0),
    Storage1  = get_object_storage_mod(),
    Metadata1 = get_metadata_db(),
    true = ets:insert(?ETS_INFO_TABLE, {?MODULE, [{num_of_containers, NumOfContainers},
                                                  {path, Path1},
                                                  {storage_mod, Storage1},
                                                  {metadata_db, Metadata1}]}),

    %% Generate process of object-storage-containers.
    %%
    lists:foreach(fun(I) ->
                          add_container(I-1)
                  end, lists:seq(1, NumOfContainers)),

    %% Launch a supervisor.
    %%
    case whereis(leo_object_storage_sup) of
        undefined ->
            {error, "NOT started supervisor"};
        SupRef ->
            case supervisor:count_children(SupRef) of
                [{specs, _},{active, Active},
                 {supervisors, _},{workers, Workers}] when Active == Workers  ->
                    ok;
                _ ->
                    {error, "Could NOT launch worker processes"}
            end
    end.


%% @doc Insert an object into the object-storage
%% @param Key = {$VNODE_ID, $OBJ_KEY}
%%
-spec(put(tuple(), pid()) ->
             ok | {error, any()}).
put(Key, ObjectPool) ->
    do_request(put, [Key, ObjectPool]).


%% @doc Retrieve an object and a metadata from the object-storage
%%
-spec(get(tuple()) ->
             {ok, list()} | not_found | {error, any()}).
get(Key) ->
    get(Key, 0, 0).

-spec(get(tuple(), integer(), integer()) ->
             {ok, list()} | not_found | {error, any()}).
get(Key, StartPos, EndPos) ->
    do_request(get, [Key, StartPos, EndPos]).


%% @doc Remove an object from the object-storage
%%
-spec(delete(tuple(), pid()) ->
             ok | {error, any()}).
delete(Key, ObjectPool) ->
    do_request(delete, [Key, ObjectPool]).


%% @doc Retrieve a metadata from the object-storage
%%
-spec(head(tuple()) ->
             {ok, metadata} | {error, any()}).
head(Key) ->
    do_request(head, [Key]).


%% @doc Fetch objects by ring-address-id
%%
-spec(fetch_by_addr_id(integer(), function()) ->
             {ok, list()} | not_found).
fetch_by_addr_id(AddrId, Fun) ->
    case get_object_storage_pid(all) of
        undefined ->
            not_found;
        List ->
            Res = lists:foldl(
                    fun(Id, Acc) ->
                            case ?SERVER_MODULE:fetch(Id, term_to_binary({AddrId, []}), Fun) of
                                {ok, Values} ->
                                    [Values|Acc];
                                _ ->
                                    Acc
                            end
                    end, [], List),
            {ok, lists:reverse(lists:flatten(Res))}
    end.


%% @doc Fetch objects by key (object-name)
%%
-spec(fetch_by_key(string(), function()) ->
             {ok, list()} | not_found).
fetch_by_key(Key, Fun) ->
    case get_object_storage_pid(all) of
        undefined ->
            not_found;
        List ->
            Res = lists:foldl(
                    fun(Id, Acc) ->
                            case ?SERVER_MODULE:fetch(Id, term_to_binary({0, Key}), Fun) of
                                {ok, Values} ->
                                    [Values|Acc];
                                _ ->
                                    Acc
                            end
                    end, [], List),
            {ok, lists:reverse(lists:flatten(Res))}
    end.


%% @doc Compact object-storage and metadata
-spec(compact() ->
             ok | list()).
compact() ->
    case get_object_storage_pid(all) of
        undefined ->
            void;
        List ->
            lists:foldl(
              fun(Id, Acc) ->
                      ok = application:set_env(?APP_NAME, Id, running),
                      NewAcc = [?SERVER_MODULE:compact(Id)|Acc],
                      ok = application:set_env(?APP_NAME, Id, idle),
                      NewAcc
              end, [], List)
    end.

%% @doc Retrieve the storage stats
-spec(stats() ->
             {ok, list()} | not_found).
stats() ->
    case get_object_storage_pid(all) of
        undefined ->
            not_found;
        List ->
            {ok, lists:reverse(
                   lists:foldl(fun(Id, Acc) ->
                                       [?SERVER_MODULE:stats(Id)|Acc]
                               end, [], List))}
    end.


%% @doc Add an object storage container into
%%
-spec(add_container(integer()) ->
             ok).
add_container(Id) ->
    case ets:lookup(?ETS_INFO_TABLE, ?MODULE) of
        [] -> {error, not_initialized};
        [{_, Props}|_] ->
            add_container_1(Id, Props)
    end.

-spec(add_container_1(integer(), list()) ->
             ok).
add_container_1(Id0, Props) ->
    Id1 = gen_id(obj_storage, Id0),
    Id2 = gen_id(metadata,    Id0),

    Path       = proplists:get_value('path',        Props),
    StorageMod = proplists:get_value('storage_mod', Props),
    MetadataDB = proplists:get_value('metadata_db', Props),

    Args = [Id1, Id0, Id2, StorageMod, Path],
    ChildSpec = {Id1,
                 {leo_object_storage_server, start_link, Args},
                 permanent, 2000, worker, [leo_object_storage_server]},

    case supervisor:start_child(leo_object_storage_sup, ChildSpec) of
        {ok, _Pid} ->
            ok = leo_backend_db_api:new(Id2, 1, MetadataDB,
                                        Path ++ ?DEF_METADATA_STORAGE_SUB_DIR ++ integer_to_list(Id0)),
            true = ets:insert(?ETS_CONTAINERS_TABLE, {Id0, [{obj_storage, Id1},
                                                            {metadata,    Id2}]}),
            ok;
        Error ->
            io:format("[ERROR] ~p~n",[Error])
    end.


%% @doc Remove an object storage container from
%%
-spec(remove_container(integer()) ->
             ok).
remove_container(Id) ->
    case ets:lookup(?ETS_CONTAINERS_TABLE, Id) of
        [] -> {error, not_found};
        [{_, Info}|_] ->
            Id1 = proplists:get_value(obj_storage, Info),
            Id2 = proplists:get_value(metadata,    Info),

            case supervisor:terminate_child(leo_object_storage_sup, Id1) of
                ok ->
                    leo_backend_db_api:stop(Id2),
                    supervisor:delete_child(leo_object_storage_sup, Id1);
                Error ->
                    Error
            end
    end.


%%--------------------------------------------------------------------
%% INNTERNAL FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Launch the object storage application
%% @private
-spec(start_app() ->
             ok | {error, any()}).
start_app() ->
    Module = leo_object_storage,
    case application:start(Module) of
        ok ->
            ?ETS_CONTAINERS_TABLE = ets:new(?ETS_CONTAINERS_TABLE,
                                            [named_table, ordered_set, public, {read_concurrency, true}]),
            ?ETS_INFO_TABLE       = ets:new(?ETS_INFO_TABLE,
                                            [named_table, set, public, {read_concurrency, true}]),
            ok;
        {error, {already_started, Module}} ->
            ok;
        Error ->
            Error
    end.


%% %% @doc Retrieve object-store directory
%% %% @private
-spec(get_path(string()) ->
             string()).
get_path(Path0) ->
    {ok, Curr} = file:get_cwd(),

    Path1 = case Path0 of
                "/"   ++ _Rest -> Path0;
                "../" ++ _Rest -> Path0;
                "./"  ++  Rest -> Curr ++ "/" ++ Rest;
                _              -> Curr ++ "/" ++ Path0
            end,

    Path2 = case (string:len(Path1) == string:rstr(Path1, "/")) of
                true  -> Path1;
                false -> Path1 ++ "/"
            end,
    Path2.


%% %% @doc Retrieve an object storage module
%% %% @private
-spec(get_object_storage_mod() ->
             atom()).
get_object_storage_mod() ->
    case application:get_env(?APP_NAME, object_storage) of
        {ok, haystack} -> leo_object_storage_haystack;
        _ ->              leo_object_storage_haystack
    end.


%% %% @doc Retrieve a metadata-db
%% %% @private
-spec(get_metadata_db() ->
             atom()).
get_metadata_db() ->
    case application:get_env(?APP_NAME, metadata_storage) of
        {ok, Metadata0} -> Metadata0;
        _ ->               ?DEF_METADATA_DB
    end.


%% @doc Retrieve an object storage process-id
%% @private
-spec(get_object_storage_pid(all | integer()) ->
             atom()).
get_object_storage_pid(Arg) ->
    Ret = ets:tab2list(?ETS_CONTAINERS_TABLE),
    get_object_storage_pid(Ret, Arg).

get_object_storage_pid([], _) ->
    undefined;

get_object_storage_pid(List, all) ->
    lists:map(fun({_, Value}) ->
                      Id = proplists:get_value(obj_storage, Value),
                      Id
              end, List);

get_object_storage_pid(List, Arg) ->
    Index = (erlang:crc32(Arg) rem erlang:length(List)) + 1,
    {_, Value} = lists:nth(Index, List),
    Id = proplists:get_value(obj_storage, Value),
    Id.


%% @doc Retrieve the status of object of pid
%% @private
-spec(get_pid_status(pid()) -> running | idle ).
get_pid_status(Pid) ->
    case application:get_env(?APP_NAME, Pid) of
        undefined ->
            idle;
        {ok, Status} ->
            Status
    end.


%% @doc Request an operation
%% @private
-spec(do_request(type_of_method(), list()) ->
             ok | {ok, list()} | {error, any()}).
do_request(get, [Key, StartPos, EndPos]) ->
    KeyBin = term_to_binary(Key),
    ?SERVER_MODULE:get(get_object_storage_pid(KeyBin), KeyBin, StartPos, EndPos);

do_request(put, [Key, ObjectPool]) ->
    KeyBin = term_to_binary(Key),
    Id = get_object_storage_pid(KeyBin),

    case get_pid_status(Id) of
        idle ->
            ?SERVER_MODULE:put(get_object_storage_pid(KeyBin), ObjectPool);
        running ->
            {error, doing_compaction}
    end;
do_request(delete, [Key, ObjectPool]) ->
    KeyBin = term_to_binary(Key),
    Id = get_object_storage_pid(KeyBin),

    case get_pid_status(Id) of
        idle ->
            ?SERVER_MODULE:delete(get_object_storage_pid(KeyBin), ObjectPool);
        running ->
            {error, doing_compaction}
    end;
do_request(head, [Key]) ->
    KeyBin = term_to_binary(Key),
    ?SERVER_MODULE:head(get_object_storage_pid(KeyBin), KeyBin).


%% @doc Generate Id for obj-storage or metadata
%% @private
-spec(gen_id(obj_storage | metadata, integer()) ->
             atom()).
gen_id(obj_storage, Id) ->
    list_to_atom(atom_to_list(?APP_NAME)
                 ++ "_"
                 ++ integer_to_list(Id));
gen_id(metadata, Id) ->
    list_to_atom("metadata"
                 ++ "_"
                 ++ integer_to_list(Id)).


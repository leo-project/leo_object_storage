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
-vsn('0.9.1').

-include("leo_object_storage.hrl").

-export([new/3,
         put/2, get/1, get/3, delete/2, head/1,
         fetch_by_addr_id/2, fetch_by_key/2,
         stats/0, compact/0]).

-define(ETS_TABLE_NAME, 'leo_object_storage_pd').
-define(SERVER_MODULE,  'leo_object_storage_server').
-define(PD_KEY_WORKERS, 'object_storage_workers').

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Create a storage-processes
%%
-spec(new(integer(), integer(), string()) ->
             ok | {error, any()}).
new(_, 0, _) ->
    {error, badarg};
new(_, _, []) ->
    {error, badarg};
new(DeviceNumber, NumOfStorages, Path0) ->
    io:format("~w:~w - ~w ~w ~p~n",[?MODULE, ?LINE, DeviceNumber, NumOfStorages, Path0]),
    ok = start_app(),

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

    ObjStorage1 =
        case application:get_env(?APP_NAME, object_storage) of
            {ok, ObjectStorage} ->
                object_storage_module(ObjectStorage);
            _ ->
                object_storage_module(?DEF_OBJECT_STORAGE)
        end,

    MetaDB1 =
        case application:get_env(?APP_NAME, metadata_storage) of
            {ok, MetaDB} ->
                MetaDB;
            _ ->
                ?DEF_METADATA_DB
        end,

    Ret = lists:map(
            fun(StorageNumber) ->
                    Id = list_to_atom(atom_to_list(?APP_NAME)
                                      ++ "_" ++ integer_to_list(DeviceNumber)
                                      ++ "_" ++ integer_to_list(StorageNumber)),
                    MetaDBId = list_to_atom("metadata"
                                            ++ "_" ++ integer_to_list(DeviceNumber)
                                            ++ "_" ++ integer_to_list(StorageNumber)),

                    case supervisor:start_child(leo_object_storage_sup,
                                                [Id, MetaDBId, DeviceNumber, StorageNumber, ObjStorage1, Path2]) of
                        {ok, _Pid} ->
                            ok = leo_backend_db_api:new(MetaDBId, 1, MetaDB1,
                                                        Path2
                                                        ++ ?DEF_METADATA_STORAGE_SUB_DIR
                                                        ++ integer_to_list(StorageNumber)),
                            Id;
                        Error ->
                            io:format("[ERROR] ~p~n",[Error]),
                            []
                    end
            end, lists:seq(0, NumOfStorages-1)),

    case whereis(leo_object_storage_sup) of
        undefined ->
            {error, "NOT started supervisor"};
        SupRef ->
            case supervisor:count_children(SupRef) of
                [{specs,_},{active,Active},{supervisors,_},{workers,Workers}] when Active == Workers  ->
                    case ets:lookup(?ETS_TABLE_NAME, ?PD_KEY_WORKERS) of
                        [] ->
                            true = ets:insert(?ETS_TABLE_NAME, {?PD_KEY_WORKERS, Ret});
                        [{?PD_KEY_WORKERS, List}] ->
                            true = ets:delete(?ETS_TABLE_NAME, ?PD_KEY_WORKERS),
                            true = ets:insert(?ETS_TABLE_NAME, {?PD_KEY_WORKERS, List ++ Ret})
                    end,
                    ok;
                _ ->
                    {error, "Could NOT started worker processes"}
            end
    end.


%% @doc Insert an object into the object-storage
%% @param KeyBin = <<{$VNODE_ID, $OBJ_KEY}>>
%%
-spec(put(binary(), pid()) ->
             ok | {error, any()}).
put(KeyBin, ObjectPool) ->
    do_request(put, [KeyBin, ObjectPool]).


%% @doc Retrieve an object and a metadata from the object-storage
%%
-spec(get(binary()) ->
             {ok, list()} | not_found | {error, any()}).
get(KeyBin) ->
    get(KeyBin, -1, -1).

-spec(get(binary(), integer(), integer()) ->
             {ok, list()} | not_found | {error, any()}).
get(KeyBin, StartPos, EndPos) ->
    do_request(get, [KeyBin, StartPos, EndPos]).


%% @doc Remove an object from the object-storage
%%
-spec(delete(binary(), pid()) ->
             ok | {error, any()}).
delete(KeyBin, ObjectPool) ->
    do_request(delete, [KeyBin, ObjectPool]).


%% @doc Retrieve a metadata from the object-storage
%%
-spec(head(KeyBin::binary()) ->
             {ok, metadata} | {error, any()}).
head(KeyBin) ->
    do_request(head, [KeyBin]).


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
            ?ETS_TABLE_NAME = ets:new(?ETS_TABLE_NAME, [named_table, public, {read_concurrency, true}]),
            ok;
        {error, {already_started, Module}} ->
            ok;
        Error ->
            Error
    end.


%% @doc Retrieve an object storage module name
%% @private
-spec(object_storage_module(atom()) ->
             atom()).
object_storage_module(haystack) ->
    leo_object_storage_haystack;
object_storage_module(_) ->
    undefined.


%% @doc Retrieve an object storage process-id
%% @private
-spec(get_object_storage_pid(all | binary()) ->
             atom()).
get_object_storage_pid(Arg) ->
    case ets:lookup(?ETS_TABLE_NAME, ?PD_KEY_WORKERS) of
        [] ->
            undefined;
        [{?PD_KEY_WORKERS, List}] when Arg == all ->
            lists:map(fun(Id) -> Id end, List);
        [{?PD_KEY_WORKERS, List}] ->
            Index = (erlang:crc32(Arg) rem erlang:length(List)) + 1,
            Id = lists:nth(Index, List),
            Id
    end.


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
do_request(get, [KeyBin, StartPos, EndPos]) ->
    ?SERVER_MODULE:get(get_object_storage_pid(KeyBin), KeyBin, StartPos, EndPos);

do_request(put, [KeyBin, ObjectPool]) ->
    Id = get_object_storage_pid(KeyBin),
    case get_pid_status(Id) of
        idle ->
            ?SERVER_MODULE:put(get_object_storage_pid(KeyBin), ObjectPool);
        running ->
            {error, doing_compaction}
    end;
do_request(delete, [KeyBin, ObjectPool]) ->
    Id = get_object_storage_pid(KeyBin),
    case get_pid_status(Id) of
        idle ->
            ?SERVER_MODULE:delete(get_object_storage_pid(KeyBin), ObjectPool);
        running ->
            {error, doing_compaction}
    end;
do_request(head, [KeyBin]) ->
    ?SERVER_MODULE:head(get_object_storage_pid(KeyBin), KeyBin).


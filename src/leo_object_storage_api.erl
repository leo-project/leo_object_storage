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
-include_lib("eunit/include/eunit.hrl").

-export([start/1,
         put/2, get/1, get/3, delete/2, head/1,
         fetch_by_addr_id/2, fetch_by_key/2,
         store/2,
         stats/0
        ]).

-export([get_object_storage_pid/1]).

-ifdef(TEST).
-export([add_incorrect_data/1]).
-endif.

-define(SERVER_MODULE, 'leo_object_storage_server').

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Create object-storage processes
%%
-spec(start(list()) ->
             ok | {error, any()}).
start([]) ->
    {error, badarg};

start(ObjectStorageInfo) ->
    case start_app() of
        ok ->
            leo_object_storage_sup:start_child(ObjectStorageInfo);
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Insert an object into the object-storage
%% @param Key = {$VNODE_ID, $OBJ_KEY}
%%
-spec(put(tuple(), #object{}) ->
             {ok, integer()} | {error, any()}).
put(Key, Object) ->
    do_request(put, [Key, Object]).


%% @doc Retrieve an object and a metadata from the object-storage
%%
-spec(get(tuple()) ->
             {ok, list()} | not_found | {error, any()}).
get(Key) ->
    get(Key, 0, 0).

-spec(get(tuple(), integer(), integer()) ->
             {ok, #metadata{}, #object{}} | not_found | {error, any()}).
get(Key, StartPos, EndPos) ->
    do_request(get, [Key, StartPos, EndPos]).


%% @doc Remove an object from the object-storage
%%
-spec(delete(tuple(), #object{}) ->
             ok | {error, any()}).
delete(Key, Object) ->
    do_request(delete, [Key, Object]).


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
-spec(fetch_by_key(binary(), function()) ->
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


%% @doc Store metadata and data
%%
-spec(store(#metadata{}, binary()) ->
             ok | {error, any()}).
store(Metadata, Bin) ->
    do_request(store, [Metadata, Bin]).


%% @doc Retrieve the storage stats
%%
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

-ifdef(TEST).
%% @doc Add incorrect datas on debug purpose
%%
-spec(add_incorrect_data(binary()) ->
             ok | {error, any()}).
add_incorrect_data(Bin) ->
    ?SERVER_MODULE:add_incorrect_data(get_object_storage_pid(Bin), Bin).
-endif.


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
            ok = leo_misc:init_env(),
            catch ets:new(?ETS_CONTAINERS_TABLE,
                          [named_table, ordered_set, public, {read_concurrency, true}]),
            catch ets:new(?ETS_INFO_TABLE,
                          [named_table, set, public, {read_concurrency, true}]),
            ok;
        {error, {already_started, Module}} ->
            ok;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "start_app/0"},
                                    {line, ?LINE}, {body, Cause}]),
            {exit, Cause}
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
                      Id = leo_misc:get_value(obj_storage, Value),
                      Id
              end, List);

get_object_storage_pid(List, Arg) ->
    Index = (erlang:crc32(Arg) rem erlang:length(List)) + 1,
    {_, Value} = lists:nth(Index, List),
    Id = leo_misc:get_value(obj_storage, Value),
    Id.


%% @doc Retrieve the status of object of pid
%% @private
-spec(get_status_by_id(pid()) -> storage_status()).
get_status_by_id(Pid) ->
    case leo_misc:get_env(?APP_NAME, {?ENV_COMPACTION_STATUS, Pid}) of
        {ok, Status} ->
            Status;
        _ ->
            ?STATE_ACTIVE
    end.


%% @doc Request an operation
%% @private
-spec(do_request(type_of_method(), list()) ->
             ok | {ok, list()} | {error, any()}).
do_request(get, [Key, StartPos, EndPos]) ->
    KeyBin = term_to_binary(Key),
    ?SERVER_MODULE:get(get_object_storage_pid(KeyBin), KeyBin, StartPos, EndPos);
do_request(store, [Metadata, Bin]) ->
    #metadata{addr_id = AddrId,
              key     = Key} = Metadata,
    Id = get_object_storage_pid(term_to_binary({AddrId, Key})),
    case get_status_by_id(Id) of
        ?STATE_ACTIVE ->
            ?SERVER_MODULE:store(Id, Metadata, Bin);
        ?STATE_COMPACTING ->
            {error, doing_compaction}
    end;
do_request(put, [Key, Object]) ->
    KeyBin = term_to_binary(Key),
    Id = get_object_storage_pid(KeyBin),

    case get_status_by_id(Id) of
        ?STATE_ACTIVE ->
            ?SERVER_MODULE:put(get_object_storage_pid(KeyBin), Object);
        ?STATE_COMPACTING ->
            {error, doing_compaction}
    end;
do_request(delete, [Key, Object]) ->
    KeyBin = term_to_binary(Key),
    Id = get_object_storage_pid(KeyBin),

    case get_status_by_id(Id) of
        ?STATE_ACTIVE ->
            ?SERVER_MODULE:delete(get_object_storage_pid(KeyBin), Object);
        ?STATE_COMPACTING ->
            {error, doing_compaction}
    end;
do_request(head, [Key]) ->
    KeyBin = term_to_binary(Key),
    ?SERVER_MODULE:head(get_object_storage_pid(KeyBin), KeyBin).


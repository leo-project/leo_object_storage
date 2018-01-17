%%======================================================================
%%
%% Leo Object Storage
%%
%% Copyright (c) 2012-2017 Rakuten, Inc.
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
%%
%% @doc The object staorge's API
%% @reference https://github.com/leo-project/leo_object_storage/blob/master/src/leo_object_storage_api.erl
%% @end
%%======================================================================
-module(leo_object_storage_api).

-author('Yosuke Hara').

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start/1, start/2,
         start_with_path/1,
         put/2,
         get/1, get/2, get/3, get/4,
         delete/2,
         head/1, head_with_calc_md5/2, head_with_check_avs/2,
         fetch_by_addr_id/2, fetch_by_addr_id/3,
         fetch_by_key/2, fetch_by_key/3,
         fetch_by_key_in_parallel/3,
         store/2,
         stats/0,
         get_eof_offset/1,
         du_and_compaction_stats/0
        ]).

-export([get_object_storage_pid/1,
         get_object_storage_pid_by_container_id/1
        ]).

-export([compact_data/0, compact_data/1,
         compact_data/2, compact_data/3,
         compact_data_via_console/2,
         compact_state/0,
         diagnose_data/0, diagnose_data/1, diagnose_data/2,
         recover_metadata/0, recover_metadata/1, recover_metadata/2
        ]).

%% to suppress unused function warnings
-export([receiver_init/5,
         fetcher_loop/5
        ]).


-ifdef(TEST).
-export([add_incorrect_data/1, modify_data/3]).
-endif.

-define(SERVER_MODULE, 'leo_object_storage_server').
-define(FETCH_BY_KEY_INTERVAL_TO_COMMUNICATE_WITH_PARENT, 5). %% Communicate with a parent process per 5 messages

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Create object-storage processes
%%
-spec(start(Option) ->
             ok | {error, any()} when Option::[{pos_integer(), string()}]).
start([]) ->
    {error, badarg};
start(Option) ->
    start(Option, undefined).
start(Option, CallbackMod) ->
    case start_app() of
        ok ->
            leo_object_storage_sup:start_child(Option, CallbackMod);
        {error, Cause} ->
            {error, Cause}
    end.


-spec(start_with_path(Path) ->
             ok | {error, any()} when Path::string()).
start_with_path(Path) ->
    case count_avs_containers(Path) of
        {ok, 0} ->
            {error, invalid_path};
        {ok, NumOfContainers} ->
            start([{NumOfContainers, Path}]);
        Other ->
            Other
    end.


%% @doc Retrieve the number of containers
%% @private
count_avs_containers(Path) ->
    Path_1 = filename:join([Path, "object"]),
    case file:list_dir(Path_1) of
        {ok, []} ->
            {ok, 0};
        {ok, RetL} ->
            count_avs_containers_1(RetL, 0);
        Other ->
            Other
    end.

%% @private
count_avs_containers_1([], SoFar) ->
    {ok, SoFar};
count_avs_containers_1([Path|Rest], SoFar) ->
    SoFar_1 =
        case length(string:tokens(Path, "_")) > 1 of
            true ->
                SoFar;
            false ->
                SoFar + 1
        end,
    count_avs_containers_1(Rest, SoFar_1).


%% @doc Insert an object into the object-storage
%% @param Key = {$VNODE_ID, $OBJ_KEY}
%%
-spec(put(AddrIdAndKey, Object) ->
             {ok, integer()} | {error, any()} when AddrIdAndKey::addrid_and_key(),
                                                   Object::#?OBJECT{}).
put(AddrIdAndKey, Object) ->
    do_request(put, [AddrIdAndKey, Object]).


%% @doc Retrieve an object and a metadata from the object-storage
%%
-spec(get(AddrIdAndKey) ->
             {ok, list()} | not_found | {error, any()} when AddrIdAndKey::addrid_and_key()).
get(AddrIdAndKey) ->
    get(AddrIdAndKey, false).

-spec(get(AddrIdAndKey, IsForcedCheck) ->
             {ok, list()} | not_found | {error, any()} when AddrIdAndKey::addrid_and_key(),
                                                            IsForcedCheck::boolean()).
get(AddrIdAndKey, IsForcedCheck) ->
    get(AddrIdAndKey, ?DEF_POS_START, ?DEF_POS_END, IsForcedCheck).

-spec(get(AddrIdAndKey, StartPos, EndPos) ->
             {ok, #?METADATA{}, #?OBJECT{}} |
             not_found |
             {error, any()} when AddrIdAndKey::addrid_and_key(),
                                 StartPos::integer(),
                                 EndPos::integer()).
get(AddrIdAndKey, StartPos, EndPos) ->
    get(AddrIdAndKey, StartPos, EndPos, false).

-spec(get(AddrIdAndKey, StartPos, EndPos, IsForcedCheck) ->
             {ok, #?METADATA{}, #?OBJECT{}} |
             not_found |
             {error, any()} when AddrIdAndKey::addrid_and_key(),
                                 StartPos::integer(),
                                 EndPos::integer(),
                                 IsForcedCheck::boolean()).
get(AddrIdAndKey, StartPos, EndPos, IsForcedCheck) ->
    do_request(get, [AddrIdAndKey, StartPos, EndPos, IsForcedCheck]).


%% @doc Remove an object from the object-storage
%%
-spec(delete(AddrIdAndKey, Object) ->
             ok | {error, any()} when AddrIdAndKey::addrid_and_key(),
                                      Object::#?OBJECT{}).
delete(AddrIdAndKey, Object) ->
    do_request(delete, [AddrIdAndKey, Object]).


%% @doc Retrieve a metadata from the object-storage
%%
-spec(head(AddrIdAndKey) ->
             {ok, binary()} |
             not_found |
             {error, any()} when AddrIdAndKey::addrid_and_key()).
head(AddrIdAndKey) ->
    do_request(head, [AddrIdAndKey]).


%% @doc Retrieve a metada/data from backend_db/object-storage
%%      AND calc MD5 based on the body data
%%
-spec(head_with_calc_md5(AddrIdAndKey, MD5Context) ->
             {ok, metadata, any()} | {error, any()} when AddrIdAndKey::addrid_and_key(),
                                                         MD5Context::any()).
head_with_calc_md5(AddrIdAndKey, MD5Context) ->
    do_request(head_with_calc_md5, [AddrIdAndKey, MD5Context]).

%% @doc Retrieve a metadata from the object-storage
%%      AND check if the corresponding AVS is broken.
%%
-spec(head_with_check_avs(AddrIdAndKey, CheckMethod) ->
             {ok, binary()} |
             not_found |
             {error, any()} when AddrIdAndKey::addrid_and_key(),
                                 CheckMethod::check_header | check_md5).
head_with_check_avs(AddrIdAndKey, check_header) ->
    do_request(head_with_check_avs, [AddrIdAndKey, check_header]);
head_with_check_avs(AddrIdAndKey, check_md5) ->
    do_request(head_with_check_avs, [AddrIdAndKey, check_md5]);
head_with_check_avs(_AddrIdAndKey, _CheckMethod) ->
    {error, invalid_method}.


%% @doc Fetch objects by ring-address-id
%%
-spec(fetch_by_addr_id(AddrId, Fun) ->
             {ok, []} | not_found when AddrId::non_neg_integer(),
                                       Fun::function()|undefined).
fetch_by_addr_id(AddrId, Fun) ->
    fetch_by_addr_id(AddrId, Fun, undefined).

-spec(fetch_by_addr_id(AddrId, Fun, MaxKeys) ->
             {ok, []} | not_found when AddrId::non_neg_integer(),
                                       Fun::function()|undefined,
                                       MaxKeys::non_neg_integer()|undefined).
fetch_by_addr_id(AddrId, Fun, MaxKeys) ->
    case get_object_storage_pid(all) of
        [] ->
            not_found;
        List ->
            List_1 = [?get_obj_storage_read_proc(PidL, AddrId) || {_, PidL} <- List],
            case fetch_by_addr_id_1(List_1, AddrId, Fun, MaxKeys, []) of
                Res ->
                    case MaxKeys of
                        undefined ->
                            {ok, Res};
                        _ ->
                            {ok, lists:sublist(Res, MaxKeys)}
                    end
            end
    end.

fetch_by_addr_id_1([],_,_,_,Acc) ->
    lists:reverse(lists:flatten(Acc));
fetch_by_addr_id_1([H|T], AddrId, Fun, MaxKeys, Acc) ->
    Acc_1 = case ?SERVER_MODULE:fetch(
                    H, {AddrId, <<>>}, Fun, MaxKeys) of
                {ok, Val} ->
                    [Val|Acc];
                _ ->
                    Acc
            end,
    fetch_by_addr_id_1(T, AddrId, Fun, MaxKeys, Acc_1).

%% @doc Fetch objects by key (object-name)
%%
-spec(fetch_by_key(Key, Fun) ->
             {ok, list()} | not_found when Key::binary(),
                                           Fun::function()).
fetch_by_key(Key, Fun) ->
    fetch_by_key(Key, Fun, undefined).

-spec(fetch_by_key(Key, Fun, MaxKeys) ->
             {ok, list()} | not_found when Key::binary(),
                                           Fun::function(),
                                           MaxKeys::non_neg_integer()|undefined).
fetch_by_key(Key, Fun, MaxKeys) ->
    case get_object_storage_pid(all) of
        [] ->
            not_found;
        List ->
            List_1 = [?get_obj_storage_read_proc(PidL, erlang:phash2(Key)) || {_, PidL} <- List],
            case catch lists:foldl(
                         fun(Id, Acc) ->
                                 case catch ?SERVER_MODULE:fetch(Id, {0, Key}, Fun, MaxKeys) of
                                     {ok, Values} ->
                                         [Values|Acc];
                                     not_found ->
                                         Acc;
                                     {_, Cause} when is_tuple(Cause) ->
                                         erlang:error(element(1, Cause));
                                     {_, Cause} ->
                                         erlang:error(Cause)
                                 end
                         end, [], List_1) of
                {'EXIT', Cause} ->
                    Cause_1 = case is_tuple(Cause) of
                                  true ->
                                      element(1, Cause);
                                  false ->
                                      Cause
                              end,
                    {error, Cause_1};
                Ret ->
                    Reply = lists:reverse(lists:flatten(Ret)),
                    case MaxKeys of
                        undefined ->
                            {ok, Reply};
                        _ ->
                            {ok, lists:sublist(Reply, MaxKeys)}
                    end
            end
    end.

%% @doc Fetch objects by key (object-name) in parallel.
%%      The number of spawned processes is same with the number of AVS files.
%%
-spec(fetch_by_key_in_parallel(Key, Fun, MaxKeys) ->
             {ok, list()} | not_found when Key::binary(),
                                           Fun::function(),
                                           MaxKeys::non_neg_integer()|undefined).
fetch_by_key_in_parallel(Key, Fun, MaxKeys) ->
    case get_object_storage_pid(all) of
        [] ->
            not_found;
        List ->
            List_1 = [?get_obj_storage_read_proc(PidL, erlang:phash2(Key)) || {_, PidL} <- List],
            spawn_link(?MODULE, receiver_init, [List_1, Key, Fun, MaxKeys, self()]),
            receive
                {ok, Ret} ->
                    Reply = lists:reverse(lists:flatten(Ret)),
                    case MaxKeys of
                        undefined ->
                            {ok, Reply};
                        _ ->
                            {ok, lists:sublist(Reply, MaxKeys)}
                    end;
                {error, Cause} ->
                    {error, Cause}
            end
    end.

receiver_init(ServerIdList, Key, Fun, MaxKeys, Parent) ->
    process_flag(trap_exit, true),
    Pids = [spawn_link(?MODULE, fetcher_loop, [ServerId, Key, Fun, MaxKeys, self()]) || ServerId <- ServerIdList],
    receiver_loop(Pids, [], [], dict:new(), Parent).

receiver_loop([], Acc, [], _, Parent) ->
    Parent ! {ok, Acc};
receiver_loop([], _Acc, Error, _, Parent) ->
    Parent ! {error, Error};
receiver_loop(Pids, Acc, Error, CounterDict, Parent) ->
    receive
        {counter, Pid, NewCnt} ->
            NewCounterDict = dict:store(Pid, NewCnt, CounterDict),
            Sum = dict:fold(fun(_K,V,ACC) -> V + ACC end, 0, NewCounterDict),
            Pid ! {average, Sum / dict:size(NewCounterDict)},
            receiver_loop(Pids, Acc, Error, NewCounterDict, Parent);
        {ok, Pid, Values} ->
            receiver_loop(lists:delete(Pid, Pids), [Values|Acc], Error, CounterDict, Parent);
        {not_found, Pid} ->
            receiver_loop(lists:delete(Pid, Pids), Acc, Error, CounterDict, Parent);
        {'EXIT', _Pid, normal} ->
            receiver_loop(Pids, Acc, Error, CounterDict, Parent);
        {'EXIT', Pid, Cause} ->
            receiver_loop(lists:delete(Pid, Pids), Acc, [Cause|Error], CounterDict, Parent);
        _Other ->
            receiver_loop(Pids, Acc, Error, CounterDict, Parent)
    end.

fetcher_loop(ServerId, Key, Fun, MaxKeys, ReceiverPid) ->
    WrapFun = fun(K, V, Acc) ->
                      Acc_1 = Fun(K, V, Acc),
                      Counter = case erlang:get(counter) of
                                    undefined ->
                                        0;
                                    C ->
                                        C
                                end,
                      NewCounter = Counter + 1,
                      erlang:put(counter, NewCounter),
                      case NewCounter rem ?FETCH_BY_KEY_INTERVAL_TO_COMMUNICATE_WITH_PARENT of
                          0 ->
                              ReceiverPid ! {counter, self(), NewCounter},
                              receive
                                  {average, Avg} ->
                                      case NewCounter > Avg of
                                          true -> timer:sleep(1);
                                          false -> nop
                                      end
                              end;
                          _ -> nop
                      end,
                      Acc_1
              end,
    case catch ?SERVER_MODULE:fetch(ServerId, {0, Key}, WrapFun, MaxKeys) of
        {ok, Values} ->
            ReceiverPid ! {ok, self(), Values},
            exit(normal);
        not_found ->
            ReceiverPid ! {not_found, self()},
            exit(normal);
        {_, Cause} when is_tuple(Cause) ->
            exit(element(1, Cause));
        {_, Cause} ->
            exit(Cause)
    end.

%% @doc Store metadata and data
%%
-spec(store(Metadata, Bin) ->
             ok | {error, any()} when Metadata::#?METADATA{},
                                      Bin::binary()).
store(Metadata, Bin) ->
    do_request(store, [Metadata, Bin]).


%% @doc Retrieve the storage stats
%%
-spec(stats() ->
             {ok, [#storage_stats{}]} | not_found).
stats() ->
    case get_object_storage_pid(all) of
        [] ->
            not_found;
        List ->
            List_1 = [_Pid || {_Pid,_} <- List],
            {ok, [Stat ||
                     {ok, Stat} <-
                         [?SERVER_MODULE:get_stats(Id) || Id <- List_1]]}
    end.


%% @doc Retrieve the storage and compaction stats
%%
-spec(du_and_compaction_stats() ->
             {ok, [tuple()]} | not_found).
du_and_compaction_stats() ->
    DUState = case stats() of
                  not_found ->
                      [];
                  {ok, RetL} ->
                      RetL
              end,
    {ok, CompactionState} = compact_state(),
    {ok, [{du, DUState},
          {compaction, CompactionState}
         ]}.


%% @doc Retrieve the storage process-id
-spec(get_object_storage_pid(Arg) ->
             [tuple()] when Arg::all | any()).
get_object_storage_pid(Arg) ->
    Ret = ets:tab2list(?ETS_CONTAINERS_TABLE),
    get_object_storage_pid(Ret, Arg).


-spec(get_object_storage_pid(List, Arg) ->
             [tuple()] when List::[{_,_}],
                           Arg::all | any()).
get_object_storage_pid([],_) ->
    [];
get_object_storage_pid(List, all) ->
    lists:map(fun({_, Value}) ->
                      {leo_misc:get_value(obj_storage, Value),
                       leo_misc:get_value(obj_storage_read, Value)}
              end, List);
get_object_storage_pid(List, Arg) ->
    Index = (erlang:crc32(Arg) rem erlang:length(List)) + 1,
    {_, Value} = lists:nth(Index, List),
    [{leo_misc:get_value(obj_storage, Value),
      leo_misc:get_value(obj_storage_read, Value)}].


%% @doc Retrieve object-storage-pid by container-id
-spec(get_object_storage_pid_by_container_id(ContainerId) ->
             Id | not_found when Id::atom(),
                     ContainerId::non_neg_integer()).
get_object_storage_pid_by_container_id(ContainerId) ->
    case ets:lookup(leo_object_storage_containers, ContainerId) of
        [] ->
            not_found;
        [{_, Value}|_] ->
            [{leo_misc:get_value(obj_storage, Value),
              leo_misc:get_value(obj_storage_read, Value)}]
    end.

%% @doc Get the EOF offset
%%      especially useful for debug/test
%%
-spec(get_eof_offset(binary()) ->
             {ok, non_neg_integer()} | {error, any()}).
get_eof_offset(Bin) ->
    [{Pid,_}|_] = get_object_storage_pid(Bin),
    ?SERVER_MODULE:get_eof_offset(Pid).


-ifdef(TEST).
%% @doc Add incorrect datas on debug purpose
%%
-spec(add_incorrect_data(binary()) ->
             ok | {error, any()}).
add_incorrect_data(Bin) ->
    [{Pid,_}|_] = get_object_storage_pid(Bin),
    ?SERVER_MODULE:add_incorrect_data(Pid, Bin).
%% @doc Modify data on debug purpose
%%
-spec(modify_data(addrid_and_key(), binary(), non_neg_integer()) ->
             ok | {error, any()}).
modify_data(AddrIdAndKey, Bin, Offset) ->
    KeyBin = term_to_binary(AddrIdAndKey),
    [{Pid,_}|_] = get_object_storage_pid(KeyBin),
    ?SERVER_MODULE:modify_data(Pid, Bin, Offset).
-endif.


%%--------------------------------------------------------------------
%% Compaction-related Functions
%%--------------------------------------------------------------------
%% @doc Execute data-compaction
-spec(compact_data() ->
             term()).
compact_data() ->
    leo_compact_fsm_controller:run().

-spec(compact_data(NumOfConcurrency) ->
             term() when NumOfConcurrency::integer()).
compact_data(NumOfConcurrency) ->
    NumOfConcurrency_1 =
        ?num_of_compaction_concurrency(NumOfConcurrency),
    leo_compact_fsm_controller:run(NumOfConcurrency_1).

-spec(compact_data(NumOfConcurrency, CallbackFun) ->
             term() when NumOfConcurrency::integer(),
                         CallbackFun::function()|undefined).
compact_data(NumOfConcurrency, CallbackFun) ->
    NumOfConcurrency_1 =
        ?num_of_compaction_concurrency(NumOfConcurrency),
    leo_compact_fsm_controller:run(NumOfConcurrency_1, CallbackFun).

-spec(compact_data(TargetPids, NumOfConcurrency, CallbackFun) ->
             term() when TargetPids::[atom()],
                         NumOfConcurrency::integer(),
                         CallbackFun::function()|undefined).
compact_data(TargetPids, NumOfConcurrency, CallbackFun) ->
    NumOfConcurrency_1 =
        ?num_of_compaction_concurrency(NumOfConcurrency),
    leo_compact_fsm_controller:run(TargetPids, NumOfConcurrency_1, CallbackFun).


%% @doc Execute data-comaction via console
%%
-spec(compact_data_via_console(AVSPath, TargetContainers) ->
             term() when AVSPath::string(),
                         TargetContainers::[non_neg_integer()]).
compact_data_via_console([], TargetContainers) ->
    leo_compact_fsm_controller:recover_metadata(TargetContainers);
compact_data_via_console(AVSPath, TargetContainers) ->
    case start_with_path(AVSPath) of
        ok ->
            TargetPids = ?get_object_storage_id(TargetContainers),
            compact_data(TargetPids, 1, undefined);
        Error ->
            Error
    end.


%% @doc Retrieve current data-compaction status
%%
compact_state() ->
    leo_compact_fsm_controller:state().


%% @doc Diagnode the data
-spec(diagnose_data() ->
             term()).
diagnose_data() ->
    leo_compact_fsm_controller:diagnose().

-spec(diagnose_data(AVSPath) ->
             ok | {error, any()} when AVSPath::string()).
diagnose_data(AVSPath) ->
    case start_with_path(AVSPath) of
        ok ->
            leo_compact_fsm_controller:diagnose();
        Error ->
            Error
    end.

-spec(diagnose_data(AVSPath, TargetContainers) ->
             ok | {error, any()} when AVSPath::string(),
                                      TargetContainers::[non_neg_integer()]).
diagnose_data([], TargetContainers) ->
    leo_compact_fsm_controller:diagnose(TargetContainers);
diagnose_data(AVSPath, TargetContainers) ->
    case start_with_path(AVSPath) of
        ok ->
            leo_compact_fsm_controller:diagnose(TargetContainers);
        Error ->
            Error
    end.


%% @doc Diagnode the data
-spec(recover_metadata() ->
             term()).
recover_metadata() ->
    leo_compact_fsm_controller:recover_metadata().

-spec(recover_metadata(AVSPath) ->
             ok | {error, any()} when AVSPath::string()).
recover_metadata(AVSPath) ->
    case start_with_path(AVSPath) of
        ok ->
            leo_compact_fsm_controller:recover_metadata();
        Error ->
            Error
    end.

-spec(recover_metadata(AVSPath, TargetContainers) ->
             ok | {error, any()} when AVSPath::string(),
                                      TargetContainers::[non_neg_integer()]).
recover_metadata([], TargetContainers) ->
    leo_compact_fsm_controller:recover_metadata(TargetContainers);
recover_metadata(AVSPath, TargetContainers) ->
    case start_with_path(AVSPath) of
        ok ->
            leo_compact_fsm_controller:recover_metadata(TargetContainers);
        Error ->
            Error
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
                                   [{module, ?MODULE_STRING},
                                    {function, "start_app/0"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Request an operation
%% @private
-spec(do_request(type_of_method(), list(_)) ->
             ok |
             {ok, binary()} |
             {ok, #?METADATA{}, #?OBJECT{}} |
             not_found |
             {error, any()}).
do_request(get, [{AddrId, Key}, StartPos, EndPos, IsForcedCheck]) ->
    KeyBin = term_to_binary({AddrId, Key}),
    case get_object_storage_pid(KeyBin) of
        [{_,PidL}|_] ->
            Pid = ?get_obj_storage_read_proc(PidL, AddrId),
            ?SERVER_MODULE:get(Pid, {AddrId, Key}, StartPos, EndPos, IsForcedCheck);
        _ ->
            {error, ?ERROR_PROCESS_NOT_FOUND}
    end;
do_request(store, [Metadata, Bin]) ->
    Metadata_1 = leo_object_storage_transformer:transform_metadata(Metadata),
    #?METADATA{addr_id = AddrId,
               key = Key} = Metadata_1,
    case get_object_storage_pid(term_to_binary({AddrId, Key})) of
        [{Pid,_}|_] ->
            ?SERVER_MODULE:store(Pid, Metadata_1, Bin);
        _ ->
            {error, ?ERROR_PROCESS_NOT_FOUND}
    end;
do_request(put, [Key, Object]) ->
    KeyBin = term_to_binary(Key),
    case get_object_storage_pid(KeyBin) of
        [{Pid,_}|_] ->
            ?SERVER_MODULE:put(Pid, Object);
        _ ->
            {error, ?ERROR_PROCESS_NOT_FOUND}
    end;
do_request(delete, [Key, Object]) ->
    KeyBin = term_to_binary(Key),
    case get_object_storage_pid(KeyBin) of
        [{Pid,_}|_] ->
            ?SERVER_MODULE:delete(Pid, Object);
        _ ->
            {error, ?ERROR_PROCESS_NOT_FOUND}
    end;
do_request(head, [{AddrId, Key}]) ->
    KeyBin = term_to_binary({AddrId, Key}),
    case get_object_storage_pid(KeyBin) of
        [{_,PidL}|_] ->
            Pid = ?get_obj_storage_read_proc(PidL, AddrId),
            ?SERVER_MODULE:head(Pid, {AddrId, Key});
        _ ->
            {error, ?ERROR_PROCESS_NOT_FOUND}
    end;
do_request(head_with_calc_md5, [{AddrId, Key}, MD5Context]) ->
    KeyBin = term_to_binary({AddrId, Key}),
    case get_object_storage_pid(KeyBin) of
        [{_,PidL}|_] ->
            Pid = ?get_obj_storage_read_proc(PidL, AddrId),
            ?SERVER_MODULE:head_with_calc_md5(
               Pid, {AddrId, Key}, MD5Context);
        _ ->
            {error, ?ERROR_PROCESS_NOT_FOUND}
    end;
do_request(head_with_check_avs, [{AddrId, Key}, CheckMethod]) ->
    KeyBin = term_to_binary({AddrId, Key}),
    case get_object_storage_pid(KeyBin) of
        [{_,PidL}|_] ->
            Pid = ?get_obj_storage_read_proc(PidL, AddrId),
            ?SERVER_MODULE:head_with_check_avs(
               Pid, {AddrId, Key}, CheckMethod);
        _ ->
            {error, ?ERROR_PROCESS_NOT_FOUND}
    end.

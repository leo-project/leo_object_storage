%%====================================================================
%%
%% Leo Object Storage
%%
%% Copyright (c) 2012
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
%% -------------------------------------------------------------------
%% Leo Object Storage - EUnit
%% @author yosuke hara
%% @doc
%% @end
%%====================================================================
-module(leo_object_storage_api_tests).
-author('yosuke hara').
-vsn('0.9.1').

-include("leo_object_storage.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

all_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun new_/1,
                           fun operate_/1,
                           fun fetch_by_addr_id_/1,
                           fun fetch_by_key_/1,
                           fun stats_/1,
                           fun compact_/1
                          ]]}.

setup() ->
    Path1 = "./avs1",
    Path2 = "./avs2",
    [Path1, Path2].

teardown([Path1, Path2]) ->
    os:cmd("rm -rf " ++ Path1),
    os:cmd("rm -rf " ++ Path2),
    ok.


new_([Path1, _]) ->
    %% 1-1.
    DivCount0 = 4,
    ok = leo_object_storage_api:start([{DivCount0, Path1}]),

    Ref = whereis(leo_object_storage_sup),
    ?assertEqual(true, is_pid(Ref)),

    [{specs,_},{active,Active0},{supervisors,_},{workers,Workers0}] = supervisor:count_children(Ref),
    ?assertEqual(DivCount0, Active0),
    ?assertEqual(DivCount0, Workers0),

    application:stop(leo_backend_db),
    application:stop(bitcask),
    application:stop(leo_object_storage),


    %% 2. Exception
    Res0 = leo_object_storage_api:start([]),
    ?assertEqual({error, badarg}, Res0),
    ok.

%% Get/Put/Delte
operate_([Path1, Path2]) ->
    ok = leo_object_storage_api:start([{4, Path1},{4, Path2}]),

    %% 1. Put
    AddrId = 0,
    Key = "air/on/g/string",
    Bin = <<"J.S.Bach">>,
    ObjectPool0 = leo_object_storage_pool:new(#object{method   = put,
                                                      addr_id  = AddrId,
                                                      key      = Key,
                                                      data     = Bin,
                                                      dsize    = byte_size(Bin)}),
    Res0 = leo_object_storage_api:put({AddrId, Key}, ObjectPool0),
    ?assertEqual(ok, Res0),

    %% 2. Get
    {ok, Meta1, ObjectPool1} = leo_object_storage_api:get({AddrId, Key}),
    ?assertEqual(AddrId, Meta1#metadata.addr_id),
    ?assertEqual(Key,    Meta1#metadata.key),
    ?assertEqual(0,      Meta1#metadata.del),

    Obj0 = leo_object_storage_pool:get(ObjectPool1),
    ?assertEqual(AddrId,         Obj0#object.addr_id),
    ?assertEqual(Key,            Obj0#object.key),
    ?assertEqual(Bin,            Obj0#object.data),
    ?assertEqual(byte_size(Bin), Obj0#object.dsize),
    ?assertEqual(0,              Obj0#object.del),

    %% 3. Store (for Copy)
    ok = leo_object_storage_api:store(Meta1, Bin),
    {ok, Meta1_1, _} = leo_object_storage_api:get({AddrId, Key}),
    ?assertEqual(AddrId, Meta1_1#metadata.addr_id),
    ?assertEqual(Key,    Meta1_1#metadata.key),
    ?assertEqual(0,      Meta1_1#metadata.del),


    %% 4. Get - Various cases
    %% >> Case of regular.
    {ok, _Meta1_1, ObjectPool1_1} = leo_object_storage_api:get({AddrId, Key}, 4, 8),
    Obj0_1 = leo_object_storage_pool:get(ObjectPool1_1),

    ?assertEqual(4, byte_size(Obj0_1#object.data)),
    ?assertEqual(<<"Bach">>, Obj0_1#object.data),

    %% >> Case of "end-position over data-size".
    {ok, _Meta1_2, ObjectPool1_2} = leo_object_storage_api:get({AddrId, Key}, 5, 9),
    Obj0_2 = leo_object_storage_pool:get(ObjectPool1_2),
    ?assertEqual(<<"ach">>, Obj0_2#object.data),

    %% >> Case of "end-position is zero". It's means "end-position is data-size".
    {ok, _Meta1_3, ObjectPool1_3} = leo_object_storage_api:get({AddrId, Key}, 2, 0),
    Obj0_3 = leo_object_storage_pool:get(ObjectPool1_3),
    ?assertEqual(<<"S.Bach">>, Obj0_3#object.data),

    %% >> Case of "start-position over data-size"
    {ok, _Meta1_4, ObjectPool1_4} = leo_object_storage_api:get({AddrId, Key}, 8, 0),
    Obj0_4 = leo_object_storage_pool:get(ObjectPool1_4),
    ?assertEqual(<<>>, Obj0_4#object.data),


    %% 5. Head
    {ok, Res2} = leo_object_storage_api:head({AddrId, Key}),
    Meta2 = binary_to_term(Res2),
    ?assertEqual(AddrId, Meta2#metadata.addr_id),
    ?assertEqual(Key,    Meta2#metadata.key),
    ?assertEqual(0,      Meta2#metadata.del),

    %% 6. Delete
    ObjectPool2 = leo_object_storage_pool:new(#object{method  = delete,
                                                      key     = Key,
                                                      addr_id = AddrId,
                                                      data    = <<>>,
                                                      del     = 1}),
    Res3 = leo_object_storage_api:delete({AddrId, Key}, ObjectPool2),
    ?assertEqual(ok, Res3),

    %% 7. Get
    Res4 = leo_object_storage_api:get({AddrId, Key}),
    ?assertEqual(not_found, Res4),

    %% 8. Head
    {ok, Res5} = leo_object_storage_api:head({AddrId, Key}),
    Meta5 = binary_to_term(Res5),
    ?assertEqual(AddrId, Meta5#metadata.addr_id),
    ?assertEqual(Key,    Meta5#metadata.key),
    ?assertEqual(1,      Meta5#metadata.del),


    application:stop(leo_backend_db),
    application:stop(bitcask),
    application:stop(leo_object_storage),
    ok.

fetch_by_addr_id_([Path1, Path2]) ->
    ok = leo_object_storage_api:start([{4, Path1},{4, Path2}]),

    ok = put_test_data(0,    "air/on/g/string/0", <<"JSB0">>),
    ok = put_test_data(127,  "air/on/g/string/1", <<"JSB1">>),
    ok = put_test_data(255,  "air/on/g/string/2", <<"JSB2">>),
    ok = put_test_data(511,  "air/on/g/string/3", <<"JSB3">>),
    ok = put_test_data(1023, "air/on/g/string/4", <<"JSB4">>),

    FromAddrId = 0,
    ToAddrId   = 255,

    Fun = fun(K, V, Acc) ->
                  {AddrId,_Key} = binary_to_term(K),
                  Metadata      = binary_to_term(V),

                  case (AddrId >= FromAddrId andalso
                        AddrId =< ToAddrId) of
                      true  ->
                          [Metadata|Acc];
                      false ->
                          Acc
                  end
          end,
    {ok, Res} = leo_object_storage_api:fetch_by_addr_id(0, Fun),
    ?assertEqual(3, length(Res)),

    application:stop(leo_backend_db),
    application:stop(bitcask),
    application:stop(leo_object_storage),
    ok.

fetch_by_key_([Path1, Path2]) ->
    ok = leo_object_storage_api:start([{4, Path1},{4, Path2}]),

    ok = put_test_data(0,    "air/on/g/string/0", <<"JSB0">>),
    ok = put_test_data(127,  "air/on/g/string/1", <<"JSB1">>),
    ok = put_test_data(255,  "air/on/g/string/2", <<"JSB2">>),
    ok = put_test_data(511,  "air/on/g/string/3", <<"JSB3">>),
    ok = put_test_data(1023, "air/on/g/string/4", <<"JSB4">>),

    Fun = fun(K, V, Acc) ->
                  {_AddrId,Key} = binary_to_term(K),
                  Metadata      = binary_to_term(V),

                  case (Key == "air/on/g/string/0" orelse
                        Key == "air/on/g/string/2" orelse
                        Key == "air/on/g/string/4") of
                      true  ->
                          [Metadata|Acc];
                      false ->
                          Acc
                  end
          end,
    {ok, Res} = leo_object_storage_api:fetch_by_key("air/on/g/string", Fun),
    ?assertEqual(3, length(Res)),

    application:stop(leo_backend_db),
    application:stop(bitcask),
    application:stop(leo_object_storage),
    ok.

stats_([Path1, Path2]) ->
    ok = leo_object_storage_api:start([{4, Path1},{4, Path2}]),

    ok = put_test_data(0,    "air/on/g/string/0", <<"JSB0">>),
    ok = put_test_data(127,  "air/on/g/string/1", <<"JSB1">>),
    ok = put_test_data(255,  "air/on/g/string/2", <<"JSB2">>),
    ok = put_test_data(511,  "air/on/g/string/3", <<"JSB3">>),
    ok = put_test_data(767,  "air/on/g/string/4", <<"JSB4">>),
    ok = put_test_data(1023, "air/on/g/string/5", <<"JSB5">>),
    ok = put_test_data(2047, "air/on/g/string/6", <<"JSB6">>),
    ok = put_test_data(4095, "air/on/g/string/7", <<"JSB7">>),

    {ok, Res} = leo_object_storage_api:stats(),
    ?assertEqual(8, length(Res)),

    application:stop(leo_backend_db),
    application:stop(bitcask),
    application:stop(leo_object_storage),
    ok.

compact_([Path1, Path2]) ->
    application:start(sasl),
    application:start(os_mon),

    ok = leo_object_storage_api:start([{4, Path1}, {4, Path2}]),

    ok = put_test_data(0,    "air/on/g/string/0", <<"JSB0">>),
    ok = put_test_data(127,  "air/on/g/string/1", <<"JSB1">>),
    ok = put_test_data(255,  "air/on/g/string/2", <<"JSB2">>),
    ok = put_test_data(511,  "air/on/g/string/3", <<"JSB3">>),
    ok = put_test_data(767,  "air/on/g/string/4", <<"JSB4">>),
    ok = put_test_data(1023, "air/on/g/string/5", <<"JSB5">>),
    ok = put_test_data(2047, "air/on/g/string/6", <<"JSB6">>),

    ok = put_test_data(4095, "air/on/g/string/7", <<"JSB7">>), %% 1st time
    ok = put_test_data(4095, "air/on/g/string/7", <<"JSB7">>), %% 2nd time

    ok = put_test_data(0,    "air/on/g/string/0", <<"JSB0-1">>),
    ok = put_test_data(511,  "air/on/g/string/3", <<"JSB3-1">>),

    AddrId = 4095,
    Key    = "air/on/g/string/7",
    ObjectPool = leo_object_storage_pool:new(#object{method  = delete,
                                                     key     = Key,
                                                     addr_id = AddrId,
                                                     data    = <<>>,
                                                     del     = 1}),
    ok = leo_object_storage_api:delete({AddrId, Key}, ObjectPool),

    %% inspect for compaction
    {ok, Res0} = leo_object_storage_api:stats(),
    Sum0 = lists:foldl(fun({ok, #storage_stats{file_path  = _ObjPath,
                                               total_num  = Total,
                                               active_num = _Active}}, Sum) ->
                               Sum + Total
                       end, 0, Res0),
    ?assertEqual(10, Sum0),
    timer:sleep(250),

    Res1 = leo_object_storage_api:compact(),
    ?assertEqual([ok,ok,ok,ok,ok,ok,ok,ok], Res1),

    timer:sleep(250),

    {ok, Res2} = leo_object_storage_api:stats(),
    Sum1 = lists:foldl(fun({ok, #storage_stats{file_path  = _ObjPath,
                                               total_num  = Total,
                                               active_num = _Active}}, Sum) ->
                               Sum + Total
                       end, 0, Res2),
    ?assertEqual(7, Sum1),

    %% inspect for after compaction
    TestAddrId0 = 0,
    TestKey0    = "air/on/g/string/0",
    TestAddrId1 = 511,
    TestKey1    = "air/on/g/string/3",

    {ok, Meta0, Obj0} = get_test_data(TestAddrId0, TestKey0),
    {ok, Meta1, Obj1} = get_test_data(TestAddrId1, TestKey1),

    ?assertEqual(TestAddrId0,  Meta0#metadata.addr_id),
    ?assertEqual(TestKey0,     Meta0#metadata.key),
    ?assertEqual(6,            Meta0#metadata.dsize),
    ?assertEqual(0,            Meta0#metadata.del),
    ?assertEqual(TestAddrId0,  Obj0#object.addr_id),
    ?assertEqual(TestKey0,     Obj0#object.key),
    ?assertEqual(6,            Obj0#object.dsize),
    ?assertEqual(<<"JSB0-1">>, Obj0#object.data),
    ?assertEqual(0,            Obj0#object.del),

    ?assertEqual(TestAddrId1,  Meta1#metadata.addr_id),
    ?assertEqual(TestKey1,     Meta1#metadata.key),
    ?assertEqual(6,            Meta1#metadata.dsize),
    ?assertEqual(0,            Meta1#metadata.del),
    ?assertEqual(TestAddrId1,  Obj1#object.addr_id),
    ?assertEqual(TestKey1,     Obj1#object.key),
    ?assertEqual(6,            Obj1#object.dsize),
    ?assertEqual(<<"JSB3-1">>, Obj1#object.data),
    ?assertEqual(0,            Obj1#object.del),


    ok = leo_object_storage_sup:stop(),
    application:stop(leo_backend_db),
    application:stop(bitcask),
    application:stop(leo_object_storage),
    application:stop(os_mon),
    application:stop(sasl),
    ok.


proper_test_() ->
    {timeout, 60000, ?_assertEqual([], proper:module(leo_object_storage_api_prop))}.


%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
put_test_data(AddrId, Key, Bin) ->
    ObjectPool = leo_object_storage_pool:new(#object{method   = put,
                                                     addr_id  = AddrId,
                                                     key      = Key,
                                                     data     = Bin,
                                                     dsize    = byte_size(Bin)}),
    ok = leo_object_storage_api:put({AddrId, Key}, ObjectPool),
    ok.

get_test_data(AddrId, Key) ->
    {ok, Meta, ObjectPool} = leo_object_storage_api:get({AddrId, Key}),
    Obj = leo_object_storage_pool:get(ObjectPool),
    {ok, Meta, Obj}.

-endif.

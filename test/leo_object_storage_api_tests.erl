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

-include_lib("eunit/include/eunit.hrl").
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
                           fun stats_/1
                          ]]}.

setup() ->
    application:start(crypto),
    Path1 = "./avs1",
    Path2 = "./avs2",
    io:format(user, "setup~n", []),
    [Path1, Path2].

teardown([Path1, Path2]) ->
    io:format(user, "teardown~n", []),
    os:cmd("rm -rf " ++ Path1),
    os:cmd("rm -rf " ++ Path2),
    application:stop(crypto),
    timer:sleep(200),
    ok.


new_([Path1, _]) ->
    %% 1-1.
    DivCount0 = 4,
    ok = leo_object_storage_api:start([{DivCount0, Path1}]),

    Ref = whereis(leo_object_storage_sup),
    ?assertEqual(true, is_pid(Ref)),

    [{specs,_},{active,Active0},{supervisors,_},{workers,Workers0}] = supervisor:count_children(Ref),
    ?assertEqual(DivCount0 + 2, Active0),  % +2 for compaction manager + backend_db_sup
    ?assertEqual(DivCount0 + 2, Workers0), % +2 for compaction manager + backend_db_sup

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
    Key = <<"air/on/g/string">>,
    Bin = <<"J.S.Bach">>,
    Object = #object{method    = put,
                     addr_id   = AddrId,
                     key       = Key,
                     ksize     = byte_size(Key),
                     data      = Bin,
                     dsize     = byte_size(Bin),
                     checksum  = leo_hex:raw_binary_to_integer(crypto:md5(Bin)),
                     timestamp = leo_date:now(),
                     clock     = leo_date:clock()},
    {ok,_ETag} = leo_object_storage_api:put({AddrId, Key}, Object),

    %% 2. Get
    {ok, Meta1, Obj0} = leo_object_storage_api:get({AddrId, Key}),
    ?assertEqual(AddrId, Meta1#metadata.addr_id),
    ?assertEqual(Key,    Meta1#metadata.key),
    ?assertEqual(0,      Meta1#metadata.del),
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


    %% 4. Get - for range query via HTTP
    %% >> Case of regular.
    {ok, _Meta1_1, Obj0_1} = leo_object_storage_api:get({AddrId, Key}, 4, 7),
    ?assertEqual(4, byte_size(Obj0_1#object.data)),
    ?assertEqual(<<"Bach">>, Obj0_1#object.data),

    %% >> Case of "end-position over data-size".
    {ok, _Meta1_2, Obj0_2} = leo_object_storage_api:get({AddrId, Key}, 5, 9),
    ?assertEqual(<<>>, Obj0_2#object.data),
    ?assertEqual(-2, Obj0_2#object.dsize),

    %% >> Case of "end-position is zero". This means "end-position is data-size".
    {ok, _Meta1_3, Obj0_3} = leo_object_storage_api:get({AddrId, Key}, 2, 0),
    ?assertEqual(<<"S.Bach">>, Obj0_3#object.data),

    %% >> Case of "start-position over data-size"
    {ok, _Meta1_4, Obj0_4} = leo_object_storage_api:get({AddrId, Key}, 8, 0),
    ?assertEqual(<<>>, Obj0_4#object.data),
    ?assertEqual(-2, Obj0_4#object.dsize),

    %% >> Case of "end-position is negative". This means retrieving from end
    {ok, _Meta1_5, Obj0_5} = leo_object_storage_api:get({AddrId, Key}, 0, -2),
    ?assertEqual(<<"ch">>, Obj0_5#object.data),

    %% 5. Head
    {ok, Res2} = leo_object_storage_api:head({AddrId, Key}),
    Meta2 = binary_to_term(Res2),
    ?assertEqual(AddrId, Meta2#metadata.addr_id),
    ?assertEqual(Key,    Meta2#metadata.key),
    ?assertEqual(0,      Meta2#metadata.del),

    %% 6. Delete
    Object2 = #object{method    = delete,
                      key       = Key,
                      ksize     = byte_size(Key),
                      addr_id   = AddrId,
                      data      = <<>>,
                      dsize     = 0,
                      checksum  = leo_hex:raw_binary_to_integer(crypto:md5(<<>>)),
                      timestamp = leo_date:now(),
                      clock     = leo_date:clock(),
                      del       = 1},
    ok = leo_object_storage_api:delete({AddrId, Key}, Object2),

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

    ok = put_test_data(0,    <<"air/on/g/string/0">>, <<"JSB0">>),
    ok = put_test_data(127,  <<"air/on/g/string/1">>, <<"JSB1">>),
    ok = put_test_data(255,  <<"air/on/g/string/2">>, <<"JSB2">>),
    ok = put_test_data(511,  <<"air/on/g/string/3">>, <<"JSB3">>),
    ok = put_test_data(1023, <<"air/on/g/string/4">>, <<"JSB4">>),

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

    ok = put_test_data(0,    <<"air/on/g/string/0">>, <<"JSB0">>),
    ok = put_test_data(127,  <<"air/on/g/string/1">>, <<"JSB1">>),
    ok = put_test_data(255,  <<"air/on/g/string/2">>, <<"JSB2">>),
    ok = put_test_data(511,  <<"air/on/g/string/3">>, <<"JSB3">>),
    ok = put_test_data(1023, <<"air/on/g/string/4">>, <<"JSB4">>),

    Fun = fun(K, V, Acc) ->
                  {_AddrId,Key} = binary_to_term(K),
                  Metadata      = binary_to_term(V),

                  case (Key == <<"air/on/g/string/0">> orelse
                        Key == <<"air/on/g/string/2">> orelse
                        Key == <<"air/on/g/string/4">>) of
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

    ok = put_test_data(0,    <<"air/on/g/string/0">>, <<"JSB0">>),
    ok = put_test_data(127,  <<"air/on/g/string/1">>, <<"JSB1">>),
    ok = put_test_data(255,  <<"air/on/g/string/2">>, <<"JSB2">>),
    ok = put_test_data(511,  <<"air/on/g/string/3">>, <<"JSB3">>),
    ok = put_test_data(767,  <<"air/on/g/string/4">>, <<"JSB4">>),
    ok = put_test_data(1023, <<"air/on/g/string/5">>, <<"JSB5">>),
    ok = put_test_data(2047, <<"air/on/g/string/6">>, <<"JSB6">>),
    ok = put_test_data(4095, <<"air/on/g/string/7">>, <<"JSB7">>),
    ok = put_test_data(4095, <<"air/on/g/string/7">>, <<"JSB8">>),

    {ok, Res} = leo_object_storage_api:stats(),
    ?assertEqual(8, length(Res)),

    catch leo_object_storage_sup:stop(),
    application:stop(leo_backend_db),
    application:stop(bitcask),
    application:stop(leo_object_storage),
    ok.

compact_test_() ->
    {timeout, 15,
     [?_test(
         begin
             Path1 = "./avs1",
             Path2 = "./avs2",
             application:start(crypto),
             application:start(sasl),
             application:start(os_mon),

             ok = leo_object_storage_api:start([{4, Path1}, {4, Path2}]),
             ok = put_test_data(0,    <<"air/on/g/string/0">>, <<"JSB0">>),
             ok = put_test_data(127,  <<"air/on/g/string/1">>, <<"JSB1">>),
             ok = put_test_data(255,  <<"air/on/g/string/2">>, <<"JSB2">>),
             ok = put_test_data(511,  <<"air/on/g/string/3">>, <<"JSB3">>),
             ok = put_test_data(767,  <<"air/on/g/string/4">>, <<"JSB4">>),
             ok = put_test_data(1023, <<"air/on/g/string/5">>, <<"JSB5">>),
             ok = put_test_data(2047, <<"air/on/g/string/6">>, <<"JSB6">>),
             ok = put_test_data(4095, <<"air/on/g/string/7">>, <<"JSB7">>), %% 1st time
             ok = put_test_data(4095, <<"air/on/g/string/7">>, <<"JSB7">>), %% 2nd time
             {ok, Res0} = leo_object_storage_api:stats(),
             {SumTotal0, SumActive0} =
                 lists:foldl(fun({ok, #storage_stats{file_path  = _ObjPath,
                                                     total_num  = Total,
                                                     active_num = Active}}, {SumTotal, SumActive}) ->
                                     {SumTotal + Total, SumActive + Active}
                             end, {0, 0}, Res0),
             ?assertEqual(9, SumTotal0),
             ?assertEqual(8, SumActive0),

             ok = put_test_data(0,    <<"air/on/g/string/0">>, <<"JSB0-1">>),
             ok = put_test_data(511,  <<"air/on/g/string/3">>, <<"JSB3-1">>),

             ?assertEqual({error,badstate}, leo_compaction_manager_fsm:suspend()),
             ?assertEqual({error,badstate}, leo_compaction_manager_fsm:resume()),

             %% append incorrect data
             _ = leo_object_storage_api:add_incorrect_data(crypto:rand_bytes(256)),

             AllTargets = leo_object_storage_api:get_object_storage_pid('all'),
             ?assertEqual({ok, #compaction_stats{status = 'idle',
                                                 total_num_of_targets    = 8,
                                                 num_of_reserved_targets = 0,
                                                 num_of_pending_targets  = 8,
                                                 num_of_ongoing_targets  = 0,
                                                 reserved_targets = [],
                                                 pending_targets  = AllTargets,
                                                 ongoing_targets  = [],
                                                 latest_exec_datetime = 0
                                                }}, leo_compaction_manager_fsm:status()),
             AddrId = 4095,
             Key    = <<"air/on/g/string/7">>,
             Object = #object{method    = delete,
                              key       = Key,
                              ksize     = byte_size(Key),
                              addr_id   = AddrId,
                              data      = <<>>,
                              dsize     = 0,
                              checksum  = leo_hex:raw_binary_to_integer(crypto:md5(<<>>)),
                              timestamp = leo_date:now(),
                              clock     = leo_date:clock(),
                              del       = 1},
             ok = leo_object_storage_api:delete({AddrId, Key}, Object),

             %% inspect for compaction
             {ok, Res1} = leo_object_storage_api:stats(),
             {SumTotal1, SumActive1, SumTotalSize1, SumActiveSize1}
                 = lists:foldl(
                     fun({ok, #storage_stats{file_path  = _ObjPath,
                                             total_sizes = TotalSize,
                                             active_sizes = ActiveSize,
                                             total_num  = Total,
                                             active_num = Active}},
                         {SumTotal, SumActive, SumTotalSize, SumActiveSize}) ->
                             {SumTotal + Total,
                              SumActive + Active,
                              SumTotalSize + TotalSize,
                              SumActiveSize + ActiveSize}
                     end, {0, 0, 0, 0}, Res1),
             ?assertEqual(12, SumTotal1),
             ?assertEqual(7, SumActive1),
             ?assertEqual(true, SumTotalSize1 > SumActiveSize1),
             timer:sleep(250),

             FunHasChargeOfNode = fun(_Key_) ->
                                          true
                                  end,
             TargetPids = leo_object_storage_api:get_object_storage_pid(all),
             io:format(user, "*** target-pids:~p~n", [TargetPids]),

             ok = leo_compaction_manager_fsm:start(TargetPids, 2, FunHasChargeOfNode),
             timer:sleep(100),

             {ok, CopactionStats} = leo_compaction_manager_fsm:status(),
             ?assertEqual('running', CopactionStats#compaction_stats.status),
             ?assertEqual(8, CopactionStats#compaction_stats.total_num_of_targets),
             ?assertEqual(true, 0 < CopactionStats#compaction_stats.num_of_pending_targets),
             ?assertEqual(true, 0 < CopactionStats#compaction_stats.num_of_ongoing_targets),

             ?assertEqual(ok, leo_compaction_manager_fsm:suspend()),
             {ok, CopactionStats2} = leo_compaction_manager_fsm:status(),
             ?assertEqual('suspend', CopactionStats2#compaction_stats.status),
             %% keep # of ongoing/pending fixed during suspend
             Pending = CopactionStats2#compaction_stats.num_of_pending_targets,
             Ongoing = CopactionStats2#compaction_stats.num_of_ongoing_targets,
             timer:sleep(1000),
             ?assertEqual(Pending, CopactionStats2#compaction_stats.num_of_pending_targets),
             ?assertEqual(Ongoing, CopactionStats2#compaction_stats.num_of_ongoing_targets),
             %% operation during suspend
             TestAddrId0 = 0,
             TestKey0    = <<"air/on/g/string/0">>,
             TestAddrId1 = 511,
             TestKey1    = <<"air/on/g/string/3">>,
             {ok, _, _} = get_test_data(TestAddrId0, TestKey0),
             {ok, _, _} = get_test_data(TestAddrId1, TestKey1),

             ?assertEqual(ok, leo_compaction_manager_fsm:resume()),

             timer:sleep(2000),
             {ok, Res2} = leo_object_storage_api:stats(),
             {SumTotal2, SumActive2, SumTotalSize2, SumActiveSize2}
                 = lists:foldl(
                     fun({ok, #storage_stats{file_path  = _ObjPath,
                                             compaction_histories = [{Start, End}|_Rest],
                                             total_sizes = TotalSize,
                                             active_sizes = ActiveSize,
                                             total_num  = Total,
                                             active_num = Active}},
                         {SumTotal, SumActive, SumTotalSize, SumActiveSize}) ->
                             ?assertEqual(true, Start =< End),
                             {SumTotal + Total,
                              SumActive + Active,
                              SumTotalSize + TotalSize,
                              SumActiveSize + ActiveSize}
                     end, {0, 0, 0, 0}, Res2),
             ?assertEqual(7, SumTotal2),
             ?assertEqual(7, SumActive2),
             ?assertEqual(true, SumTotalSize2 =:= SumActiveSize2),

             %% inspect for after compaction
             TestAddrId0 = 0,
             TestKey0    = <<"air/on/g/string/0">>,
             TestAddrId1 = 511,
             TestKey1    = <<"air/on/g/string/3">>,

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
             application:stop(crypto),
             os:cmd("rm -rf " ++ Path1),
             os:cmd("rm -rf " ++ Path2),
             true end)]}.


%% proper_test_() ->
%%     {timeout, 60000, ?_assertEqual([], proper:module(leo_object_storage_api_prop))}.


%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
put_test_data(AddrId, Key, Bin) ->
    Object = #object{method    = put,
                     addr_id   = AddrId,
                     key       = Key,
                     ksize     = byte_size(Key),
                     data      = Bin,
                     dsize     = byte_size(Bin),
                     checksum  = leo_hex:raw_binary_to_integer(crypto:md5(Bin)),
                     timestamp = leo_date:now(),
                     clock     = leo_date:clock()
                    },
    {ok, _Checksum} = leo_object_storage_api:put({AddrId, Key}, Object),
    ok.

get_test_data(AddrId, Key) ->
    leo_object_storage_api:get({AddrId, Key}).

-endif.

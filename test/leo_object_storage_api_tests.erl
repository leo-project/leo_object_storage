%%====================================================================
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
%%====================================================================
-module(leo_object_storage_api_tests).
-author('yosuke hara').

-include_lib("eunit/include/eunit.hrl").
-include("leo_object_storage.hrl").

-export([notify/4]).

notify(slow_operation, Method, Key, ProcessingTime) ->
    ?debugVal({slow_operation, Method, Key, ProcessingTime});
notify(_Info,_Method,_Key,_ProcessingTime) ->
    ok.


-ifdef(EUNIT).

%%======================================================================
%% Compaction TEST
%%======================================================================
-define(AVS_DIR_FOR_COMPACTION, "comaction_test/").

diagnosis_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### DIAGNOSIS.START ###"),
             os:cmd("rm -rf " ++ ?AVS_DIR_FOR_COMPACTION),
             application:start(sasl),
             application:start(os_mon),
             application:start(crypto),
             application:start(leo_object_storage),
             ok
     end,
     fun (_) ->
             application:stop(crypto),
             application:stop(os_mon),
             application:stop(sasl),
             timer:sleep(1000),
             application:stop(leo_object_storage),
             timer:sleep(1000),
             ?debugVal("### DIAGNOSIS.END ###"),
             ok
     end,
     [
      {"test dianosis",
       {timeout, 1000, fun diagnose/0}}
     ]}.

recovery_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### RECOVERY.START ###"),
             os:cmd("rm -rf " ++ ?AVS_DIR_FOR_COMPACTION),
             application:start(sasl),
             application:start(os_mon),
             application:start(crypto),
             application:start(leo_object_storage),
             ok
     end,
     fun (_) ->
             application:stop(crypto),
             application:stop(os_mon),
             application:stop(sasl),
             timer:sleep(1000),
             application:stop(leo_object_storage),
             timer:sleep(1000),
             ?debugVal("### RECOVERY.END ###"),
             ok
     end,
     [
      {"test recovery",
       {timeout, 1000, fun recover/0}}
     ]}.

compaction_0_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### COMPACTION.START #1 ###"),
             os:cmd("rm -rf " ++ ?AVS_DIR_FOR_COMPACTION),
             application:start(sasl),
             application:start(os_mon),
             application:start(crypto),
             application:start(leo_object_storage),
             ok
     end,
     fun (_) ->
             application:stop(crypto),
             application:stop(os_mon),
             application:stop(sasl),
             timer:sleep(1000),
             application:stop(leo_object_storage),
             timer:sleep(1000),
             ?debugVal("### COMPACTION.END #1 ###"),
             ok
     end,
     [
      {"test compaction",
       {timeout, 1000, fun compact/0}}
     ]}.

compaction_1_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### COMPACTION.START #2 ###"),
             os:cmd("rm -rf " ++ ?AVS_DIR_FOR_COMPACTION),
             application:start(sasl),
             application:start(os_mon),
             application:start(crypto),
             application:start(leo_object_storage),
             ok
     end,
     fun (_) ->
             application:stop(crypto),
             application:stop(os_mon),
             application:stop(sasl),
             timer:sleep(1000),
             application:stop(leo_object_storage),
             timer:sleep(1000),
             ?debugVal("### COMPACTION.END #2 ###"),
             ok
     end,
     [
      {"test compaction",
       {timeout, 1000, fun compact_1/0}}
     ]}.

compaction_3_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("### COMPACTION.START #3 ###"),
             os:cmd("rm -rf " ++ ?AVS_DIR_FOR_COMPACTION),
             application:start(sasl),
             application:start(os_mon),
             application:start(crypto),
             application:start(leo_object_storage),
             ok
     end,
     fun (_) ->
             application:stop(crypto),
             application:stop(os_mon),
             application:stop(sasl),
             timer:sleep(1000),
             application:stop(leo_object_storage),
             timer:sleep(1000),
             ?debugVal("### COMPACTION.END #3 ###"),
             ok
     end,
     [
      {"test compaction",
       {timeout, 1000, fun compact_3/0}}
     ]}.

diagnose() ->
    %% Launch object-storage
    leo_object_storage_api:start([{1, ?AVS_DIR_FOR_COMPACTION}]),
    ?debugVal(leo_compact_fsm_controller:state()),

    ok = put_regular_bin(1, 50),
    ok = put_irregular_bin(),
    ok = put_regular_bin(36, 25),
    ok = put_irregular_bin(),
    ok = put_regular_bin_with_cmeta(51, 50),
    ok = put_irregular_bin(),
    ok = put_large_bin(101),
    ok = leo_object_storage_api:delete({1, <<"TEST_10">>},
                                       #?OBJECT{method    = delete,
                                                addr_id   = 1,
                                                key       = <<"TEST_10">>,
                                                ksize     = 7,
                                                data      = <<>>,
                                                dsize     = 0,
                                                checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, <<>>)),
                                                timestamp = leo_date:now(),
                                                clock     = leo_date:clock(),
                                                del = 1
                                               }),
    ok = leo_object_storage_api:delete({1, <<"TEST_50">>},
                                       #?OBJECT{method    = delete,
                                                addr_id   = 1,
                                                key       = <<"TEST_50">>,
                                                ksize     = 7,
                                                data      = <<>>,
                                                dsize     = 0,
                                                checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, <<>>)),
                                                timestamp = leo_date:now(),
                                                clock     = leo_date:clock(),
                                                del = 1
                                               }),

    %% Execute to diagnose data
    timer:sleep(1000),
    ok = leo_compact_fsm_controller:diagnose(),
    ok = check_status(),

    %% Check # of active objects and total of objects
    {ok, [#storage_stats{total_num  = TotalNum,
                         active_num = ActiveNum
                        }|_]} = leo_object_storage_api:stats(),
    ?debugVal({TotalNum, ActiveNum}),
    ?assertEqual(128, TotalNum),
    ?assertEqual(99,  ActiveNum),

    {ok, State} = leo_compact_fsm_controller:state(),
    {ok, DU_Copmaction_Stats} = leo_object_storage_api:du_and_compaction_stats(),
    ?debugVal(State#compaction_stats.acc_reports),
    ?debugVal(DU_Copmaction_Stats),
    ok.

recover() ->
    %% Launch object-storage
    leo_object_storage_api:start([{1, ?AVS_DIR_FOR_COMPACTION}]),
    ?debugVal(leo_compact_fsm_controller:state()),

    ok = put_regular_bin(1, 50),
    ok = put_irregular_bin(),
    ok = put_regular_bin(36, 25),
    ok = put_irregular_bin(),
    ok = put_regular_bin_with_cmeta(51, 50),
    ok = put_irregular_bin(),
    ok = put_large_bin(101),
    ok = leo_object_storage_api:delete({1, <<"TEST_10">>},
                                       #?OBJECT{method    = delete,
                                                addr_id   = 1,
                                                key       = <<"TEST_10">>,
                                                ksize     = 7,
                                                data      = <<>>,
                                                dsize     = 0,
                                                checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, <<>>)),
                                                timestamp = leo_date:now(),
                                                clock     = leo_date:clock(),
                                                del = 1
                                               }),
    ok = leo_object_storage_api:delete({1, <<"TEST_50">>},
                                       #?OBJECT{method    = delete,
                                                addr_id   = 1,
                                                key       = <<"TEST_50">>,
                                                ksize     = 7,
                                                data      = <<>>,
                                                dsize     = 0,
                                                checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, <<>>)),
                                                timestamp = leo_date:now(),
                                                clock     = leo_date:clock(),
                                                del = 1
                                               }),

    %% Execute to diagnose data
    timer:sleep(1000),
    ok = delete_metadata(101),
    ok = leo_compact_fsm_controller:recover_metadata(),
    ok = check_status(),

    %% Check # of active objects and total of objects
    timer:sleep(1000),
    {ok, [#storage_stats{total_num  = TotalNum,
                         active_num = ActiveNum
                        }|_]} = leo_object_storage_api:stats(),
    ?debugVal({TotalNum, ActiveNum}),

    {ok, State} = leo_compact_fsm_controller:state(),
    ?debugVal(State#compaction_stats.acc_reports),

    ok = check_metadata(101),
    ok.


compact() ->
    %% Launch object-storage
    leo_object_storage_api:start([{1, ?AVS_DIR_FOR_COMPACTION}], ?MODULE),
    ok = put_regular_bin(1, 50),
    ok = put_irregular_bin(),
    ok = put_regular_bin(36, 25),
    ok = put_irregular_bin(),
    ok = put_regular_bin_with_cmeta(51, 50),
    ok = put_irregular_bin(),
    ok = put_large_bin(101),

    %% Execute compaction
    timer:sleep(1000),
    FunHasChargeOfNode = fun(_Key_,_NumOfReplicas_) ->
                                 true
                         end,
    TargetPids = [_Pid || {_Pid,_}
                              <-leo_object_storage_api:get_object_storage_pid(all)],
    ok = leo_compact_fsm_controller:run(TargetPids, 1, FunHasChargeOfNode),

    %% Insert objects during the compaction
    ok = put_regular_bin(200, 25),
    ok = put_irregular_bin(),
    ok = put_regular_bin_with_cmeta(225, 15),
    ok = put_regular_bin(240, 10),

    %% Change waiting-time of the procs
    [leo_compact_fsm_controller:decrease() || _Num <- lists:seq(1, 30)],
    timer:sleep(1000),

    ok = leo_compact_fsm_controller:increase(),
    timer:sleep(10),
    ok = leo_compact_fsm_controller:increase(),

    %% Check comaction status
    ok = check_status(),

    %% Check # of active objects and total of objects
    timer:sleep(1000),
    Stats = leo_object_storage_api:stats(),
    {ok, [#storage_stats{total_num  = TotalNum,
                         active_num = ActiveNum
                        }|_]} = Stats,
    ?assertEqual(151, TotalNum),
    ?assertEqual(TotalNum, ActiveNum),
    ok.

compact_1() ->
    %% Launch object-storage
    leo_object_storage_api:start([{1, ?AVS_DIR_FOR_COMPACTION}]),
    ok = put_regular_bin(1, 50),
    ok = put_irregular_bin(),
    ok = put_regular_bin(36, 25),
    ok = put_irregular_bin(),
    ok = put_regular_bin_with_cmeta(51, 50),
    ok = put_irregular_bin(),
    ok = put_irregular_bin(),
    ok = put_irregular_bin(),

    %% Execute compaction
    timer:sleep(1000),
    FunHasChargeOfNode = fun(_Key_,_NumOfReplicas_) ->
                                 true
                         end,
    TargetPids = [_Pid || {_Pid,_}
                              <- leo_object_storage_api:get_object_storage_pid(all)],
    ok = leo_compact_fsm_controller:run(TargetPids, 1, FunHasChargeOfNode),

    %% Check comaction status
    ok = check_status(),

    %% Check # of active objects and total of objects
    timer:sleep(1000),
    Stats = leo_object_storage_api:stats(),
    {ok, [#storage_stats{total_num  = TotalNum,
                         active_num = ActiveNum
                        }|_]} = Stats,
    ?assertEqual(100, TotalNum),
    ?assertEqual(TotalNum, ActiveNum),
    ok.

compact_3() ->
    %% Launch object-storage
    leo_object_storage_api:start([{4, ?AVS_DIR_FOR_COMPACTION}]),

    Key = list_to_binary(lists:duplicate(4100, $X)),
    CMeta = [{?PROP_CMETA_CLUSTER_ID, 'leofs_1'},
             {?PROP_CMETA_NUM_OF_REPLICAS, 3},
             {?PROP_CMETA_VER, leo_date:clock()},
             {?PROP_CMETA_UDM, [{<<"name">>, <<"LeoFS">>},
                                {<<"category">>, <<"distributed storage">>},
                                {<<"url">>, <<"leo-project.net/leofs">>}
                               ]
             }],
    %% inserted but will be compacted due to the key size limit
    put_irregular_bin(Key, CMeta, 0, leo_date:clock()),
    %% inserted but will be compacted due to the csize limit
    Key2 = list_to_binary("invalid_csize"),
    put_irregular_bin(Key2, CMeta, 1024 * 1024 * 11, leo_date:clock()),
    %% inserted but will be compacte due to the msize limit
    Key3 = list_to_binary("invalid_msize"),
    UDM = crypto:strong_rand_bytes(4096),
    CMeta2 = [{?PROP_CMETA_CLUSTER_ID, 'leofs_1'},
             {?PROP_CMETA_NUM_OF_REPLICAS, 3},
             {?PROP_CMETA_VER, leo_date:clock()},
             {?PROP_CMETA_UDM, UDM
             }],
    put_irregular_bin(Key3, CMeta2, 0, leo_date:clock()),
    %% inserted but will be compacted due to the clock limit
    Key4 = list_to_binary("invalid_clock"),
    put_irregular_bin(Key4, CMeta, 0, 4633552912011364),

    Expected = lists:duplicate(10, ok),
    Actual = [ put_regular_bin(1, 50) || _ <- lists:seq(1, 10) ],
    ?assertEqual(Expected, Actual),
    Min = 1024 * 1024,
    Range = Min * 2,
    ok = put_irregular_bin(Min, Range),

    %% Execute compaction
    timer:sleep(1000),
    {ok, Stats} = leo_object_storage_api:stats(),
    {SumTotal0, SumActive0} =
                 lists:foldl(
                   fun(#storage_stats{total_num  = Total,
                                      active_num = Active},
                       {SumTotal, SumActive}) ->
                           {SumTotal + Total, SumActive + Active}
                   end, {0, 0}, Stats),
    ?assertEqual(504, SumTotal0), %% +4 caused by inserted but invalid data
    ?assertEqual(54, SumActive0), %% +4 caused by inserted but invalid data
    FunHasChargeOfNode = fun(_Key_,_NumOfReplicas_) ->
                                 true
                         end,
    TargetPids = [_Pid || {_Pid,_}
                              <- leo_object_storage_api:get_object_storage_pid(all)],
    ok = leo_compact_fsm_controller:run(TargetPids, 2, FunHasChargeOfNode),

    %% Check comaction status
    ok = check_status(),

    %% Check # of active objects and total of objects
    timer:sleep(1000),
    {ok, Stats1} = leo_object_storage_api:stats(),
    {SumTotal1, SumActive1} =
                 lists:foldl(
                   fun(#storage_stats{total_num  = Total,
                                      active_num = Active},
                       {SumTotal, SumActive}) ->
                           {SumTotal + Total, SumActive + Active}
                   end, {0, 0}, Stats1),
    ?assertEqual(50, SumTotal1),
    ?assertEqual(50, SumActive1),
    ok.


check_status() ->
    timer:sleep(1000),
    ID = 'leo_compact_worker_0',
    %% shouldn't be blocked
    {_, _} = sys:get_state(ID),
    case leo_compact_fsm_controller:state() of
        {ok, #compaction_stats{status = ?ST_IDLING}} ->
            %% check pid_pairs is empty
            {_, State} = sys:get_state(leo_compact_fsm_controller),
            ?debugVal(State),
            %% 1:record name, 2: id, 3: server_pairs, 4: pid_pairs
            ?assertEqual([], erlang:element(4, State)),
            ok;
        {ok, #compaction_stats{locked_targets = []}} ->
            check_status();
        {ok, #compaction_stats{locked_targets = LockedTargets}} ->
            [ID|_] = LockedTargets,
            ok = leo_compact_fsm_worker:state(ID, self()),
            receive
                idling ->
                    void;
                _Status ->
                    Ret = put_regular_bin_1(300),
                    ?debugVal(Ret)
            end,
            check_status();
        {ok, _Other} ->
            check_status();
        Error ->
            ?debugVal(Error),
            Error
    end.

%% @doc Put data
%% @private
put_regular_bin(_, 0) ->
    ok;
put_regular_bin(Index, Counter) ->
    {ok,_} = put_regular_bin_1(Index),
    put_regular_bin(Index + 1, Counter -1).

put_regular_bin_1(Index) ->
    AddrId = 1,
    Key = list_to_binary(lists:append(["TEST_", integer_to_list(Index)])),
    Bin = crypto:strong_rand_bytes(erlang:phash2(leo_date:clock(), (1024 * 1024))),

    Object = #?OBJECT{method    = put,
                      addr_id   = AddrId,
                      key       = Key,
                      ksize     = byte_size(Key),
                      data      = Bin,
                      dsize     = byte_size(Bin),
                      meta      = <<>>,
                      msize     = 0,
                      checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin)),
                      timestamp = leo_date:now(),
                      clock     = leo_date:clock()
                     },
    leo_object_storage_api:put({AddrId, Key}, Object).


put_large_bin(Index) ->
    AddrId = 1,
    Key = list_to_binary(lists:append(["TEST_", integer_to_list(Index)])),
    Len = 1024 * 1024,
    Object = #?OBJECT{method    = put,
                      addr_id   = AddrId,
                      key       = Key,
                      ksize     = byte_size(Key),
                      data      = <<>>,
                      dsize     = Len,
                      checksum  = ?MD5_EMPTY_BIN,
                      timestamp = leo_date:now(),
                      clock     = leo_date:clock(),
                      csize     = Len,
                      cnumber   = 100,
                      cindex    = 0
                     },
    {ok, _} = leo_object_storage_api:put({AddrId, Key}, Object),
    ok.


%% @doc Put data with custom-metadata
%% @private
put_regular_bin_with_cmeta(_, 0) ->
    ok;
put_regular_bin_with_cmeta(Index, Counter) ->
    AddrId = 1,
    Key = list_to_binary(lists:append(["TEST_", integer_to_list(Index)])),
    Bin = crypto:strong_rand_bytes(erlang:phash2(leo_date:clock(), (1024 * 1024))),

    CMeta = [{?PROP_CMETA_CLUSTER_ID, 'leofs_1'},
             {?PROP_CMETA_NUM_OF_REPLICAS, 3},
             {?PROP_CMETA_PREFERRED_R, 1},
             {?PROP_CMETA_PREFERRED_W, 2},
             {?PROP_CMETA_PREFERRED_D, 2},
             {?PROP_CMETA_VER, leo_date:clock()},
             {?PROP_CMETA_UDM, [{<<"name">>, <<"LeoFS">>},
                                {<<"category">>, <<"distributed storage">>},
                                {<<"url">>, <<"leo-project.net/leofs">>},
                                {<<"index">>, list_to_binary(integer_to_list(Index))}
                               ]
             }],
    CMetaBin = leo_object_storage_transformer:list_to_cmeta_bin(CMeta),
    %% CMetaBin = leo_object_storage_transformer:list_to_cmeta_bin(
    %%              [{?PROP_CMETA_CLUSTER_ID, 'remote_cluster'},
    %%               {?PROP_CMETA_NUM_OF_REPLICAS, 3}]),

    Object = #?OBJECT{method    = put,
                      addr_id   = AddrId,
                      key       = Key,
                      ksize     = byte_size(Key),
                      data      = Bin,
                      dsize     = byte_size(Bin),
                      meta      = CMetaBin,
                      msize     = byte_size(CMetaBin),
                      checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin)),
                      timestamp = leo_date:now(),
                      clock     = leo_date:clock()
                     },
    {ok, _} = leo_object_storage_api:put({AddrId, Key}, Object),
    put_regular_bin_with_cmeta(Index + 1, Counter -1).


%% @doc Put irregular data
%% @private
put_irregular_bin() ->
    Min = 1024 * 16,
    Range = 1024 * 512,
    put_irregular_bin(Min, Range).

put_irregular_bin(Min, Range) ->
    Len = case erlang:phash2(leo_date:clock(), Range) of
              Val when Val < Min ->
                  Min;
              Val ->
                  Val
          end,
    ?debugVal(Len),
    Bin = crypto:strong_rand_bytes(Len),
    _ = leo_object_storage_api:add_incorrect_data(Bin),
    {ok, Offset} = leo_object_storage_api:get_eof_offset(Bin),
    io:format(user, "[garbage] *** start:~p, end:~p~n", [Offset - byte_size(Bin), Offset]),
    ok.

put_irregular_bin(Key, CMeta, CSize, Clock) ->
    AddrId = 1,
    Bin = crypto:strong_rand_bytes(erlang:phash2(leo_date:clock(), (1024 * 1024))),
    CMetaBin = leo_object_storage_transformer:list_to_cmeta_bin(CMeta),

    Object = #?OBJECT{method    = put,
                      addr_id   = AddrId,
                      key       = Key,
                      ksize     = byte_size(Key),
                      data      = Bin,
                      dsize     = byte_size(Bin),
                      csize     = CSize,
                      meta      = CMetaBin,
                      msize     = byte_size(CMetaBin),
                      checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin)),
                      timestamp = leo_date:now(),
                      clock     = Clock
                     },
    leo_object_storage_api:put({AddrId, Key}, Object).

%% @private
delete_metadata(0) ->
    ok;
delete_metadata(Index) ->
    KeyBin = list_to_binary(lists:append(["TEST_", integer_to_list(Index)])),
    case leo_backend_db_api:delete('leo_metadata_0', KeyBin) of
        ok ->
            delete_metadata(Index - 1);
        _ ->
            {error, invalid_key}
    end.

%% @private
check_metadata(0) ->
    ok;
check_metadata(Index) ->
    KeyBin = list_to_binary(lists:append(["TEST_", integer_to_list(Index)])),
    case leo_backend_db_api:get('leo_metadata_0', KeyBin) of
        {ok, _Bin} ->
            check_metadata(Index - 1);
        Error ->
            ?debugVal(Error),
            {error, invalid_key}
    end.

%%======================================================================
%% Suite TEST
%%======================================================================
suite_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun new_/1,
                           fun operate_/1,
                           fun fetch_by_addr_id_/1,
                           fun fetch_by_key_/1,
                           fun msg_collector_/1
                          ]]}.

setup() ->
    application:start(sasl),
    application:start(os_mon),
    application:start(crypto),
    Path1 = "./avs1",
    Path2 = "./avs2",
    os:cmd("rm -rf " ++ Path1),
    os:cmd("rm -rf " ++ Path2),
    io:format(user, "setup~n", []),
    [Path1, Path2].

teardown([Path1, Path2]) ->
    io:format(user, "teardown~n", []),
    os:cmd("rm -rf " ++ Path1),
    os:cmd("rm -rf " ++ Path2),
    application:stop(crypto),
    application:stop(os_mon),
    application:stop(sasl),
    timer:sleep(1000),
    application:stop(leo_object_storage),
    timer:sleep(1000),
    ok.

msg_collector_(_) ->
    leo_object_storage_msg_collector:init(false),
    {ok, Msgs} = leo_object_storage_msg_collector:get(),
    ?assertEqual(0, erlang:length(leo_misc:get_value(?MSG_ITEM_TIMEOUT, Msgs, []))),
    ?assertEqual(0, erlang:length(leo_misc:get_value(?MSG_ITEM_SLOW_OP, Msgs, []))),
    ok = leo_object_storage_msg_collector:clear(),
    ok = leo_object_storage_msg_collector:notify(?MSG_ITEM_TIMEOUT, put, key),
    ok = leo_object_storage_msg_collector:notify(?MSG_ITEM_SLOW_OP, put, key, 1000),
    {ok, Msgs2} = leo_object_storage_msg_collector:get(),
    ?assertEqual(0, erlang:length(leo_misc:get_value(?MSG_ITEM_TIMEOUT, Msgs2, []))),
    ?assertEqual(0, erlang:length(leo_misc:get_value(?MSG_ITEM_SLOW_OP, Msgs2, []))),

    leo_object_storage_msg_collector:init(true),
    {ok, Msgs3} = leo_object_storage_msg_collector:get(),
    ?assertEqual(0, erlang:length(leo_misc:get_value(?MSG_ITEM_TIMEOUT, Msgs3, []))),
    ?assertEqual(0, erlang:length(leo_misc:get_value(?MSG_ITEM_SLOW_OP, Msgs3, []))),
    ok = leo_object_storage_msg_collector:notify(?MSG_ITEM_TIMEOUT, put, key),
    ok = leo_object_storage_msg_collector:notify(?MSG_ITEM_TIMEOUT, get, key2),
    ok = leo_object_storage_msg_collector:notify(?MSG_ITEM_TIMEOUT, delete, key3),
    ok = leo_object_storage_msg_collector:notify(?MSG_ITEM_SLOW_OP, put, key, 1000),
    ok = leo_object_storage_msg_collector:notify(?MSG_ITEM_SLOW_OP, get, key2, 1000),
    ok = leo_object_storage_msg_collector:notify(?MSG_ITEM_SLOW_OP, delete, key3, 1000),
    {ok, Msgs4} = leo_object_storage_msg_collector:get(),
    ?assertEqual(3, erlang:length(leo_misc:get_value(?MSG_ITEM_TIMEOUT, Msgs4, []))),
    ?assertEqual(3, erlang:length(leo_misc:get_value(?MSG_ITEM_SLOW_OP, Msgs4, []))),
    ok = leo_object_storage_msg_collector:clear(),
    {ok, Msgs5} = leo_object_storage_msg_collector:get(),
    ?assertEqual(0, erlang:length(leo_misc:get_value(?MSG_ITEM_TIMEOUT, Msgs5, []))),
    ?assertEqual(0, erlang:length(leo_misc:get_value(?MSG_ITEM_SLOW_OP, Msgs5, []))),
    ok.

new_([Path1, _]) ->
    %% 1-1.
    NumOfAVS = 4,
    ok = leo_object_storage_api:start([{NumOfAVS, Path1}]),

    Ref = whereis(leo_object_storage_sup),
    ?assertEqual(true, is_pid(Ref)),

    %% {ok, ?AVS_HEADER_VSN_TOBE} =
    %%     leo_object_storage_server:get_avs_version_bin(
    %%       leo_object_storage_api:get_object_storage_pid_first()),
    application:stop(leo_backend_db),
    application:stop(bitcask),
    application:stop(leo_object_storage),

    %% 2. Exception
    Res0 = leo_object_storage_api:start([]),
    ?assertEqual({error, badarg}, Res0),
    ok.

%% Get/Put/Delte
operate_([Path1, Path2]) ->
    application:set_env(leo_object_storage, sync_mode, ?SYNC_MODE_PERIODIC, [{persistent, true}]),
    application:set_env(leo_object_storage, sync_interval_in_ms, 300, [{persistent, true}]),
    application:set_env(leo_object_storage, is_strict_check, true, [{persistent, true}]),
    ok = leo_object_storage_api:start([{4, Path1},{4, Path2}]),

    %% 1. Put
    AddrId = 0,
    Key = <<"air/on/g/string">>,
    Bin = <<"J.S.Bach">>,
    Object = #?OBJECT{method    = put,
                      addr_id   = AddrId,
                      key       = Key,
                      ksize     = byte_size(Key),
                      data      = Bin,
                      dsize     = byte_size(Bin),
                      checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin)),
                      timestamp = leo_date:now(),
                      clock     = leo_date:clock()},
    {ok,_ETag} = leo_object_storage_api:put({AddrId, Key}, Object),

    ZeroByteKey = <<"air/on/g/string/0byte">>,
    {ok,_ETag} = leo_object_storage_api:put(
                   {AddrId, ZeroByteKey}, Object#?OBJECT{
                                                    ksize = byte_size(ZeroByteKey),
                                                    key = ZeroByteKey,
                                                    dsize = 0,
                                                    data = <<>>}),
    %% 2. Get
    {ok, Meta1, Obj0} = leo_object_storage_api:get({AddrId, Key}),
    ?assertEqual(AddrId, Meta1#?METADATA.addr_id),
    ?assertEqual(Key,    Meta1#?METADATA.key),
    ?assertEqual(0,      Meta1#?METADATA.del),
    ?assertEqual(AddrId,         Obj0#?OBJECT.addr_id),
    ?assertEqual(Key,            Obj0#?OBJECT.key),
    ?assertEqual(Bin,            Obj0#?OBJECT.data),
    ?assertEqual(byte_size(Bin), Obj0#?OBJECT.dsize),
    ?assertEqual(0,              Obj0#?OBJECT.del),

    {ok,_ZeroByteMeta, ZeroByteObj} = leo_object_storage_api:get({AddrId, ZeroByteKey}),
    ?assertEqual(<<>>, ZeroByteObj#?OBJECT.data),
    ?assertEqual(0,    ZeroByteObj#?OBJECT.dsize),

    %% 2-1. Head with calculating MD5
    ExpectedMD5 = crypto:hash(md5, Bin),
    Context = crypto:hash_init(md5),
    {ok, MetaMD5, Context2} = leo_object_storage_api:head_with_calc_md5({AddrId, Key}, Context),
    ?assertEqual(ExpectedMD5, crypto:hash_final(Context2)),
    ?assertEqual(AddrId, MetaMD5#?METADATA.addr_id),
    ?assertEqual(Key,    MetaMD5#?METADATA.key),

    %% 3. Store (for Copy)
    ok = leo_object_storage_api:store(Meta1, Bin),
    {ok, Meta1_1, _} = leo_object_storage_api:get({AddrId, Key}),
    ?assertEqual(AddrId, Meta1_1#?METADATA.addr_id),
    ?assertEqual(Key,    Meta1_1#?METADATA.key),
    ?assertEqual(0,      Meta1_1#?METADATA.del),


    %% 4. Get - for range query via HTTP
    %% >> Case of regular.
    {ok, _Meta1_1, Obj0_1} = leo_object_storage_api:get({AddrId, Key}, 4, 7),
    ?assertEqual(4, byte_size(Obj0_1#?OBJECT.data)),
    ?assertEqual(<<"Bach">>, Obj0_1#?OBJECT.data),

    %% >> Case of "end-position over data-size".
    {ok, _Meta1_2, Obj0_2} = leo_object_storage_api:get({AddrId, Key}, 5, 9),
    ?assertEqual(<<>>, Obj0_2#?OBJECT.data),
    ?assertEqual(-2, Obj0_2#?OBJECT.dsize),

    %% >> Case of "end-position is zero". This means "end-position is data-size".
    {ok, _Meta1_3, Obj0_3} = leo_object_storage_api:get({AddrId, Key}, 2, 0),
    ?assertEqual(<<"S.Bach">>, Obj0_3#?OBJECT.data),

    %% >> Case of "start-position over data-size"
    {ok, _Meta1_4, Obj0_4} = leo_object_storage_api:get({AddrId, Key}, 8, 0),
    ?assertEqual(<<>>, Obj0_4#?OBJECT.data),
    ?assertEqual(-2, Obj0_4#?OBJECT.dsize),

    %% >> Case of "end-position is negative". This means retrieving from end
    {ok, _Meta1_5, Obj0_5} = leo_object_storage_api:get({AddrId, Key}, 0, -2),
    ?assertEqual(<<"ch">>, Obj0_5#?OBJECT.data),

    %% 5. Head
    {ok, Res2} = leo_object_storage_api:head({AddrId, Key}),
    Meta2 = binary_to_term(Res2),
    ?assertEqual(AddrId, Meta2#?METADATA.addr_id),
    ?assertEqual(Key,    Meta2#?METADATA.key),
    ?assertEqual(0,      Meta2#?METADATA.del),

    %% 5-2 Head with Check AVS for normal cases
    {ok, _} = leo_object_storage_api:head_with_check_avs({AddrId, Key}, check_header),
    {ok, _} = leo_object_storage_api:head_with_check_avs({AddrId, Key}, check_md5),

    %% 6. Delete
    Object2 = #?OBJECT{method    = delete,
                       key       = Key,
                       ksize     = byte_size(Key),
                       addr_id   = AddrId,
                       data      = <<>>,
                       dsize     = 0,
                       checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, <<>>)),
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
    ?assertEqual(AddrId, Meta5#?METADATA.addr_id),
    ?assertEqual(Key,    Meta5#?METADATA.key),
    ?assertEqual(1,      Meta5#?METADATA.del),

    %% 8. Put with custom-metadata
    ClusterId = "cluster_id",
    NumOfReplicas = 1,
    Ver = 1,
    UDM = [{<<"name">>, <<"LeoFS">>},
           {<<"category">>, <<"distributed storage">>},
           {<<"url">>, <<"leo-project.net/leofs">>}
          ],
    CMeta = [{?PROP_CMETA_CLUSTER_ID, ClusterId},
             {?PROP_CMETA_NUM_OF_REPLICAS, NumOfReplicas},
             {?PROP_CMETA_VER, Ver},
             {?PROP_CMETA_UDM, UDM}
            ],
    CMetaBin = leo_object_storage_transformer:list_to_cmeta_bin(CMeta),

    Obj2 = #?OBJECT{method    = put,
                    addr_id   = AddrId,
                    key       = Key,
                    ksize     = byte_size(Key),
                    data      = Bin,
                    dsize     = byte_size(Bin),
                    meta      = CMetaBin,
                    msize     = byte_size(CMetaBin),
                    checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin)),
                    timestamp = leo_date:now(),
                    clock     = leo_date:clock(),

                    cluster_id = ClusterId,
                    num_of_replicas = NumOfReplicas,
                    ver = Ver
                   },
    {ok,_} = leo_object_storage_api:put({AddrId, Key}, Obj2),
    {ok, Res6, Res7} = leo_object_storage_api:get({AddrId, Key}),
    %% Head with Check AVS for normal cases with custom metadata
    {ok, _} = leo_object_storage_api:head_with_check_avs({AddrId, Key}, check_header),
    {ok, _} = leo_object_storage_api:head_with_check_avs({AddrId, Key}, check_md5),
    ?assertEqual(ClusterId,     Res6#?METADATA.cluster_id),
    ?assertEqual(NumOfReplicas, Res6#?METADATA.num_of_replicas),
    ?assertEqual(Ver,           Res6#?METADATA.ver),
    ?assertEqual(ClusterId,     Res7#?OBJECT.cluster_id),
    ?assertEqual(NumOfReplicas, Res7#?OBJECT.num_of_replicas),
    ?assertEqual(Ver,           Res7#?OBJECT.ver),

    ?debugVal(binary_to_term(Res7#?OBJECT.meta)),
     CMeta_1 = binary_to_term(Res7#?OBJECT.meta),
    ?assertEqual(ClusterId, leo_misc:get_value(?PROP_CMETA_CLUSTER_ID, CMeta_1)),
    ?assertEqual(NumOfReplicas, leo_misc:get_value(?PROP_CMETA_NUM_OF_REPLICAS, CMeta_1)),
    ?assertEqual(Ver, leo_misc:get_value(?PROP_CMETA_VER, CMeta_1)),
    ?assertEqual(UDM, leo_misc:get_value(?PROP_CMETA_UDM, CMeta_1)),

    %% Get AVS broken
    {ok, Offset} = leo_object_storage_api:get_eof_offset(term_to_binary({AddrId, Key})),
    ok = leo_object_storage_api:modify_data({AddrId, Key}, <<"deadbeaf">>, Offset - 8),
    {error, invalid_object} = leo_object_storage_api:get({AddrId, Key}),

    %% Head with Check AVS for abnormal cases
    {error, invalid_method} = leo_object_storage_api:head_with_check_avs({AddrId, Key}, check_body),
    {error, _} = leo_object_storage_api:head_with_check_avs({AddrId, Key}, check_header),
    {error, invalid_object} = leo_object_storage_api:head_with_check_avs({AddrId, Key}, check_md5),

    application:stop(leo_backend_db),
    application:stop(bitcask),
    application:stop(leo_object_storage),
    ok.

fetch_by_addr_id_([Path1, Path2]) ->
    application:set_env(leo_object_storage, sync_mode, ?SYNC_MODE_WRITETHROUGH, [{persistent, true}]),
    ok = leo_object_storage_api:start([{4, Path1},{4, Path2}]),

    try
        ok = put_test_data(0,    <<"air/on/g/string/0">>, <<"JSB0">>),
        ok = put_test_data(127,  <<"air/on/g/string/1">>, <<"JSB1">>),
        ok = put_test_data(255,  <<"air/on/g/string/2">>, <<"JSB2">>),
        ok = put_test_data(511,  <<"air/on/g/string/3">>, <<"JSB3">>),
        ok = put_test_data(1023, <<"air/on/g/string/4">>, <<"JSB4">>),

        FromAddrId = 0,
        ToAddrId   = 255,

        Fun = fun(_K, V, Acc) ->
                      %% Key = binary_to_term(K),
                      %% AddrId = leo_object_storage_api:head(Key),
                      Metadata      = binary_to_term(V),
                      AddrId = Metadata#?METADATA.addr_id,

                      case (AddrId >= FromAddrId andalso
                            AddrId =< ToAddrId) of
                          true  ->
                              io:format(user, "[debug]meta:~p~n", [Metadata]),
                              [Metadata|Acc];
                          false ->
                              Acc
                      end
              end,
        {ok, Res} = leo_object_storage_api:fetch_by_addr_id(0, Fun),
        ?assertEqual(3, length(Res))
    after
        application:stop(leo_backend_db),
        application:stop(bitcask),
        application:stop(leo_object_storage)
    end,
    ok.

fetch_by_key_([Path1, Path2]) ->
    application:set_env(leo_object_storage, sync_mode, ?SYNC_MODE_WRITETHROUGH, [{persistent, true}]),
    ok = leo_object_storage_api:start([{4, Path1},{4, Path2}]),
    try
        ok = put_test_data(0,    <<"air/on/g/string/0">>, <<"JSB0">>),
        ok = put_test_data(127,  <<"air/on/g/string/1">>, <<"JSB1">>),
        ok = put_test_data(255,  <<"air/on/g/string/2">>, <<"JSB2">>),
        ok = put_test_data(511,  <<"air/on/g/string/3">>, <<"JSB3">>),
        ok = put_test_data(1023, <<"air/on/g/string/4">>, <<"JSB4">>),

        Fun = fun(K, V, Acc) ->
                      Metadata      = binary_to_term(V),

                      case (K == <<"air/on/g/string/0">> orelse
                            K == <<"air/on/g/string/2">> orelse
                            K == <<"air/on/g/string/4">>) of
                          true  ->
                              io:format(user, "[debug]meta:~p~n", [Metadata]),
                              [Metadata|Acc];
                          false ->
                              Acc
                      end
              end,
        {ok, Res} = leo_object_storage_api:fetch_by_key(<<"air/on/g/string">>, Fun),
        ?assertEqual(3, length(Res)),
        {ok, Res2} = leo_object_storage_api:fetch_by_key(<<"air/on/g/string">>, Fun, 2),
        ?assertEqual(2, length(Res2)),
        {ok, Res3} = leo_object_storage_api:fetch_by_key_in_parallel(<<"air/on/g/string">>, Fun, undefined),
        ?assertEqual(3, length(Res3)),
        {ok, Res4} = leo_object_storage_api:fetch_by_key_in_parallel(<<"air/on/g/string">>, Fun, 1),
        ?assertEqual(1, length(Res4))
    after
        application:stop(leo_backend_db),
        application:stop(bitcask),
        application:stop(leo_object_storage)
    end,
    ok.

stats_test_() ->
    {timeout, 15,
     [?_test(
         begin
             application:start(sasl),
             application:start(os_mon),
             application:start(crypto),
             Path1 = "./avs1",
             Path2 = "./avs2",
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

             application:stop(leo_backend_db),
             application:stop(bitcask),
             application:stop(leo_object_storage),
             io:format(user, "*** [test]stopped ~n~n", []),

             %% relaunch and validate stored datas
             ok = leo_object_storage_api:start([{4, Path1},{4, Path2}]),
             io:format(user, "~n*** [test]restarted ~n", []),
             {ok, Res1} = leo_object_storage_api:stats(),
             ?debugVal(Res1),
             ?assertEqual(8, length(Res1)),

             {SumTotal0, SumActive0} =
                 lists:foldl(
                   fun(#storage_stats{file_path  = _ObjPath,
                                      total_num  = Total,
                                      active_num = Active},
                       {SumTotal, SumActive}) ->
                           ?debugVal({SumTotal, SumActive}),
                           {SumTotal + Total, SumActive + Active}
                   end, {0, 0}, Res1),
             ?assertEqual(9, SumTotal0),
             ?assertEqual(8, SumActive0),

             catch leo_object_storage_sup:stop(),
             io:format(user, "*** [test]stopped2 ~n", []),
             application:stop(leo_backend_db),
             application:stop(bitcask),
             application:stop(leo_object_storage),
             application:stop(crypto),
             application:stop(os_mon),
             application:stop(sasl),
             os:cmd("rm -rf " ++ Path1),
             os:cmd("rm -rf " ++ Path2),
             true end)]}.

compaction_2_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("***** COMPACTION.START #4 *****"),
             os:cmd("rm -rf " ++ ?AVS_DIR_FOR_COMPACTION),
             application:start(sasl),
             application:start(os_mon),
             application:start(crypto),
             ok
     end,
     fun (_) ->
             application:stop(leo_object_storage),
             application:stop(crypto),
             application:stop(os_mon),
             application:stop(sasl),
             timer:sleep(1000),
             ?debugVal("***** COMPACTION.END #4 *****"),
             ok
     end,
     [
      {"test compaction - irregular case",
       {timeout, 1000, fun compact_2/0}}
     ]}.

compact_2() ->
    Path1 = ?AVS_DIR_FOR_COMPACTION ++ "avs1",
    Path2 = ?AVS_DIR_FOR_COMPACTION ++ "avs2",

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
        lists:foldl(fun(#storage_stats{file_path  = _ObjPath,
                                       total_num  = Total,
                                       active_num = Active},
                        {SumTotal, SumActive}) ->
                            {SumTotal + Total, SumActive + Active}
                    end, {0, 0}, Res0),
    ?assertEqual(9, SumTotal0),
    ?assertEqual(8, SumActive0),
    ?assertEqual({error,badstate}, leo_compact_fsm_controller:suspend()),
    ?assertEqual({error,badstate}, leo_compact_fsm_controller:resume()),

    %% append incorrect data based on IS devenv's corrupted data
    {ok, CorruptedDataBlock} = file:read_file("../test/broken_part.avs"),
    _ = leo_object_storage_api:add_incorrect_data(CorruptedDataBlock),

    ok = put_test_data(0,    <<"air/on/g/string/0">>, <<"JSB0-1">>),
    ok = put_test_data(511,  <<"air/on/g/string/3">>, <<"JSB3-1">>),

    ok = put_test_data(10001, <<"air/on/g/string/1/0">>, <<"JSB0-1">>),
    ok = put_test_data(10002, <<"air/on/g/string/1/2">>, <<"JSB0-1">>),
    ok = put_test_data(10003, <<"air/on/g/string/1/3">>, <<"JSB0-1">>),
    ok = put_test_data(10004, <<"air/on/g/string/1/4">>, <<"JSB0-1">>),
    ok = put_test_data(10005, <<"air/on/g/string/1/5">>, <<"JSB0-1">>),
    ok = put_test_data(10006, <<"air/on/g/string/1/6">>, <<"JSB0-1">>),

    %% append an object w/a user defined metadata
    UDM_1 = [{<<"name">>, <<"LeoFS">>},
             {<<"category">>, <<"distributed storage">>},
             {<<"url">>, <<"leo-project.net/leofs">>}
            ],
    CMeta_1 = [{?PROP_CMETA_CLUSTER_ID, 'leofs_1'},
               {?PROP_CMETA_NUM_OF_REPLICAS, 3},
               {?PROP_CMETA_PREFERRED_R, 1},
               {?PROP_CMETA_PREFERRED_W, 2},
               {?PROP_CMETA_PREFERRED_D, 2},
               {?PROP_CMETA_VER, leo_date:clock()},
               {?PROP_CMETA_UDM, UDM_1}
              ],
    CMetaBin_1 = leo_object_storage_transformer:list_to_cmeta_bin(CMeta_1),
    ok = put_test_data_with_custom_metadata(
           90001, <<"air/on/g/string/2/0">>, <<"JSB-1-W-CMETA">>, CMetaBin_1),
    ok = put_test_data_with_custom_metadata(
           90002, <<"air/on/g/string/2/1">>, <<>>, CMetaBin_1), %% 'dsize = 0'

    {ok,_Metadata_UDM_1,_Object_UDM_1} = get_test_data(90001, <<"air/on/g/string/2/0">>),
    ?debugVal(binary_to_term(_Metadata_UDM_1#?METADATA.meta)),
    ?debugVal(_Metadata_UDM_1#?METADATA.msize),
    ?debugVal(binary_to_term(_Object_UDM_1#?OBJECT.meta)),
    ?debugVal(_Object_UDM_1#?OBJECT.msize),
    ?assertEqual(true, _Metadata_UDM_1#?METADATA.meta == _Object_UDM_1#?OBJECT.meta),
    ?assertEqual(true, _Metadata_UDM_1#?METADATA.msize == _Object_UDM_1#?OBJECT.msize),

    {ok, UDM_1a} = leo_object_storage_transformer:get_udm_from_cmeta_bin(
                     _Metadata_UDM_1#?METADATA.meta),
    ?debugVal(UDM_1a),
    ?assertEqual(true, UDM_1 == UDM_1a),


    {ok,_Metadata_UDM_1b,_Object_UDM_1b} = get_test_data(90002, <<"air/on/g/string/2/1">>),
    ?debugVal(binary_to_term(_Metadata_UDM_1b#?METADATA.meta)),
    ?debugVal(_Metadata_UDM_1b#?METADATA.msize),
    ?debugVal(binary_to_term(_Object_UDM_1b#?OBJECT.meta)),
    ?debugVal(_Object_UDM_1b#?OBJECT.msize),
    ?assertEqual(true, _Metadata_UDM_1b#?METADATA.meta == _Object_UDM_1b#?OBJECT.meta),
    ?assertEqual(true, _Metadata_UDM_1b#?METADATA.msize == _Object_UDM_1b#?OBJECT.msize),

    {ok, _Metadata_UDM_1h} = leo_object_storage_api:head({90001, <<"air/on/g/string/2/0">>}),
    _Metadata_UDM_1h_2 = binary_to_term(_Metadata_UDM_1h),
    ?debugVal(binary_to_term(_Metadata_UDM_1h_2#?METADATA.meta)),
    ?debugVal(_Metadata_UDM_1h_2#?METADATA.msize),
    ?assertEqual(true, _Metadata_UDM_1#?METADATA.meta == _Metadata_UDM_1h_2#?METADATA.meta),
    ?assertEqual(true, _Metadata_UDM_1#?METADATA.msize == _Metadata_UDM_1h_2#?METADATA.msize),

    %% preparing the data-compaction
    AllTargets = [_Pid || {_Pid,_}
                              <- leo_object_storage_api:get_object_storage_pid('all')],
    ?assertEqual({ok, #compaction_stats{status = ?ST_IDLING,
                                        total_num_of_targets    = 8,
                                        num_of_reserved_targets = 0,
                                        num_of_pending_targets  = 8,
                                        num_of_ongoing_targets  = 0,
                                        reserved_targets = [],
                                        pending_targets  = AllTargets,
                                        ongoing_targets  = [],
                                        latest_exec_datetime = 0
                                       }}, leo_compact_fsm_controller:state()),
    AddrId = 4095,
    Key    = <<"air/on/g/string/7">>,
    Object = #?OBJECT{method    = delete,
                      key       = Key,
                      ksize     = byte_size(Key),
                      addr_id   = AddrId,
                      data      = <<>>,
                      dsize     = 0,
                      checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, <<>>)),
                      timestamp = leo_date:now(),
                      clock     = leo_date:clock(),
                      del       = 1},
    ok = leo_object_storage_api:delete({AddrId, Key}, Object),

    %% inspect for compaction
    {ok, Res1} = leo_object_storage_api:stats(),
    ?debugVal(Res1),
    {SumTotal1, SumActive1, SumTotalSize1, SumActiveSize1}
        = get_avs_stats_summary(Res1),
    ?assertEqual(19, SumTotal1),
    ?assertEqual(15, SumActive1),
    ?assertEqual(true, SumTotalSize1 > SumActiveSize1),

    {ok,_Metadata_UDM_2,_Object_UDM_2} = get_test_data(90001, <<"air/on/g/string/2/0">>),
    ?debugVal({_Metadata_UDM_2,_Object_UDM_2}),
    ?debugVal(binary_to_term(_Metadata_UDM_2#?METADATA.meta)),
    ?debugVal(_Metadata_UDM_2#?METADATA.msize),
    ?debugVal(binary_to_term(_Object_UDM_2#?OBJECT.meta)),
    ?debugVal(_Object_UDM_2#?OBJECT.msize),
    ?assertEqual(true, _Metadata_UDM_2#?METADATA.meta == _Object_UDM_2#?OBJECT.meta),
    ?assertEqual(true, _Metadata_UDM_2#?METADATA.msize == _Object_UDM_2#?OBJECT.msize),

    ?assertEqual(true, _Metadata_UDM_1#?METADATA.meta == _Metadata_UDM_2#?METADATA.meta),
    ?assertEqual(true, _Metadata_UDM_1#?METADATA.msize == _Metadata_UDM_2#?METADATA.msize),
    ?assertEqual(true, _Object_UDM_1#?OBJECT.meta == _Object_UDM_2#?OBJECT.meta),
    ?assertEqual(true, _Object_UDM_1#?OBJECT.msize == _Object_UDM_2#?OBJECT.msize),
    timer:sleep(1000),


    %% executing the data-compaction
    FunHasChargeOfNode = fun(_Key_,_NumOfReplicas_) ->
                                 true
                         end,
    TargetPids = [_Pid || {_Pid,_}
                              <- leo_object_storage_api:get_object_storage_pid(all)],
    io:format(user, "*** target-pids:~p~n", [TargetPids]),

    ok = leo_compact_fsm_controller:run(TargetPids, 2, FunHasChargeOfNode),

    {ok, CompactionStats} = leo_compact_fsm_controller:state(),
    ?assertEqual(?ST_RUNNING, CompactionStats#compaction_stats.status),
    ?assertEqual(8, CompactionStats#compaction_stats.total_num_of_targets),
    ?assertEqual(true, 0 < CompactionStats#compaction_stats.num_of_pending_targets),
    ?assertEqual(true, 0 < CompactionStats#compaction_stats.num_of_ongoing_targets),

    ?assertEqual(ok, leo_compact_fsm_controller:suspend()),
    {ok, CompactionStats2} = leo_compact_fsm_controller:state(),
    ?assertEqual(?ST_SUSPENDING, CompactionStats2#compaction_stats.status),
    %% keep # of ongoing/pending fixed during suspend
    Pending = CompactionStats2#compaction_stats.num_of_pending_targets,
    Ongoing = CompactionStats2#compaction_stats.num_of_ongoing_targets,
    timer:sleep(1000),
    ?assertEqual(Pending, CompactionStats2#compaction_stats.num_of_pending_targets),
    ?assertEqual(Ongoing, CompactionStats2#compaction_stats.num_of_ongoing_targets),
    %% operation during suspend
    TestAddrId0 = 0,
    TestKey0    = <<"air/on/g/string/0">>,
    TestAddrId1 = 511,
    TestKey1    = <<"air/on/g/string/3">>,
    {ok, _, _} = get_test_data(TestAddrId0, TestKey0),
    {ok, _, _} = get_test_data(TestAddrId1, TestKey1),
    ?assertEqual(ok, leo_compact_fsm_controller:resume()),


    %% Waiting for finished data-compaction
    ok = check_status(),
    timer:sleep(1000),
    {ok, Res2} = leo_object_storage_api:stats(),
    ?debugVal(Res2),
    {SumTotal2, SumActive2, SumTotalSize2, SumActiveSize2}
        = get_avs_stats_summary(Res2),

    {ok,_Metadata_UDM_3,_Object_UDM_3} = get_test_data(90001, <<"air/on/g/string/2/0">>),
    ?debugVal({_Metadata_UDM_3,_Object_UDM_3}),
    ?debugVal(binary_to_term(_Metadata_UDM_3#?METADATA.meta)),
    ?debugVal(_Metadata_UDM_3#?METADATA.msize),
    ?debugVal(binary_to_term(_Object_UDM_3#?OBJECT.meta)),
    ?debugVal(_Object_UDM_3#?OBJECT.msize),

    ?assertEqual(true, _Metadata_UDM_3#?METADATA.meta == _Object_UDM_3#?OBJECT.meta),
    ?assertEqual(true, _Metadata_UDM_3#?METADATA.msize == _Object_UDM_3#?OBJECT.msize),
    ?assertEqual(true, _Metadata_UDM_1#?METADATA.meta == _Metadata_UDM_3#?METADATA.meta),
    ?assertEqual(true, _Metadata_UDM_1#?METADATA.msize == _Metadata_UDM_3#?METADATA.msize),
    ?assertEqual(true, _Object_UDM_1#?OBJECT.meta == _Object_UDM_3#?OBJECT.meta),
    ?assertEqual(true, _Object_UDM_1#?OBJECT.msize == _Object_UDM_3#?OBJECT.msize),

    io:format(user, "[debug] summary:~p~n", [{SumTotal2, SumActive2, SumTotalSize2, SumActiveSize2}]),
    ?debugVal({SumTotal2, SumActive2}),
    ?assertEqual(15, SumTotal2),
    ?assertEqual(15, SumActive2),
    ?assertEqual(true, SumTotalSize2 == SumActiveSize2),
    timer:sleep(1000),

    %% confirm whether first compaction have broken avs files or not
    ok = leo_compact_fsm_controller:run(TargetPids, 2, FunHasChargeOfNode),
    ok = check_status(),
    timer:sleep(1000),
    {ok, Res3} = leo_object_storage_api:stats(),
    ?debugVal(Res3),
    Ret3_1 = get_avs_stats_summary(Res3),
    ?debugVal(Ret3_1),
    {SumTotal2, SumActive2, SumTotalSize2, SumActiveSize2} = Ret3_1,

    %% inspect for after compaction
    TestAddrId0 = 0,
    TestKey0    = <<"air/on/g/string/0">>,
    TestAddrId1 = 511,
    TestKey1    = <<"air/on/g/string/3">>,

    {ok, Meta0, Obj0} = get_test_data(TestAddrId0, TestKey0),
    {ok, Meta1, Obj1} = get_test_data(TestAddrId1, TestKey1),

    ?assertEqual(TestAddrId0,  Meta0#?METADATA.addr_id),
    ?assertEqual(TestKey0,     Meta0#?METADATA.key),
    ?assertEqual(6,            Meta0#?METADATA.dsize),
    ?assertEqual(0,            Meta0#?METADATA.del),
    ?assertEqual(TestAddrId0,  Obj0#?OBJECT.addr_id),
    ?assertEqual(TestKey0,     Obj0#?OBJECT.key),
    ?assertEqual(6,            Obj0#?OBJECT.dsize),
    ?assertEqual(<<"JSB0-1">>, Obj0#?OBJECT.data),
    ?assertEqual(0,            Obj0#?OBJECT.del),

    ?assertEqual(TestAddrId1,  Meta1#?METADATA.addr_id),
    ?assertEqual(TestKey1,     Meta1#?METADATA.key),
    ?assertEqual(6,            Meta1#?METADATA.dsize),
    ?assertEqual(0,            Meta1#?METADATA.del),
    ?assertEqual(TestAddrId1,  Obj1#?OBJECT.addr_id),
    ?assertEqual(TestKey1,     Obj1#?OBJECT.key),
    ?assertEqual(6,            Obj1#?OBJECT.dsize),
    ?assertEqual(<<"JSB3-1">>, Obj1#?OBJECT.data),
    ?assertEqual(0,            Obj1#?OBJECT.del),
    ok.

%% proper_test_() ->
%%     {timeout, 60000, ?_assertEqual([], proper:module(leo_object_storage_api_prop))}.


%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
get_avs_stats_summary(ResStats) ->
    lists:foldl(
      fun(#storage_stats{file_path  = _ObjPath,
                         total_sizes = TotalSize,
                         active_sizes = ActiveSize,
                         total_num  = Total,
                         active_num = Active} = StorageStats,
          {SumTotal, SumActive, SumTotalSize, SumActiveSize}) ->
              io:format(user, "[debug] ~p~n",[StorageStats]),
              {SumTotal      + Total,
               SumActive     + Active,
               SumTotalSize  + TotalSize,
               SumActiveSize + ActiveSize}
      end, {0, 0, 0, 0}, ResStats).


put_test_data(AddrId, Key, Bin) ->
    Object = #?OBJECT{method    = put,
                      addr_id   = AddrId,
                      key       = Key,
                      ksize     = byte_size(Key),
                      data      = Bin,
                      dsize     = byte_size(Bin),
                      checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin)),
                      timestamp = leo_date:now(),
                      clock     = leo_date:clock()
                     },
    {ok, _Checksum} = leo_object_storage_api:put({AddrId, Key}, Object),
    ok.

put_test_data_with_custom_metadata(AddrId, Key, Bin, CMetaBin) ->
    Object = #?OBJECT{method    = put,
                      addr_id   = AddrId,
                      key       = Key,
                      ksize     = byte_size(Key),
                      data      = Bin,
                      dsize     = byte_size(Bin),
                      meta      = CMetaBin,
                      msize     = byte_size(CMetaBin),
                      checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin)),
                      timestamp = leo_date:now(),
                      clock     = leo_date:clock()
                     },
    {ok, _Checksum} = leo_object_storage_api:put({AddrId, Key}, Object),
    ok.

get_test_data(AddrId, Key) ->
    leo_object_storage_api:get({AddrId, Key}).

-endif.

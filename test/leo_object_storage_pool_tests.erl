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
-module(leo_object_storage_pool_tests).
-author('yosuke hara').

-include_lib("eunit/include/eunit.hrl").
-include("leo_object_storage.hrl").

%%--------------------------------------------------------------------
%% TEST FUNCTIONS
%%--------------------------------------------------------------------
-ifdef(EUNIT).

all_test_() ->
    {foreach, fun setup/0, fun teardown/1,
     [{with, [T]} || T <- [fun all_/1]]}.

setup() ->
    ok.

teardown(_) ->
    ok.


all_(_) ->
    AddrId = 255,
    Key  = "air/on/g/string",
    Bin  = <<"J.S.Bach">>,
    Size = byte_size(Bin),
    Ring = 12345,

    Object = #object{addr_id = AddrId,
                     key     = Key,
                     data    = Bin,
                     dsize   = Size
                    },
    Pool = leo_object_storage_pool:new(Object),

    %%get/1, set_ring_hash/2, head/1
    Res0 = leo_object_storage_pool:get(Pool),
    ?assertEqual(AddrId, Res0#object.addr_id),
    ?assertEqual(Key,    Res0#object.key),
    ?assertEqual(Bin,    Res0#object.data),
    ?assertEqual(Size,   Res0#object.dsize),
    ?assertEqual(0,      Res0#object.del),

    ok = leo_object_storage_pool:set_ring_hash(Pool, Ring),

    Res2 = leo_object_storage_pool:head(Pool),
    ?assertEqual(AddrId, Res2#metadata.addr_id),
    ?assertEqual(Key,    Res2#metadata.key),
    ?assertEqual(Size,   Res2#metadata.dsize),
    ?assertEqual(Ring,   Res2#metadata.ring_hash),

    leo_object_storage_pool:destroy(Pool),
    ok.

-endif.


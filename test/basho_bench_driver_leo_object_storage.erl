%%======================================================================
%%
%% Leo Object Storage
%%
%% Copyright (c) 2012-2015 Rakuten, Inc.
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
%% Leo Object Storage - Stress Test (basho_bench)
%% @doc
%% @end
%%======================================================================
-module(basho_bench_driver_leo_object_storage).

-export([new/1,
         run/4]).

-include_lib("leo_object_storage/include/leo_object_storage.hrl").


%% @doc initialize
%%
-spec(new(any()) ->
             ok).
new(1) ->
    Procs = basho_bench_config:get(obj_storage_procs, 64),
    Path  = basho_bench_config:get(obj_storage_path,  "./avs/"),
    ok = leo_object_storage_api:start([{Procs, Path}]),
    {ok, null};
new(_) ->
    {ok, null}.


%% @doc run.
%%
-spec(run(get, any(), any(), any()) ->
             {ok, any()} | {error, any(), any()}).
run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case leo_object_storage_api:get({0, Key}) of
        {ok, _, _} ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Cause} ->
            {error, Cause, State}
    end;

run(put, KeyGen, ValueGen, State) ->
    Key  = KeyGen(),
    Val  = ValueGen(),
    case put_test_data(Key, Val) of
        {ok, _ETag} ->
            {ok, State};
        {error, Cause} ->
            {error, Cause, State}
    end.

put_test_data(Key, Bin) ->
    Object = #?OBJECT{method    = put,
                      addr_id   = 0,
                      key       = Key,
                      ksize     = byte_size(Key),
                      data      = Bin,
                      dsize     = byte_size(Bin),
                      checksum  = leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin)),
                      timestamp = leo_date:now(),
                      clock     = leo_date:clock()
                     },
    leo_object_storage_api:put({0, Key}, Object).

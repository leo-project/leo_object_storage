%%======================================================================
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
%% ---------------------------------------------------------------------
%% Leo Object Storage - Stress Test (basho_bench)
%% @doc
%% @end
%%======================================================================
-module(basho_bench_driver_leo_object_storage).

-export([new/1,
         run/4]).


%% @doc initialize
%%
-spec(new(any()) ->
             ok).
new(_Id) ->
    Procs = basho_bench_config:get(obj_storage_procs, 64),
    Path  = basho_bench_config:get(obj_storage_path,  "/avs/"),
    ok = leo_object_storage_api:new(0, Procs, Path),
    {ok, null}.


%% @doc run.
%%
-spec(run(get, any(), any(), any()) ->
             {ok, any()} | {error, any(), any()}).
run(get,_KeyGen, _ValueGen, State) ->
    %% Key = KeyGen(),
    {ok, State};
    %% case leo_redundant_manager_api:get_redundancies_by_key(integer_to_list(Key)) of
    %%     {ok, _} ->
    %%         {ok, State};
    %%     {error, Reason} ->
    %%         {error, Reason, State}
    %% end;

run(put, KeyGen, ValueGen, State) ->
    _Key = KeyGen(),
    _Val = ValueGen(),
    {ok, State}.

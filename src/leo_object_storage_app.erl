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
%% Leo Object Storage
%% @doc
%% @reference
%% @end
%%======================================================================
-module(leo_object_storage_app).

-author('Yosuke Hara').

-behaviour(application).

-include_lib("eunit/include/eunit.hrl").

%% Application and Supervisor callbacks
-export([start/2, prep_stop/1, stop/1,
         profile_output/0]).

%%----------------------------------------------------------------------
%% Application behaviour callbacks
%%----------------------------------------------------------------------
start(_Type, _Args) ->
    consider_profiling(),
    case leo_object_storage_sup:start_link() of
        {ok,_Pid} = Ret ->
            Ret;
        Other ->
            Other
    end.

prep_stop(_State) ->
    ok = leo_object_storage_sup:stop(),
    ok.

stop(_State) ->
    ok.


-spec profile_output() -> ok.
profile_output() ->
    eprof:stop_profiling(),
    eprof:log("leo_object_storage.procs.profile"),
    eprof:analyze(procs),
    eprof:log("leo_object_storage.total.profile"),
    eprof:analyze(total).


-spec consider_profiling() -> profiling | not_profiling | {error, any()}.
consider_profiling() ->
    case application:get_env(leo_object_storage, profile) of
        {ok, true} ->
            {ok, _Pid} = eprof:start(),
            eprof:start_profiling([self()]);
        _ ->
            not_profiling
    end.

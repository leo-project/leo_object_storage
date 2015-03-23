%%====================================================================
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
%% -------------------------------------------------------------------
%% Leo Object Storage - Property TEST
%% @doc
%% @end
%%====================================================================
-module(leo_object_storage_api_prop).

-author('Yosuke Hara').

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("leo_object_storage.hrl").

-behaviour('proper_statem').

-export([initial_state/0,
         command/1,
         precondition/2,
         next_state/3,
         postcondition/3]).


-type obj_type() :: 'haystack'.

-record(state, {stored = []  :: [string()],
                type         :: obj_type()
               }).


-define(PATH1, "./temp1").
-define(PATH2, "./temp2").
-define(NUM_OF_PROCS1, 3).
-define(NUM_OF_PROCS2, 3).


%% @doc Utils
%%
key() ->
    N = case erlang:get("K") of
            undefined -> 0;
            20        -> 0;
            Value     -> Value
        end,
    erlang:put("K", N+1),
    lists:append(["key_", integer_to_list(N)]).

objpool(Method) ->
    N = erlang:get("K"),
    Key = lists:append(["key_", integer_to_list(N)]),
    Bin = crypto:rand_bytes(1024),
    leo_object_storage_pool:new(#?OBJECT{method   = Method,
                                         addr_id  = 0,
                                         key      = Key,
                                         data     = Bin,
                                         dsize    = byte_size(Bin)}).

%% @doc Property TEST
%%
prop_store() ->
    ?FORALL(Cmds, commands(?MODULE, initial_state()),
            begin
                ok = leo_object_storage_api:start([{?NUM_OF_PROCS1, ?PATH1},
                                                   {?NUM_OF_PROCS2, ?PATH2}]),
                {H,S,Res} = run_commands(?MODULE, Cmds),
                ?debugVal({H,S,Res}),

                application:stop(leo_backend_db),
                application:stop(bitcask),
                application:stop(leo_object_storage),
                os:cmd("rm -rf " ++ ?PATH1),
                os:cmd("rm -rf " ++ ?PATH2),

                ?WHENFAIL(
                   io:format("History: ~p\nState: ~p\nRes: ~p\n", [H,S,Res]),
                   collect('haystack', Res =:= ok))
            end).



%% @doc Initialize state
%%
initial_state() ->
    #state{type = 'haystack'}.


%% @doc Command
%%
command(_S) ->
    Cmd0 = [{call, leo_object_storage_api, put,    [key(), objpool(put)]},
            {call, leo_object_storage_api, get,    [key()]},
            {call, leo_object_storage_api, delete, [key(), objpool(delete)]}],
    oneof(Cmd0).


%% @doc Pre-Condition
%%
precondition(_S, {call,_,_,_}) ->
    true.


%% @doc Next-State
%%
next_state(S, _V, {call,_,put,[Key, _ObjectPool]}) ->
    ?debugVal({put, _V}),
    case proplists:is_defined(Key, S#state.stored) of
        false ->
            S#state{stored = S#state.stored ++ [Key]};
        true ->
            S
    end;

next_state(S, _V, {call,_,delete,[Key, _ObjectPool]}) ->
    ?debugVal({delete, _V}),
    S#state{stored=proplists:delete(Key, S#state.stored)};
next_state(S, _V, {call,_,_,_}) ->
    %% ?debugVal(_V),
    S.


%% @doc Post-Condition
%%
postcondition(_S,_V,_) ->
    %% ?debugVal(_V),
    true.

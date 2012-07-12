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
%% Leo Object Storage - Pool
%% @doc
%% @end
%%======================================================================
-module(leo_object_storage_pool).

-author('Yosuke Hara').
-vsn('0.9.0').

-include("leo_object_storage.hrl").

-export([new/1, new/2, destroy/1,
         loop/4,
         get/1, set_ring_hash/2, head/1]).

-define(DEF_TIMEOUT, 3000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Create a process
%%
-spec(new(#object{}) ->
             {ok, pid()}).
new(Object) ->
    new(Object, ?DEF_TIMEOUT).

-spec(new(#object{}, integer()) ->
             {ok, pid()}).
new(#object{clock = 0} = Object, Timeout) ->
    new(Object#object{clock = leo_utils:clock()}, Timeout);

new(Object0, Timeout) ->
    Key = Object0#object.key,
    KeyBin  = erlang:list_to_binary(Key),
    Object1 = Object0#object{
                key_bin = KeyBin,
                ksize   = erlang:byte_size(KeyBin)},

    #object{key       = Key,
            addr_id   = AddrId,
            ksize     = KSize,
            dsize     = DSize,
            timestamp = Timestamp} = Object1,

    spawn(?MODULE, loop, [Key, #metadata{key       = Key,
                                         addr_id   = AddrId,
                                         ksize     = KSize,
                                         dsize     = DSize,
                                         timestamp = Timestamp}, Object1, Timeout]).


%% @doc Receiver
%%
-spec(loop(string(), #metadata{}, #object{}, integer()) ->
             ok).
loop(Key, Meta, DataObj, Timeout) ->
    receive
        {Client, get} ->
            Client ! {ok, DataObj},
            loop(Key, Meta, DataObj, Timeout);
        {Client, set_ring_hash, RingHash} ->
            Client ! ok,
            loop(Key, Meta#metadata{ring_hash = RingHash},
                 DataObj#object{ring_hash = RingHash}, Timeout);
        {Client, head} ->
            Client ! {ok, Meta},
            loop(Key, Meta, DataObj, Timeout);
        {_Client, destroy} ->
            destroy_fun()
    after Timeout ->
            destroy_fun()
    end.


%% @doc Destroy own process
%%
-spec(destroy(pid) ->
             ok).
destroy(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! {self(), destroy};
        false ->
            void
    end,
    ok.


%% @doc Retrieve an object
%%
-spec(get(pid()) ->
             any() | not_found).
get(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! {self(), get},
            receive
                {ok, Res} ->
                    Res
            after ?DEF_TIMEOUT ->
                    not_found
            end;
        false ->
            not_found
    end.


%% @doc Set ring-hach in own process
%%
set_ring_hash(Pid, RingHash) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! {self(), set_ring_hash, RingHash},
            receive
                ok ->
                    ok
            after ?DEF_TIMEOUT ->
                    {error, timeout}
            end;
        false ->
            not_found
    end.


%% @doc Retrieve attributes from own process
%%
-spec(head(pid()) ->
             any() | not_found).
head(Pid) ->
    case erlang:is_process_alive(Pid) of
        true ->
            Pid ! {self(), head},
            receive
                {ok, Res} ->
                    Res
            after ?DEF_TIMEOUT ->
                    not_found
            end;
        false ->
            not_found
    end.


%%--------------------------------------------------------------------
%% INNNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc destroy own process
%% @private
destroy_fun() ->
    garbage_collect(self()),
    exit(self(), destroy).


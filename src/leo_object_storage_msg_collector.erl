%%======================================================================
%%
%% LeoStorage
%%
%% Copyright (c) 2012-2018 Rakuten, Inc.
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
%% Message Collector (error notification for leo_watchdog)
%%
%% @doc Leo Storage's Message Receiver
%% @reference https://github.com/leo-project/leo_object_storage/blob/master/src/leo_object_storage_msg_collector.erl
%% @end
%%======================================================================
-module(leo_object_storage_msg_collector).

-include_lib("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([init/1,
         clear/0,
         get/0,
         notify/3, notify/4
        ]).

%%====================================================================
%% API
%%====================================================================
%% @doc create an ets table to store messages
%%
-spec(init(Enabled) ->
             ok | {error, any()} when Enabled::boolean()).
init(Enabled) ->
    application:set_env(?APP_NAME, msg_collector, Enabled),
    case Enabled of
        true ->
            ets:new(?ETS_TIMEOUT_MSG_TABLE, [set, public, named_table, {write_concurrency, true}]),
            ets:new(?ETS_SLOWOP_MSG_TABLE, [set, public, named_table, {write_concurrency, true}]),
            ok;
        false ->
            ok
    end.

%% @doc Clear all messages
%%      This is meant to be called from https://github.com/leo-project/leofs/blob/develop/apps/leo_storage/src/leo_storage_watchdog_msgs.erl
-spec(clear() ->
             ok).
clear() ->
    case ?env_enable_msg_collector() of
        true ->
            ets:delete_all_objects(?ETS_TIMEOUT_MSG_TABLE),
            ets:delete_all_objects(?ETS_SLOWOP_MSG_TABLE),
            ok;
        false ->
            ok
    end.

%% @doc Retrieve all messages
%%      This is meant to be called from https://github.com/leo-project/leofs/blob/develop/apps/leo_storage/src/leo_storage_watchdog_msgs.erl
-spec(get() ->
             {ok, Messages} | {error, any()} when Messages::list()).
get() ->
    case ?env_enable_msg_collector() of
        true ->
            TimeoutList = [{M, K} || {_, M, K} <- ets:tab2list(?ETS_TIMEOUT_MSG_TABLE)],
            SlowOpList = [{M, K, P} || {_, M, K, P} <- ets:tab2list(?ETS_SLOWOP_MSG_TABLE)],
            {ok, [{?MSG_ITEM_TIMEOUT, TimeoutList}, {?MSG_ITEM_SLOW_OP, SlowOpList}]};
        false ->
            {ok, []}
    end.

-spec(notify(Msg, Method, Key) ->
             ok | {error, any()} when Msg::atom(),
                                      Method::put|get|delete,
                                      Key::binary()).
notify(?MSG_ITEM_TIMEOUT, Method, Key) ->
    case ?env_enable_msg_collector() of
        true ->
            true = ets:insert(?ETS_TIMEOUT_MSG_TABLE, {erlang:unique_integer(), Method, Key}),
            ok;
        false ->
            ok
    end.

-spec(notify(Msg, Method, Key, ProcessingTime) ->
             ok | {error, any()} when Msg::atom(),
                                      Method::put|get|delete,
                                      Key::binary(),
                                      ProcessingTime::non_neg_integer()).
notify(?MSG_ITEM_SLOW_OP, Method, Key, ProcessingTime) ->
    error_logger:info_msg("~p,~p,~p,~p~n",
                          [{module, ?MODULE_STRING}, {function, "notify/4"},
                           {line, ?LINE}, {body, [{cause, "slow operation"},
                                                  {method, Method},
                                                  {key, Key},
                                                  {processing_time, ProcessingTime}]}]),

    case ?env_enable_msg_collector() of
        true ->
            true = ets:insert(?ETS_SLOWOP_MSG_TABLE, {erlang:unique_integer(), Method, Key, ProcessingTime}),
            ok;
        false ->
            ok
    end.

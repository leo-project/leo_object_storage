%%======================================================================
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
%% ---------------------------------------------------------------------
%% Leo Object Storage
%%
%% @doc The disk space monitor
%% @reference https://github.com/leo-project/leo_object_storage/blob/master/src/leo_object_storage_diskspace_mon.erl
%% @end
%%======================================================================
-module(leo_object_storage_diskspace_mon).
-behaviour(gen_server).

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/2,
         stop/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-record(state, {
          check_intervals = timer:minutes(1) :: pos_integer(),
          avs_path_and_server_pairs = [] :: [{string(), atom()}]
         }).

-define(DEF_TIMEOUT, timer:seconds(30)).


%%====================================================================
%% API
%%====================================================================
%% @doc Starts the server with check intervals (sec)
%%
-spec(start_link(CheckIntervals, AvsPathAndServerPairs) ->
             {ok, pid()} | {error, any()} when CheckIntervals::pos_integer(),
                                               AvsPathAndServerPairs::[{string(), atom()}]).
start_link(CheckIntervals, AvsPathAndServerPairs) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [CheckIntervals, AvsPathAndServerPairs], []).


%% @doc Stop this server
%%
-spec(stop() ->
             ok).
stop() ->
    gen_server:call(?MODULE, stop, ?DEF_TIMEOUT).


%%====================================================================
%% GEN_SERVER CALLBACKS
%%====================================================================
%% @doc Initiates the server
init([CheckIntervals, AvsPathAndServerPairs]) ->
    erlang:send_after(CheckIntervals, self(), trigger),
    {ok, #state{check_intervals = CheckIntervals,
                avs_path_and_server_pairs = AvsPathAndServerPairs}}.


%% @doc gen_server callback - Module:handle_call(Request, From, State) -> Result
handle_call(stop,_From, State) ->
    {stop, shutdown, ok, State};
handle_call(_,_From, State) ->
    {reply, ok, State}.

%% @doc Handling cast message
%% <p>
%% gen_server callback - Module:handle_cast(Request, State) -> Result.
%% </p>
handle_cast(_Msg, State) ->
    {noreply, State}.


%% @doc Handling all non call/cast messages
%% <p>
%% gen_server callback - Module:handle_info(Info, State) -> Result.
%% </p>
handle_info(trigger, #state{check_intervals = CheckIntervals,
                            avs_path_and_server_pairs = AvsPathAndServerPairs} = State) ->
    ok = check_disk_space(AvsPathAndServerPairs),
    erlang:send_after(CheckIntervals, self(), trigger),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc This function is called by a gen_server when it is about to
%%      terminate. It should be the opposite of Module:init/1 and do any necessary
%%      cleaning up. When it returns, the gen_server terminates with Reason.
terminate(_Reason,_State) ->
    ok.

%% @doc Convert process state when code is changed
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%====================================================================
%% Internal Functions
%%====================================================================
check_disk_space([]) ->
    ok;
check_disk_space([{AVSPath, Servers} | Rest]) ->
    case is_freespace_lt_avs(AVSPath) of
        {ok, Ret} ->
            notify_diskspace_status(Servers, Ret);
        {error, Error} ->
            error_logger:error_msg(
              "~p,~p,~p,~p~n",
              [{module, ?MODULE_STRING}, {function, "check_disk_space/2"},
               {line, ?LINE},
               {body, [{simple_reason, "Cannot retrieve a mount path of AVS"},
                       {cause, Error}]}])
    end,
    check_disk_space(Rest).


%% @doc Return {ok, true} if the free disk space is less than(lt) the size of the current AVS,
%%      otherwise {ok, false} or {error, Cause} if some error happened.
%% @private
is_freespace_lt_avs(AVSPath) ->
    case leo_file:file_get_mount_path(AVSPath) of
        {ok, {_MountPath, TotalSize, UsedPercentage}} ->
            FreeSize = TotalSize * ((100 - UsedPercentage) / 100) * 1024,
            AVSSize = filelib:file_size(AVSPath),
            {ok, FreeSize < AVSSize};
        Error ->
            {error, Error}
    end.


%% @private
notify_diskspace_status([],_Ret) ->
    ok;
notify_diskspace_status([Server|Rest], Ret) ->
    Msg = case Ret of
              true ->
                  'error';
              false ->
                  'ok'
          end,
    leo_object_storage_server:update_diskspace_status(Server, Msg),
    notify_diskspace_status(Rest, Ret).

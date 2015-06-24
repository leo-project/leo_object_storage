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
%% Leo Object Storage - Supervisor
%% @doc
%% @reference
%% @end
%%======================================================================
-module(leo_object_storage_sup).

-author('Yosuke Hara').

-behaviour(supervisor).

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, start_link/1, start_link/2,
         stop/0,
         init/1,
         start_child/1, start_child/2]).

-define(DEVICE_ID_INTERVALS, 10000).

%%-----------------------------------------------------------------------
%% API-1
%%-----------------------------------------------------------------------
%% @spec () -> ok
%% @doc start link...
%% @end
-spec(start_link() ->
             {ok, pid()} | {error, any()}).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec(start_link([{pos_integer(), string()}]) ->
             {ok, pid()} | {error, any()}).
start_link(ObjectStorageInfo) ->
    start_link(ObjectStorageInfo, undefined).

-spec(start_link([{pos_integer(), string()}], module()|undefined) ->
             {ok, pid()} | {error, any()}).
start_link(ObjectStorageInfo, CallbackMod) ->
    Res = case whereis(?MODULE) of
              undefined ->
                  supervisor:start_link({local, ?MODULE}, ?MODULE, []);
              Pid ->
                  {ok, Pid}
          end,
    _ = start_child(ObjectStorageInfo, CallbackMod),
    Res.


%% @spec () -> ok |
%%             not_started
%% @doc stop process.
%% @end
-spec(stop() ->
             ok | not_started).
stop() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) == true ->
            List = supervisor:which_children(Pid),
            ok = close_storage(List),
            ok;
        _ ->
            not_started
    end.


%% ---------------------------------------------------------------------
%% Callbacks
%% ---------------------------------------------------------------------
%% @spec (Params) -> ok
%% @doc stop process.
%% @end
%% @private
init([]) ->
    {ok, {{one_for_one, 5, 60}, []}}.


%% ---------------------------------------------------------------------
%% API-2
%% ---------------------------------------------------------------------
-spec(start_child([{pos_integer(), string()}]) ->
             ok | no_return()).
start_child(ObjectStorageInfo) ->
    start_child(ObjectStorageInfo, undefined).

-spec(start_child([{pos_integer(), string()}], CallbackMod) ->
             ok | no_return() when CallbackMod::module()|undefined).
start_child(ObjectStorageInfo, CallbackMod) ->
    %% initialize ets-tables
    ok = leo_misc:init_env(),
    catch ets:new(?ETS_CONTAINERS_TABLE,
                  [named_table, ordered_set, public, {read_concurrency, true}]),
    catch ets:new(?ETS_INFO_TABLE,
                  [named_table, set, public, {read_concurrency, true}]),

    MetadataDB = ?env_metadata_db(),
    IsStrictCheck = ?env_strict_check(),

    BackendDBSupPid = start_child_1(),
    ok = start_child_2(),
    {ok, ServerPairL} = start_child_3(ObjectStorageInfo, 0,
                                     MetadataDB, BackendDBSupPid,
                                     IsStrictCheck, []),
    ok = start_child_4(ServerPairL),
    ok = start_child_5(),
    ok = start_child_6(CallbackMod),
    ok.


%% @doc Launch backend-db's sup
%%      under the leo_object_storage_sup
%% @private
start_child_1() ->
    case whereis(leo_backend_db_sup) of
        undefined ->
            ChildSpec0 = {leo_backend_db_sup,
                          {leo_backend_db_sup, start_link, []},
                          permanent, 2000, worker, [leo_backend_db_sup]},
            case supervisor:start_child(?MODULE, ChildSpec0) of
                {ok, Pid} ->
                    Pid;
                {error, Cause0} ->
                    exit(Cause0)
            end;
        Pid ->
            Pid
    end.

%% @doc Launch the logger
%% @private
start_child_2() ->
    case whereis(leo_logger_sup) of
        undefined ->
            ChildSpec = {leo_logger_sup,
                         {leo_logger_sup, start_link, []},
                         permanent, 2000, supervisor, [leo_logger_sup]},
            case supervisor:start_child(?MODULE, ChildSpec) of
                {ok, _Pid} ->
                    ok;
                {error, Cause} ->
                    exit(Cause)
            end;
        _ ->
            ok
    end.


%% @doc Launch backend-db's processes
%%      under the leo_object_storage_sup
%% @private
start_child_3([],_,_,_,_,Acc) ->
    {ok, Acc};
start_child_3([{NumOfContainers, Path}|Rest], Index,
              MetadataDB, BackendDBSupPid, IsStrictCheck, Acc) ->
    Path_1 = get_path(Path),
    Props  = [{num_of_containers, NumOfContainers},
              {path,              Path_1},
              {metadata_db,       MetadataDB},
              {is_strict_check,   IsStrictCheck}
             ],
    true = ets:insert(?ETS_INFO_TABLE,
                      {list_to_atom(?MODULE_STRING ++ integer_to_list(Index)), Props}),
    {ok, Acc_1} = start_child_3_1(Index, NumOfContainers - 1, BackendDBSupPid, Props, Acc),
    start_child_3(Rest, Index + 1, MetadataDB, BackendDBSupPid, IsStrictCheck, Acc_1).


%% @doc Launch
%% @private
start_child_3_1(_,-1,_,_,Acc) ->
    {ok, Acc};
start_child_3_1(DeviceIndex, ContainerIndex, BackendDBSupPid, Props, Acc) ->
    Id = (DeviceIndex * ?DEVICE_ID_INTERVALS) + ContainerIndex,
    case add_container(BackendDBSupPid, Id, Props) of
        {ok, ServerPair} ->
            start_child_3_1(DeviceIndex, ContainerIndex - 1,
                            BackendDBSupPid, Props, [ServerPair|Acc]);
        {error, Cause} ->
            exit(Cause)
    end.


%% @doc Launch a Compaction manager
%%      under the leo_object_storage_sup
%% @private
start_child_4(ServerPairL) ->
    ChildSpec = {leo_compact_fsm_controller,
                 {leo_compact_fsm_controller, start_link, [ServerPairL]},
                 permanent, 2000, worker, [leo_compact_fsm_controller]},
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok, _Pid} ->
            ok;
        {error, Cause} ->
            exit(Cause)
    end.


%% @doc Check supervisor's status
%% @private
start_child_5() ->
    case whereis(?MODULE) of
        undefined ->
            exit(not_initialized);
        SupRef ->
            Ret = case supervisor:count_children(SupRef) of
                      [_|_] = Props ->
                          Active  = leo_misc:get_value('active',  Props),
                          Workers = leo_misc:get_value('workers', Props),
                          case (Active > 0 andalso Workers > 0) of
                              true ->
                                  ok;
                              false ->
                                  {error, ?ERROR_COULD_NOT_START_WORKER}
                          end;
                      _ ->
                          {error, ?ERROR_COULD_NOT_START_WORKER}
                  end,

            case Ret of
                ok ->
                    ok;
                {error, _Cause} ->
                    case ?MODULE:stop() of
                        ok ->
                            exit(invalid_launch);
                        not_started ->
                            exit(noproc)
                    end
            end
    end.

start_child_6(CallbackMod) ->
    ChildSpec = {leo_object_storage_event_notifier,
                 {leo_object_storage_event_notifier, start_link, [CallbackMod]},
                 permanent, 2000, worker, [leo_object_storage_event_notifier]},
    {ok, _} = supervisor:start_child(leo_object_storage_sup, ChildSpec),
    ok.


%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
%% @doc Terminate children
%% @private
-spec(close_storage(list()) ->
             ok).
close_storage([]) ->
    ok;
close_storage([{Id,_Pid, worker, ['leo_object_storage_server' = Mod|_]}|T]) ->
    _ = Mod:close(Id),
    close_storage(T);
close_storage([_|T]) ->
    close_storage(T).


%% %% @doc Retrieve object-store directory
%% %% @private
-spec(get_path(string()) ->
             string()).
get_path(Path0) ->
    {ok, Curr} = file:get_cwd(),

    Path1 = case Path0 of
                "/"   ++ _Rest -> Path0;
                "../" ++ _Rest -> Path0;
                "./"  ++  Rest -> Curr ++ "/" ++ Rest;
                _              -> Curr ++ "/" ++ Path0
            end,

    Path2 = case (string:len(Path1) == string:rstr(Path1, "/")) of
                true  -> Path1;
                false -> Path1 ++ "/"
            end,
    Path2.


%% @doc Add an object storage container into
%%
-spec(add_container(BackendDBSupPid, Id, Props) ->
             {ok,  ServerPair} |
             {error, any()} when BackendDBSupPid::pid(),
                                 Id::integer(),
                                 Props::[{atom(), any()}],
                                 ServerPair::{atom(), atom()}).
add_container(BackendDBSupPid, Id, Props) ->
    ObjStorageId    = gen_id(obj_storage,     Id),
    MetaDBId        = gen_id(metadata,        Id),
    LoggerId        = gen_id(diagnosis_logger,Id),
    CompactWorkerId = gen_id(compact_worker,  Id),

    %% %% Launch metadata-db
    MetadataDB = leo_misc:get_value('metadata_db', Props),
    Path       = leo_misc:get_value('path', Props),
    ok = leo_backend_db_sup:start_child(
           BackendDBSupPid, MetaDBId, 1, MetadataDB,
           lists:append([Path, ?DEF_METADATA_STORAGE_SUB_DIR, integer_to_list(Id)])),

    %% %% Launch compact_fsm_worker
    Ret = case add_container_1(leo_compact_fsm_worker,
                               CompactWorkerId, ObjStorageId, MetaDBId, LoggerId) of
              ok ->
                  %% Launch object-storage
                  add_container_1(leo_object_storage_server, Id, ObjStorageId,
                                  MetaDBId, CompactWorkerId, LoggerId, Props);
              {error,{already_started,_Pid}} ->
                  add_container_1(leo_object_storage_server, Id, ObjStorageId,
                                  MetaDBId, CompactWorkerId, LoggerId, Props);
              {error, Cause} ->
                  {error, Cause}
          end,
    case Ret of
        ok ->
            {ok, {CompactWorkerId, ObjStorageId}};
        Other ->
            Other
    end.

%% @private
add_container_1(leo_compact_fsm_worker = Mod,
                Id, ObjStorageId, MetaDBId, LoggerId) ->
    ChildSpec = {Id,
                 {Mod, start_link,
                  [Id, ObjStorageId, MetaDBId, LoggerId]},
                 permanent, 2000, worker, [Mod]},
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok,_} ->
            ok;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "add_container_1/4"},
                                    {line, ?LINE},
                                    {body, Cause}]),
            {error, Cause}
    end.

add_container_1(leo_object_storage_server = Mod, BaseId,
                ObjStorageId, MetaDBId, CompactWorkerId, LoggerId, Props) ->
    Path          = leo_misc:get_value('path',            Props),
    IsStrictCheck = leo_misc:get_value('is_strict_check', Props),

    Args = [ObjStorageId, BaseId,
            MetaDBId, CompactWorkerId, LoggerId, Path, IsStrictCheck],
    ChildSpec = {ObjStorageId,
                 {Mod, start_link, Args},
                 permanent, 2000, worker, [Mod]},

    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok,_} ->
            true = ets:insert(?ETS_CONTAINERS_TABLE, {BaseId, [{obj_storage,    ObjStorageId},
                                                               {metadata,       MetaDBId},
                                                               {compact_worker, CompactWorkerId}]}),
            ok = leo_misc:set_env(?APP_NAME, {?ENV_COMPACTION_STATUS, ObjStorageId}, ?STATE_ACTIVE),
            ok;
        {error,{already_started,_Pid}} ->
            ok;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "add_container/3"},
                                    {line, ?LINE},
                                    {body, Cause}]),
            {error, Cause}
    end.


%% @doc Generate Id for obj-storage or metadata
%%
-spec(gen_id(obj_storage | metadata | diagnosis_logger | compact_worker, integer()) ->
             atom()).
gen_id(obj_storage, Id) ->
    list_to_atom(lists:append([atom_to_list(?APP_NAME),
                               "_",
                               integer_to_list(Id)]));
gen_id(metadata, Id) ->
    list_to_atom(lists:append(["leo_metadata_",
                               integer_to_list(Id)]));
gen_id(diagnosis_logger, Id) ->
    list_to_atom(lists:append(["leo_diagnosis_log_",
                               integer_to_list(Id)]));
gen_id(compact_worker, Id) ->
    list_to_atom(lists:append(["leo_compact_worker_",
                               integer_to_list(Id)])).

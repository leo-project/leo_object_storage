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
%% Leo Object Storage - Supervisor
%% @doc
%% @end
%%======================================================================
-module(leo_object_storage_sup).

-author('Yosuke Hara').

-behaviour(supervisor).

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, start_link/1,
         stop/0,
         init/1,
         start_child/1]).

-define(DEVICE_ID_INTERVALS, 10000).

%%-----------------------------------------------------------------------
%% API-1
%%-----------------------------------------------------------------------
%% @spec () -> ok
%% @doc start link...
%% @end
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_link(ObjectStorageInfo) ->
    Res = case whereis(?MODULE) of
              undefined ->
                  supervisor:start_link({local, ?MODULE}, ?MODULE, []);
              Pid ->
                  {ok, Pid}
          end,
    _ = start_child(ObjectStorageInfo),
    Res.


%% @spec () -> ok |
%%             not_started
%% @doc stop process.
%% @end
stop() ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) == true ->
            List = supervisor:which_children(Pid),
            Len  = length(List),

            ok = terminate_children(List),
            timer:sleep(Len * 100),
            exit(Pid, shutdown),
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
-spec(start_child(list(tuple())) ->
             ok | true).
start_child(ObjectStorageInfo) ->
    %% initialize ets-tables
    ok = leo_misc:init_env(),
    catch ets:new(?ETS_CONTAINERS_TABLE,
                  [named_table, ordered_set, public, {read_concurrency, true}]),
    catch ets:new(?ETS_INFO_TABLE,
                  [named_table, set, public, {read_concurrency, true}]),

    %% Launch backend-db's sup
    %%   under the leo_object_storage_sup
    ChildSpec0 = {leo_backend_db_sup,
                  {leo_backend_db_sup, start_link, []},
                  permanent, 2000, worker, [leo_backend_db_sup]},
    BackendDBSupPid =
        case supervisor:start_child(?MODULE, ChildSpec0) of
            {ok, Pid} ->
                Pid;
            {error, Cause0} ->
                error_logger:error_msg("~p,~p,~p,~p~n",
                                       [{module, ?MODULE_STRING}, {function, "start_child/2"},
                                        {line, ?LINE}, {body, "Could NOT start backend-db sup"}]),
                exit(Cause0)
        end,

    %% Launch backend-db's processes
    %%   under the leo_object_storage_sup
    MetadataDB = ?env_metadata_db(),
    _ = lists:foldl(
          fun({Containers, Path0}, I) ->
                  Path1 = get_path(Path0),
                  Props = [{num_of_containers, Containers},
                           {path,              Path1},
                           {metadata_db,       MetadataDB}],

                  true = ets:insert(?ETS_INFO_TABLE,
                                    {list_to_atom(?MODULE_STRING ++ integer_to_list(I)), Props}),
                  ok = lists:foreach(fun(N) ->
                                             Id = (I * ?DEVICE_ID_INTERVALS) + N,
                                             ok = add_container(BackendDBSupPid, Id, Props)
                                     end, lists:seq(0, Containers-1)),
                  I + 1
          end, 0, ObjectStorageInfo),

    %% Launch a Compaction manager
    %%   under the leo_object_storage_sup
    ChildSpec1 = {leo_compaction_manager_fsm,
                  {leo_compaction_manager_fsm, start_link, []},
                  permanent, 2000, worker, [leo_compaction_manager_fsm]},
    case supervisor:start_child(?MODULE, ChildSpec1) of
        {ok, _Pid} ->
            void;
        {error, Cause1} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "start_child/2"},
                                    {line, ?LINE}, {body, "Could NOT start compaction manager process"}]),
            exit(Cause1)
    end,


    %% Check supervisor's status
    case whereis(?MODULE) of
        undefined ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "start_child/2"},
                                    {line, ?LINE}, {body, "NOT started supervisor"}]),
            exit(not_initialized);
        SupRef ->
            case supervisor:count_children(SupRef) of
                [{specs, _},{active, Active},
                 {supervisors, _},{workers, Workers}] when Active == Workers  ->
                    ok;
                _ ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "start_child/2"},
                                            {line, ?LINE}, {body, "Could NOT start worker processes"}]),
                    case ?MODULE:stop() of
                        ok ->
                            exit(invalid_launch);
                        not_started ->
                            exit(noproc)
                    end
            end
    end.


%% ---------------------------------------------------------------------
%% Inner Function(s)
%% ---------------------------------------------------------------------
%% @doc Terminate children
%% @private
-spec(terminate_children(list()) ->
             ok).
terminate_children([]) ->
    ok;
terminate_children([{Id,_Pid, worker, [Mod|_]}|T]) ->
    case Mod of
        leo_backend_db_sup ->
            Mod:stop();
        _ ->
            Mod:stop(Id)
    end,
    terminate_children(T);
terminate_children([_|T]) ->
    terminate_children(T).


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
-spec(add_container(pid(), integer(), list()) ->
             ok).
add_container(BackendDBSupPid, Id0, Props) ->
    Id1 = gen_id(obj_storage, Id0),
    Id2 = gen_id(metadata,    Id0),

    Path       = leo_misc:get_value('path',        Props),
    MetadataDB = leo_misc:get_value('metadata_db', Props),

    %% %% Launch metadata-db
    case leo_backend_db_sup:start_child(
           BackendDBSupPid, Id2, 1, MetadataDB,
           lists:append([Path, ?DEF_METADATA_STORAGE_SUB_DIR, integer_to_list(Id0)])) of
        ok ->
            %% Launch object-storage
            Args = [Id1, Id0, Id2, Path],
            ChildSpec = {Id1,
                         {leo_object_storage_server, start_link, Args},
                         permanent, 2000, worker, [leo_object_storage_server]},

            case supervisor:start_child(?MODULE, ChildSpec) of
                {ok, _Pid} ->
                    true = ets:insert(?ETS_CONTAINERS_TABLE, {Id0, [{obj_storage, Id1},
                                                                    {metadata,    Id2}]}),

                    ok = leo_misc:set_env(?APP_NAME, {?ENV_COMPACTION_STATUS, Id1}, ?STATE_ACTIVE),
                    ok;
                {error, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "add_container/3"},
                                            {line, ?LINE},
                                            {body, Cause}]),
                    {error, Cause}
            end;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "add_container/3"},
                                    {line, ?LINE},
                                    {body, Cause}]),
            {error, Cause}
    end.


%% @doc Generate Id for obj-storage or metadata
%%
-spec(gen_id(obj_storage | metadata, integer()) ->
             atom()).
gen_id(obj_storage, Id) ->
    list_to_atom(lists:append([atom_to_list(?APP_NAME),
                               "_",
                               integer_to_list(Id)]));
gen_id(metadata, Id) ->
    list_to_atom(lists:append(["leo_metadata",
                               "_",
                               integer_to_list(Id)])).


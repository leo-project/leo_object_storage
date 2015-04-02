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
%%
%%======================================================================
-define(APP_NAME, 'leo_object_storage').

%% Default Values
-define(AVS_FILE_EXT, ".avs").
-define(DEF_METADATA_DB,              'bitcask').
-define(DEF_OBJECT_STORAGE_SUB_DIR,   "object/").
-define(DEF_METADATA_STORAGE_SUB_DIR, "metadata/").
-define(DEF_STATE_SUB_DIR,            "state/").

-define(SERVER_OBJ_STORAGE, 'object_storage').
-define(SERVER_METADATA_DB, 'metadata_db').

%% ETS-Table
-define(ETS_CONTAINERS_TABLE, 'leo_object_storage_containers').
-define(ETS_INFO_TABLE,       'leo_object_storage_info').

%% regarding compaction
-define(ENV_COMPACTION_STATUS,    'compaction_status').
-define(STATE_RUNNING_COMPACTION, 'compacting').
-define(STATE_ACTIVE,             'active').
-type(storage_status() :: ?STATE_RUNNING_COMPACTION | ?STATE_ACTIVE).

-define(DEF_LIMIT_COMPACTION_PROCS, 4).

-ifdef(TEST).
-define(DEF_MIN_COMPACTION_WT,  0).    %% 0msec
-define(DEF_REG_COMPACTION_WT,  100).  %% 100msec
-define(DEF_MAX_COMPACTION_WT,  300).  %% 300msec
-define(DEF_STEP_COMPACTION_WT, 100).  %% 100msec
-else.
-define(DEF_MIN_COMPACTION_WT,  100).  %% 100msec
-define(DEF_REG_COMPACTION_WT,  300).  %% 300msec
-define(DEF_MAX_COMPACTION_WT,  1000). %% 1000msec
-define(DEF_STEP_COMPACTION_WT, 100).  %% 100msec
-endif.

-ifdef(TEST).
-define(DEF_MIN_COMPACTION_BP,   10). %% 10
-define(DEF_REG_COMPACTION_BP,   50). %% 50
-define(DEF_MAX_COMPACTION_BP,  150). %% 150
-define(DEF_STEP_COMPACTION_BP,  50). %% 100
-else.
-define(DEF_MIN_COMPACTION_BP,    1000). %%   1,000
-define(DEF_REG_COMPACTION_BP,   10000). %%  10,000
-define(DEF_MAX_COMPACTION_BP,  100000). %% 100,000
-define(DEF_STEP_COMPACTION_BP,   1000). %%   1,000
-endif.


-undef(ST_IDLING).
-undef(ST_RUNNING).
-undef(ST_SUSPENDING).
-define(ST_IDLING,     'idling').
-define(ST_RUNNING,    'running').
-define(ST_SUSPENDING, 'suspending').
-type(compaction_state() :: ?ST_IDLING     |
                            ?ST_RUNNING    |
                            ?ST_SUSPENDING).


-undef(EVENT_RUN).
-undef(EVENT_DIAGNOSE).
-undef(EVENT_LOCK).
-undef(EVENT_SUSPEND).
-undef(EVENT_RESUME).
-undef(EVENT_FINISH).
-undef(EVENT_STATE).
-undef(EVENT_INCR_WT).
-undef(EVENT_DECR_WT).
-undef(EVENT_INCR_BP).
-undef(EVENT_DECR_BP).
-define(EVENT_RUN,      'run').
-define(EVENT_DIAGNOSE, 'diagnose').
-define(EVENT_LOCK,     'lock').
-define(EVENT_SUSPEND,  'suspend').
-define(EVENT_RESUME,   'resume').
-define(EVENT_FINISH,   'finish').
-define(EVENT_STATE,    'state').
-define(EVENT_INCR_WT,  'incr_interval').
-define(EVENT_DECR_WT,  'decr_interval').
-define(EVENT_INCR_BP,  'incr_batch_of_msgs').
-define(EVENT_DECR_BP,  'decr_batch_of_msgs').
-type(compaction_event() ::?EVENT_RUN      |
                           ?EVENT_DIAGNOSE |
                           ?EVENT_LOCK     |
                           ?EVENT_SUSPEND  |
                           ?EVENT_RESUME   |
                           ?EVENT_FINISH   |
                           ?EVENT_STATE    |
                           ?EVENT_INCR_WT  |
                           ?EVENT_DECR_WT  |
                           ?EVENT_INCR_BP  |
                           ?EVENT_DECR_BP
                           ).

%% @doc Compaction related definitions
-define(RET_SUCCESS, 'success').
-define(RET_FAIL,    'fail').
-type(compaction_ret() :: ?RET_SUCCESS |
                          ?RET_FAIL |
                          undefined).

-define(MAX_LEN_HIST, 50).

-record(compaction_report, {
          file_path = [] :: string(),
          avs_ver = <<>> :: binary(),
          num_of_active_objs  = 0 :: non_neg_integer(),
          size_of_active_objs = 0 :: non_neg_integer(),
          total_num_of_objs   = 0 :: non_neg_integer(),
          total_size_of_objs  = 0 :: non_neg_integer(),
          start_datetime = [] :: string(),
          end_datetime   = [] :: string(),
          errors = []  :: [{non_neg_integer(),non_neg_integer()}],
          duration = 0 :: non_neg_integer(),
          result :: atom()
         }).

-record(compaction_hist, {
          start_datetime = 0 :: non_neg_integer(),
          end_datetime   = 0 :: non_neg_integer(),
          duration = 0 :: non_neg_integer(),
          result :: compaction_ret()
         }).

-record(compaction_stats, {
          status = ?ST_IDLING :: compaction_state(),
          total_num_of_targets    = 0  :: non_neg_integer(),
          num_of_reserved_targets = 0  :: non_neg_integer(),
          num_of_pending_targets  = 0  :: non_neg_integer(),
          num_of_ongoing_targets  = 0  :: non_neg_integer(),
          reserved_targets = []        :: [atom()],
          pending_targets  = []        :: [atom()],
          ongoing_targets  = []        :: [atom()],
          locked_targets   = []        :: [atom()],
          latest_exec_datetime = 0     :: non_neg_integer(),
          acc_reports = []             :: [#compaction_report{}]
         }).

%% Error Constants
%%
-define(ERROR_FD_CLOSED,                "already closed file-descriptor").
-define(ERROR_FILE_OPEN,                "file open error").
-define(ERROR_INVALID_DATA,             "invalid data").
-define(ERROR_DATA_SIZE_DID_NOT_MATCH,  "data-size did not match").
-define(ERROR_COMPACT_SUSPEND_FAILURE,  "comaction-suspend filure").
-define(ERROR_COMPACT_RESUME_FAILURE,   "comaction-resume filure").
-define(ERROR_PROCESS_NOT_FOUND,        "server process not found").
-define(ERROR_COULD_NOT_GET_MOUNT_PATH, "could not get mout path").
-define(ERROR_LOCKED_CONTAINER,         "locked obj-conatainer").
-define(ERROR_COULD_NOT_START_WORKER,   "could NOT start worker processes").

-define(DEL_TRUE,  1).
-define(DEL_FALSE, 0).

-type(del_flag() :: ?DEL_TRUE | ?DEL_FALSE).
-type(type_of_method() :: get | put | delete | head | head_with_calc_md5 | store).

-define(MD5_EMPTY_BIN, 281949768489412648962353822266799178366).
-define(MAX_KEY_SIZE,  1024 * 4).

%%--------------------------------------------------------------------
%% AVS-Related
%%--------------------------------------------------------------------
%% AVS version strings
-define(AVS_HEADER_VSN_2_2,  <<"LeoFS AVS-2.2">>). %% leofs v0.14 - v1.0.0-pre1
-define(AVS_HEADER_VSN_2_4,  <<"LeoFS AVS-2.4">>). %% leofs v1.0.0-pre1 - current ver
-define(AVS_HEADER_VSN_TOBE, ?AVS_HEADER_VSN_2_4).

%% Max Data Block Size to be larger than leo_gateway's large object settings
-define(MAX_DATABLOCK_SIZE, 1024 * 1024 * 10).

%% @doc Generate an key for backend db
-define(gen_backend_key(_VSN, _AddrId, _Key),
        begin
            case _VSN of
                ?AVS_HEADER_VSN_2_2 ->
                    term_to_binary({_AddrId, _Key});
                ?AVS_HEADER_VSN_2_4 ->
                    _Key
            end
        end).

-define(AVS_HEADER_VSN,     <<?AVS_HEADER_VSN_TOBE/binary,13,10>>).
-define(AVS_PART_OF_HEADER, <<"CHKSUM:128,KSIZE:16,BLEN_MSIZE:32,DSIZE:32,OFFSET:64,ADDRID:128,CLOCK:64,TIMESTAMP:42,DEL:1,BUF:437,CHUNK_SIZE:32,CHUNK_NUM:24,CHUNK_INDEX:24",13,10>>).
-define(AVS_PART_OF_BODY,   <<"KEY/binary,DATA/binary",13,10>>).
-define(AVS_PART_OF_FOOTER, <<"PADDING:64",13,10>>).
-define(AVS_SUPER_BLOCK,    <<?AVS_HEADER_VSN/binary,
                              ?AVS_PART_OF_HEADER/binary,
                              ?AVS_PART_OF_BODY/binary,
                              ?AVS_PART_OF_FOOTER/binary>>).
-define(AVS_SUPER_BLOCK_LEN, byte_size(?AVS_SUPER_BLOCK)).

%% ------------------------ %%
-define(BLEN_CHKSUM,       128). %% chechsum (MD5)
-define(BLEN_KSIZE,         16). %% key size
-define(BLEN_MSIZE,         32). %% custome-metadata size
-define(BLEN_DSIZE,         32). %% file size
-define(BLEN_OFFSET,        64). %% offset
-define(BLEN_ADDRID,       128). %% ring-address id
-define(BLEN_CLOCK,         64). %% clock
-define(BLEN_TS_Y,          12). %% timestamp-year
-define(BLEN_TS_M,           6). %% timestamp-month
-define(BLEN_TS_D,           6). %% timestamp-day
-define(BLEN_TS_H,           6). %% timestamp-hour
-define(BLEN_TS_N,           6). %% timestamp-min
-define(BLEN_TS_S,           6). %% timestamp-sec
-define(BLEN_DEL,            1). %% delete flag
-define(BLEN_CHUNK_SIZE,    32). %% * chunked data size    (for large-object)
-define(BLEN_CHUNK_NUM,     24). %% * # of chunked objects (for large-object)
-define(BLEN_CHUNK_INDEX,   24). %% * chunked object index (for large-object)
%% ----------------------------- %%
-define(BLEN_BUF,          437). %% buffer
%% ----------------------------- %%
-define(BLEN_HEADER,      1024). %% 128 Byte
-define(LEN_PADDING,         8). %% footer

-type(addrid_and_key() :: {non_neg_integer(), binary()}).

%%--------------------------------------------------------------------
%% Records
%%--------------------------------------------------------------------
-record(backend_info, {
          backend            :: atom(),
          avs_ver_cur = <<>> :: binary(),
          avs_ver_prv = <<>> :: binary(), %% need to know during compaction
          linked_path = []   :: string(),
          file_path   = []   :: string(),
          write_handler      :: pid()|undefined,
          read_handler       :: pid()|undefined
         }).

-record(metadata, { %% - leofs-v1.0.0-pre3
          key = <<>>          :: binary(),  %% filename
          addr_id    = 0      :: integer(), %% ring-address id (MD5 > hex-to-integer)
          ksize      = 0      :: integer(), %% file-path size
          dsize      = 0      :: integer(), %% data size
          msize      = 0      :: integer(), %% custom-metadata size

          csize      = 0      :: integer(), %% * chunked data size    (for large-object)
          cnumber    = 0      :: integer(), %% * # of chunked objects (for large-object)
          cindex     = 0      :: integer(), %% * chunked object index (for large-object)

          offset     = 0      :: integer(), %% object-container's offset
          clock      = 0      :: integer(), %% clock
          timestamp  = 0      :: integer(), %% timestamp
          checksum   = 0      :: integer(), %% checksum (MD5 > hex-to-integer)
          ring_hash  = 0      :: integer(), %% RING's Hash(CRC32) when write an object.
          del = ?DEL_FALSE    :: del_flag() %% [{0,not_deleted}, {1,deleted}]
         }).

-record(metadata_1, { %% leofs-v1.0.0 - current ver
          key = <<>>          :: binary(),  %% filename
          addr_id    = 0      :: integer(), %% ring-address id (MD5 > hex-to-integer)
          ksize      = 0      :: integer(), %% file-path size
          dsize      = 0      :: integer(), %% data size
          msize      = 0      :: integer(), %% custom-metadata size

          csize      = 0      :: integer(), %% * chunked data size    (for large-object)
          cnumber    = 0      :: integer(), %% * # of chunked objects (for large-object)
          cindex     = 0      :: integer(), %% * chunked object index (for large-object)

          offset     = 0      :: integer(), %% object-container's offset
          clock      = 0      :: integer(), %% clock
          timestamp  = 0      :: integer(), %% timestamp
          checksum   = 0      :: integer(), %% checksum (MD5 > hex-to-integer)
          ring_hash  = 0      :: integer(), %% RING's Hash(CRC32) when write an object.

          cluster_id          :: atom(),    %% cluster-id for the mdc-replication
          num_of_replicas = 0 :: integer(), %% # of replicas for the mdc-replication
          ver = 0             :: integer(), %% version number

          del = ?DEL_FALSE    :: del_flag() %% [{0,not_deleted}, {1,deleted}]
         }).
-define(METADATA, 'metadata_1').

-record(object, { %% - leofs-v1.0.0-pre3
          method,
          key        = <<>>   :: binary(),  %% filename
          addr_id    = 0      :: integer(), %% ring-address id (MD5 > hex-to-integer)
          data       = <<>>   :: binary(),  %% file
          meta       = <<>>   :: binary(),  %% custom-metadata
          ksize      = 0      :: integer(), %% filename size
          dsize      = 0      :: integer(), %% data size
          msize      = 0      :: integer(), %% custom-metadata size

          csize      = 0      :: integer(), %% * chunked data size    (for large-object)
          cnumber    = 0      :: integer(), %% * # of chunked objects (for large-object)
          cindex     = 0      :: integer(), %% * chunked object index (for large-object)

          offset     = 0      :: integer(), %% object-container's offset
          clock      = 0      :: integer(), %% clock
          timestamp  = 0      :: integer(), %% timestamp
          checksum   = 0      :: integer(), %% checksum (MD5 > hex-to-integer)
          ring_hash  = 0      :: integer(), %% RING's Hash(CRC32) when write an object.
          req_id     = 0      :: integer(), %% request id
          del = ?DEL_FALSE    :: del_flag() %% delete flag
         }).

-record(object_1, { %% leofs-v1.0.0 - current ver
          method,
          key        = <<>>   :: binary(),  %% filename
          addr_id    = 0      :: integer(), %% ring-address id (MD5 > hex-to-integer)
          data       = <<>>   :: binary(),  %% file
          meta       = <<>>   :: binary(),  %% custom-metadata
          ksize      = 0      :: integer(), %% filename size
          dsize      = 0      :: integer(),         %% data size
          msize      = 0      :: integer(), %% custom-metadata size

          csize      = 0      :: integer(), %% * chunked data size    (for large-object)
          cnumber    = 0      :: integer(), %% * # of chunked objects (for large-object)
          cindex     = 0      :: integer(), %% * chunked object index (for large-object)

          offset     = 0      :: integer(), %% object-container's offset
          clock      = 0      :: integer(), %% clock
          timestamp  = 0      :: integer(), %% timestamp
          checksum   = 0      :: integer(), %% checksum (MD5 > hex-to-integer)
          ring_hash  = 0      :: integer(), %% RING's Hash(CRC32) when write an object.
          req_id     = 0      :: integer(), %% request id

          cluster_id          :: atom(),    %% cluster-id for the mdc-replication
          num_of_replicas = 0 :: integer(), %% # of replicas for the mdc-replication
          ver = 0             :: integer(), %% version number
          del = ?DEL_FALSE    :: del_flag() %% delete flag
         }).
-define(OBJECT, 'object_1').

-record(storage_stats, {
          file_path       = [] :: string(),
          total_sizes     = 0  :: non_neg_integer(),
          active_sizes    = 0  :: non_neg_integer(),
          total_num       = 0  :: non_neg_integer(),
          active_num      = 0  :: non_neg_integer(),
          compaction_hist = [] :: [#compaction_hist{}]
         }).

%% apllication-env
-define(env_metadata_db(),
        case application:get_env(?APP_NAME, metadata_storage) of
            {ok, EnvMetadataDB} -> EnvMetadataDB;
            _ -> ?DEF_METADATA_DB
        end).

-ifdef(TEST).
-define(env_strict_check(), true).
-else.
-define(env_strict_check(),
        case application:get_env(?APP_NAME, strict_check) of
            {ok, EnvStrictCheck} -> EnvStrictCheck;
            _ -> false
        end).
-endif.

-define(env_enable_diagnosis_log(),
        case application:get_env(leo_object_storage,
                                 is_enable_diagnosis_log) of
            {ok, true} ->
                true;
            _ ->
                false
        end).

-define(env_limit_num_of_compaction_procs(),
        case application:get_env(leo_object_storage,
                                 limit_num_of_compaction_procs) of
            {ok, EnvLimitCompactionProcs} when is_integer(EnvLimitCompactionProcs) ->
                EnvLimitCompactionProcs;
            _ ->
                ?DEF_LIMIT_COMPACTION_PROCS
        end).

%% [Interval between batch processes]
-define(env_compaction_interval_between_batch_procs_min(),
        case application:get_env(leo_object_storage,
                                 compaction_waiting_time_min) of
            {ok, EnvMinCompactionWT} when is_integer(EnvMinCompactionWT) ->
                EnvMinCompactionWT;
            _ ->
                ?DEF_MIN_COMPACTION_WT
        end).

-define(env_compaction_interval_between_batch_procs_reg(),
        case application:get_env(leo_object_storage,
                                 compaction_waiting_time_regular) of
            {ok, EnvRegCompactionWT} when is_integer(EnvRegCompactionWT) ->
                EnvRegCompactionWT;
            _ ->
                ?DEF_REG_COMPACTION_WT
        end).

-define(env_compaction_interval_between_batch_procs_max(),
        case application:get_env(leo_object_storage,
                                 compaction_waiting_time_max) of
            {ok, EnvMaxCompactionWT} when is_integer(EnvMaxCompactionWT) ->
                EnvMaxCompactionWT;
            _ ->
                ?DEF_MAX_COMPACTION_WT
        end).

-define(env_compaction_interval_between_batch_procs_step(),
        case application:get_env(leo_object_storage,
                                 compaction_waiting_time_step) of
            {ok, EnvStepCompactionWT} when is_integer(EnvStepCompactionWT) ->
                EnvStepCompactionWT;
            _ ->
                ?DEF_STEP_COMPACTION_WT
        end).

%% [Number of batch processes]
-define(env_compaction_num_of_batch_procs_max(),
        case application:get_env(leo_object_storage, batch_procs_max) of
            {ok, EnvMaxCompactionBP} when is_integer(EnvMaxCompactionBP) ->
                EnvMaxCompactionBP;
            _ ->
                ?DEF_MAX_COMPACTION_BP
        end).

-define(env_compaction_num_of_batch_procs_reg(),
        case application:get_env(leo_object_storage, batch_procs_regular) of
            {ok, EnvRegCompactionBP} when is_integer(EnvRegCompactionBP) ->
                EnvRegCompactionBP;
            _ ->
                ?DEF_REG_COMPACTION_BP
        end).

-define(env_compaction_num_of_batch_procs_min(),
        case application:get_env(leo_object_storage, batch_procs_min) of
            {ok, EnvMinCompactionBP} when is_integer(EnvMinCompactionBP) ->
                EnvMinCompactionBP;
            _ ->
                ?DEF_MIN_COMPACTION_BP
        end).
-define(env_compaction_num_of_batch_procs_step(),
        case application:get_env(leo_object_storage, batch_procs_step) of
            {ok, EnvStepCompactionBP} when is_integer(EnvStepCompactionBP) ->
                EnvStepCompactionBP;
            _ ->
                ?DEF_STEP_COMPACTION_BP
        end).


-define(num_of_compaction_concurrency(_PrmNumOfConcurrency),
        begin
            _EnvNumOfConcurrency = ?env_limit_num_of_compaction_procs(),
            case (_PrmNumOfConcurrency > _EnvNumOfConcurrency) of
                true  -> _EnvNumOfConcurrency;
                false -> _PrmNumOfConcurrency
            end
        end).

-define(get_object_storage_id(_TargetContainers),
        begin
            lists:flatten(
              lists:map(
                fun(_Id) ->
                        case leo_object_storage_api:get_object_storage_pid_by_container_id(_Id) of
                            not_found -> [];
                            _Pid -> _Pid
                        end
                end,_TargetContainers))
        end).

%% custom-metadata's items for MDC-replication:
-define(PROP_CMETA_CLUSTER_ID, 'cluster_id').
-define(PROP_CMETA_NUM_OF_REPLICAS, 'num_of_replicas').
-define(PROP_CMETA_VER, 'ver').


%% @doc Generate a raw file path
-define(gen_raw_file_path(_FilePath),
        lists:append([_FilePath, "_", integer_to_list(leo_date:now())])).

%% @doc Retrieve object-storage info
-define(get_obj_storage_info(_ObjStorageId),
        leo_object_storage_server:get_backend_info(_ObjStorageId, ?SERVER_OBJ_STORAGE)).


%% @doc Diagnosis log-related definitions
-define(DEF_LOG_SUB_DIR,        "log/").
-define(LOG_GROUP_ID_DIAGNOSIS, 'log_diagnosis_grp').
-define(LOG_ID_DIAGNOSIS,       'log_diagnosis').
-define(LOG_FILENAME_DIAGNOSIS, "leo_object_storage_").
-define(DIAGNOSIS_REP_SUFFIX,   ".report").


%% @doc Output diagnosis log
-define(output_diagnosis_log(Metadata),
        begin
            #?METADATA{offset = _Offset,
                       addr_id = _AddrId,
                       key = _Key,
                       dsize = _Dsize,
                       clock = _Clock,
                       timestamp = _Timestamp,
                       del = _Del
                      } = Metadata,
            _Path_1 = binary_to_list(_Key),

            {_Path_2, _CIndex} =
                begin
                    _Tokens = string:tokens(_Path_1, "\n"),
                    case length(_Tokens) of
                        1 ->
                            {_Path_1, 0};
                        _ ->
                            [_ParentPath,_CNum|_] = _Tokens,
                            case catch list_to_integer(_CNum) of
                                {'EXIT',_} ->
                                    {_ParentPath, 0};
                                _Num->
                                    {_ParentPath, _Num}
                            end
                    end
                end,

            leo_logger_client_base:append(
              {LoggerId, #message_log{format  = "~w\t~w\t~s\t~w\t~w\t~w\t~s\t~w~n",
                                      message = [_Offset,
                                                 _AddrId,
                                                 _Path_2,
                                                 _CIndex,
                                                 _Dsize,
                                                 _Clock,
                                                 leo_date:date_format(_Timestamp),
                                                 _Del
                                                ]}
              })
        end).

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
%%======================================================================
-compile(nowarn_deprecated_type).
-define(APP_NAME, 'leo_object_storage').

-ifdef(namespaced_types).
-type otp_set() :: sets:set().
-else.
-type otp_set() :: set().
-endif.

%% Default Values
-define(AVS_FILE_EXT, ".avs").
-define(DEF_METADATA_DB, 'bitcask').
-define(DEF_OBJECT_STORAGE_SUB_DIR, "object/").
-define(DEF_METADATA_STORAGE_SUB_DIR, "metadata/").
-define(DEF_STATE_SUB_DIR, "state/").

-define(SERVER_OBJ_STORAGE, 'object_storage').
-define(SERVER_METADATA_DB, 'metadata_db').

-define(DEF_NUM_OF_OBJ_STORAGE_READ_PROCS, 3).

%% ETS-Table
-define(ETS_CONTAINERS_BY_DISK_TABLE, "leo_object_storage_containers_by_disk_").
-define(ETS_CONTAINERS_TABLE, 'leo_object_storage_containers').
-define(ETS_INFO_TABLE, 'leo_object_storage_info').
-define(ETS_TIMEOUT_MSG_TABLE, 'leo_object_storage_timeout_msg').
-define(ETS_SLOWOP_MSG_TABLE, 'leo_object_storage_slowop_msg').

%% regarding compaction
-define(ENV_COMPACTION_STATUS, 'compaction_status').
-define(STATE_RUNNING_COMPACTION, 'compacting').
-define(STATE_ACTIVE, 'active').
-type(storage_status() :: ?STATE_RUNNING_COMPACTION | ?STATE_ACTIVE).

-define(DEF_LIMIT_COMPACTION_PROCS, 4).
-define(DEF_THRESHOLD_SLOW_PROC, 1000).

-ifdef(TEST).
-define(DEF_MIN_COMPACTION_WT, 10).   %% 10msec
-define(DEF_REG_COMPACTION_WT, 100).  %% 100msec
-define(DEF_MAX_COMPACTION_WT, 300).  %% 300msec
-define(DEF_COMPACTION_TIMEOUT, timer:minutes(1)). %% 1min
-else.
-define(DEF_MIN_COMPACTION_WT,  300). %% 300msec
-define(DEF_REG_COMPACTION_WT,  500). %% 500msec
-define(DEF_MAX_COMPACTION_WT, 3000). %% 3sec
-define(DEF_COMPACTION_TIMEOUT, timer:minutes(1)). %% 1min
-endif.

-ifdef(TEST).
-define(DEF_MIN_COMPACTION_BP,  10). %% 10
-define(DEF_REG_COMPACTION_BP,  50). %% 50
-define(DEF_MAX_COMPACTION_BP, 150). %% 150
-else.
-define(DEF_MIN_COMPACTION_BP,   100). %%    100
-define(DEF_REG_COMPACTION_BP,  1000). %%  1,000
-define(DEF_MAX_COMPACTION_BP, 10000). %% 10,000
-endif.

-define(DEF_COMPACTION_SKIP_PS, 512).
-define(DEF_COMPACTION_FQ_IN_BYTES, 10485760). %% 10MB = 1024 * 1024 * 10

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
-undef(EVENT_INCREASE).
-undef(EVENT_DECREASE).
-define(EVENT_RUN,      'run').
-define(EVENT_DIAGNOSE, 'diagnose').
-define(EVENT_LOCK,     'lock').
-define(EVENT_SUSPEND,  'suspend').
-define(EVENT_RESUME,   'resume').
-define(EVENT_FINISH,   'finish').
-define(EVENT_STATE,    'state').
-define(EVENT_INCREASE,  'increase').
-define(EVENT_DECREASE,  'decrease').
-type(compaction_event() ::?EVENT_RUN      |
                           ?EVENT_DIAGNOSE |
                           ?EVENT_LOCK     |
                           ?EVENT_SUSPEND  |
                           ?EVENT_RESUME   |
                           ?EVENT_FINISH   |
                           ?EVENT_STATE    |
                           ?EVENT_INCREASE |
                           ?EVENT_DECREASE
                           ).

%% @doc Compaction related definitions
-define(RET_SUCCESS, 'success').
-define(RET_FAIL,    'fail').
-type(compaction_ret() :: ?RET_SUCCESS |
                          ?RET_FAIL |
                          undefined).

-define(MAX_LEN_HIST, 50).
-define(MAX_RETRY_TIMES, 2).
-define(WAIT_TIME_AFTER_ERROR, 200). %% 200ms

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
-define(ERROR_NOT_ALLOWED_ACCESS,       "not allowed access").
-define(ERROR_COULD_NOT_START_WORKER,   "could NOT start worker processes").
-define(ERROR_FREESPACE_LT_AVS,   "The disk free space is less than the size of the AVS file").

-define(ERROR_MSG_SLOW_OPERATION, 'slow_operation').
-define(ERROR_MSG_TIMEOUT,        'timeout').
-define(MSG_ITEM_SLOW_OP, 'slow_operation').
-define(MSG_ITEM_TIMEOUT, 'timeout').

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
%% https://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
%% According to the above spec, 5GB is the theoretical limit however the size is stored in a 32bit field
%% so we set it to 2GB for now.
-define(MAX_DATABLOCK_SIZE, 1024 * 1024 * 1024 * 2).

%% Constants to validate a chunk retrieved from AVS
%% spec: https://github.com/leo-project/leofs/issues/527#issuecomment-262163109
%% -define(MAX_KEY_SIZE, 2048). already defined at the above section
-define(MAX_MSIZE, 4096).
-define(MAX_CSIZE, ?MAX_DATABLOCK_SIZE).
-define(MAX_OFFSET, 1024 * 1024 * 1024 * 1024 * 16).
-define(MAX_CLOCK, 4633552912011362).


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

-define(DEF_POS_START, -1).
-define(DEF_POS_END,   -1).


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
          key = <<>> :: binary(),        %% filename
          addr_id = 0 :: integer(),      %% ring-address id (MD5 > hex-to-integer)
          ksize = 0 :: integer(),        %% file-path size
          dsize = 0 :: integer(),        %% data size
          msize = 0 :: integer(),        %% custom-metadata size
          csize  = 0 :: integer(),       %% * chunked data size    (for large-object)
          cnumber = 0 :: integer(),      %% * # of chunked objects (for large-object)
          cindex = 0 :: integer(),       %% * chunked object index (for large-object)
          offset = 0 :: integer(),       %% object-container's offset
          clock = 0 :: integer(),        %% clock
          timestamp = 0 :: integer(),    %% timestamp
          checksum = 0 :: integer(),     %% checksum (MD5 > hex-to-integer)
          ring_hash = 0 :: integer(),    %% RING's Hash(CRC32) when write an object.
          del = ?DEL_FALSE :: del_flag() %% [{0,not_deleted}, {1,deleted}]
         }).

-record(metadata_1, { %% leofs-v1.0.0 - v1.3.0
          key = <<>> :: binary(),           %% filename
          addr_id = 0 :: integer(),         %% ring-address id (MD5 > hex-to-integer)
          ksize = 0 :: integer(),           %% file-path size
          dsize = 0 :: integer(),           %% data size
          msize = 0 :: integer(),           %% custom-metadata size
          csize = 0 :: integer(),           %% * chunked data size    (for large-object)
          cnumber = 0 :: integer(),         %% * # of chunked objects (for large-object)
          cindex = 0 :: integer(),          %% * chunked object index (for large-object)
          offset = 0 :: integer(),          %% object-container's offset
          clock = 0 :: integer(),           %% clock
          timestamp = 0 :: integer(),       %% timestamp
          checksum = 0 :: integer(),        %% checksum (MD5 > hex-to-integer)
          ring_hash = 0 :: integer(),       %% RING's Hash(CRC32) when write an object.
          cluster_id :: atom(),             %% [+] cluster-id for the mdc-replication
          num_of_replicas = 0 :: integer(), %% [+] # of replicas for the mdc-replication
          ver = 0 :: integer(),             %% [+] version number
          del = ?DEL_FALSE :: del_flag()    %% [{0,not_deleted}, {1,deleted}]
         }).

-record(metadata_2, { %% leofs-v1.3.1 - v1.3.2.1
          key = <<>> :: binary(),           %% filename
          addr_id = 0 :: integer(),         %% ring-address id (MD5 > hex-to-integer)
          ksize = 0 :: integer(),           %% file-path size
          dsize = 0 :: integer(),           %% data size
          meta = <<>> :: binary(),          %% [+] custom-metadata (user defined metadata)
          msize = 0 :: integer(),           %% custom-metadata size
          csize = 0 :: integer(),           %% * chunked data size    (for large-object)
          cnumber = 0 :: integer(),         %% * # of chunked objects (for large-object)
          cindex  = 0 :: integer(),         %% * chunked object index (for large-object)
          offset  = 0 :: integer(),         %% object-container's offset
          clock = 0 :: integer(),           %% clock
          timestamp = 0 :: integer(),       %% timestamp
          checksum = 0 :: integer(),        %% checksum (MD5 > hex-to-integer)
          ring_hash = 0 :: integer(),       %% RING's Hash(CRC32) when write an object.
          cluster_id :: atom(),             %% cluster-id for the mdc-replication
          num_of_replicas = 0 :: integer(), %% # of replicas for the mdc-replication
          ver = 0 :: integer(),             %% version number
          del = ?DEL_FALSE :: del_flag()    %% [{0,not_deleted}, {1,deleted}]
         }).

-record(metadata_3, { %% leofs-v1.3.1 - the latest version
          key = <<>> :: binary(),                   %% filename
          addr_id = 0 :: integer(),                 %% ring-address id (MD5 > hex-to-integer)
          ksize = 0 :: integer(),                   %% file-path size
          dsize = 0 :: integer(),                   %% data size
          meta = <<>> :: binary(),                  %% custom-metadata (user defined metadata)
          msize = 0 :: integer(),                   %% custom-metadata size
          csize = 0 :: integer(),                   %% * chunked data size    (for large-object)
          cnumber = 0 :: integer(),                 %% * # of chunked objects (for large-object)
          cindex  = 0 :: integer(),                 %% * chunked object index (for large-object)
          offset  = 0 :: integer(),                 %% object-container's offset
          clock = 0 :: integer(),                   %% clock
          timestamp = 0 :: integer(),               %% timestamp
          checksum = 0 :: integer(),                %% checksum (MD5 > hex-to-integer)
          ring_hash = 0 :: integer(),               %% RING's Hash(CRC32) when write an object.
          cluster_id :: atom(),                     %% cluster-id for the mdc-replication
          num_of_replicas = 0 :: non_neg_integer(), %% [mdcr/bucket] # of replicas for the mdc-replication
                                                    %%              - [0: no effects,
                                                    %%                 1..*: preferred value of the data-replicatino]
                                                    %%                  as well as preferred_r, preferred_w, preferred_d
          preferred_r = 0 :: non_neg_integer(),     %% [+] [mdcr/bucket] # of replicas needed for a successful READ operation
          preferred_w = 0 :: non_neg_integer(),     %% [+] [mdcr/bucket] # of replicas needed for a successful WRITE operation
          preferred_d = 0 :: non_neg_integer(),     %% [+] [mdcr/bucket] # of replicas needed for a successful DELETE operation
          ver = 0 :: integer(),                     %% version number
          del = ?DEL_FALSE :: del_flag()            %% [{0,not_deleted}, {1,deleted}]
         }).
-define(METADATA, 'metadata_3').

-record(object, { %% - leofs-v1.0.0-pre3
          method,
          key = <<>> :: binary(),        %% filename
          addr_id = 0 :: integer(),      %% ring-address id (MD5 > hex-to-integer)
          data = <<>> :: binary(),       %% file
          meta = <<>> :: binary(),       %% custom-metadata
          ksize = 0 :: integer(),        %% filename size
          dsize = 0 :: integer(),        %% data size
          msize = 0 :: integer(),        %% custom-metadata size
          csize = 0 :: integer(),        %% * chunked data size    (for large-object)
          cnumber = 0 :: integer(),      %% * # of chunked objects (for large-object)
          cindex = 0 :: integer(),       %% * chunked object index (for large-object)
          offset = 0 :: integer(),       %% object-container's offset
          clock = 0 :: integer(),        %% clock
          timestamp = 0 :: integer(),    %% timestamp
          checksum = 0 :: integer(),     %% checksum (MD5 > hex-to-integer)
          ring_hash = 0 :: integer(),    %% RING's Hash(CRC32) when write an object.
          req_id = 0 :: integer(),       %% request id
          del = ?DEL_FALSE :: del_flag() %% delete flag
         }).

-record(object_1, { %% leofs-v1.0.0 - v1.3.2.1
          method,
          key = <<>>   :: binary(),         %% filename
          addr_id = 0 :: integer(),         %% ring-address id (MD5 > hex-to-integer)
          data = <<>> :: binary(),          %% file
          meta = <<>> :: binary(),          %% custom-metadata (user defined metadata)
          ksize = 0 :: integer(),           %% filename size
          dsize = 0 :: integer(),           %% data size
          msize = 0 :: integer(),           %% custom-metadata size
          csize = 0 :: integer(),           %% * chunked data size    (for large-object)
          cnumber = 0 :: integer(),         %% * # of chunked objects (for large-object)
          cindex = 0 :: integer(),          %% * chunked object index (for large-object)
          offset = 0 :: integer(),          %% object-container's offset
          clock = 0 :: integer(),           %% clock
          timestamp = 0 :: integer(),       %% timestamp
          checksum = 0 :: integer(),        %% checksum (MD5 > hex-to-integer)
          ring_hash = 0 :: integer(),       %% RING's Hash(CRC32) when write an object.
          req_id= 0 :: integer(),           %% request id
          cluster_id :: atom(),             %% [+] cluster-id for the mdc-replication
          num_of_replicas = 0 :: integer(), %% [+] # of replicas for the mdc-replication
          ver = 0 :: integer(),             %% [+] version number
          del = ?DEL_FALSE :: del_flag()    %% delete flag
         }).

-record(object_2, { %% leofs-v1.3.3 - the latest version
          method,
          key = <<>>   :: binary(),         %% filename
          addr_id = 0 :: integer(),         %% ring-address id (MD5 > hex-to-integer)
          data = <<>> :: binary(),          %% file
          meta = <<>> :: binary(),          %% custom-metadata (user defined metadata)
          ksize = 0 :: integer(),           %% filename size
          dsize = 0 :: integer(),           %% data size
          msize = 0 :: integer(),           %% custom-metadata size
          csize = 0 :: integer(),           %% * chunked data size    (for large-object)
          cnumber = 0 :: integer(),         %% * # of chunked objects (for large-object)
          cindex = 0 :: integer(),          %% * chunked object index (for large-object)
          offset = 0 :: integer(),          %% object-container's offset
          clock = 0 :: integer(),           %% clock
          timestamp = 0 :: integer(),       %% timestamp
          checksum = 0 :: integer(),        %% checksum (MD5 > hex-to-integer)
          ring_hash = 0 :: integer(),       %% RING's Hash(CRC32) when write an object.
          req_id= 0 :: integer(),           %% request id
          cluster_id :: atom(),             %% cluster-id for the mdc-replication
          num_of_replicas = 0 :: non_neg_integer(), %% [mdcr/bucket] # of replicas for the mdc-replication
                                                    %%              - [0: no effects,
                                                    %%                 1..*: preferred value of the data-replicatino]
                                                    %%                  as well as preferred_r, preferred_w, preferred_d
          preferred_r = 0 :: non_neg_integer(),     %% [+] [mdcr/bucket] # of replicas needed for a successful READ operation
          preferred_w = 0 :: non_neg_integer(),     %% [+] [mdcr/bucket] # of replicas needed for a successful WRITE operation
          preferred_d = 0 :: non_neg_integer(),     %% [+] [mdcr/bucket] # of replicas needed for a successful DELETE operation
          ver = 0 :: integer(),                     %% version number
          del = ?DEL_FALSE :: del_flag()            %% delete flag
         }).
-define(OBJECT, 'object_2').

-record(storage_stats, {
          file_path       = [] :: string(),
          total_sizes     = 0  :: non_neg_integer(),
          active_sizes    = 0  :: non_neg_integer(),
          total_num       = 0  :: non_neg_integer(),
          active_num      = 0  :: non_neg_integer(),
          compaction_hist = [] :: [#compaction_hist{}]
         }).


-define(DEF_SYNC_INTERVAL, 1000).
-define(SYNC_MODE_NONE, 'none').
-define(SYNC_MODE_PERIODIC, 'periodic').
-define(SYNC_MODE_WRITETHROUGH, 'writethrough').
-type(sync_mode() :: ?SYNC_MODE_NONE |
                     ?SYNC_MODE_PERIODIC |
                     ?SYNC_MODE_WRITETHROUGH).
-define(OBJ_PRV_READ_WRITE, 'read_and_write').
-define(OBJ_PRV_READ_ONLY, 'read').
-define(OBJ_PRV_WRITE_ONLY, 'write').
-type(obj_privilege() :: ?OBJ_PRV_READ_WRITE |
                         ?OBJ_PRV_READ_ONLY |
                         ?OBJ_PRV_WRITE_ONLY).
-record(obj_server_state, {
          id :: atom(),
          seq_num = 0 :: non_neg_integer(),
          privilege = ?OBJ_PRV_READ_WRITE :: obj_privilege(),
          meta_db_id :: atom(),
          compaction_worker_id :: atom(),
          diagnosis_logger_id :: atom(),
          root_path = [] :: string(),
          object_storage = #backend_info{}  :: #backend_info{},
          storage_stats  = #storage_stats{} :: #storage_stats{},
          state_filepath :: string(),
          sync_mode = ?SYNC_MODE_NONE :: sync_mode(),
          sync_interval_in_ms = ?DEF_SYNC_INTERVAL :: pos_integer(),
          is_strict_check = false :: boolean(),
          is_locked = false :: boolean(),
          is_del_blocked = false  :: boolean(),
          threshold_slow_processing = ?DEF_THRESHOLD_SLOW_PROC :: non_neg_integer(),
          is_able_to_write = true :: boolean()
         }).

%% apllication-env
-define(env_metadata_db(),
        case application:get_env(?APP_NAME, metadata_storage) of
            {ok, EnvMetadataDB} ->
                EnvMetadataDB;
            _ ->
                ?DEF_METADATA_DB
        end).

-define(env_sync_mode(),
        case application:get_env(?APP_NAME, sync_mode) of
            {ok, SyncMode} ->
                SyncMode;
            _ ->
                ?SYNC_MODE_NONE
        end).

-define(env_sync_interval(),
        case application:get_env(?APP_NAME, sync_interval_in_ms) of
            {ok, SyncInterval} ->
                SyncInterval;
            _ ->
                ?DEF_SYNC_INTERVAL
        end).

-ifdef(TEST).
-define(env_strict_check(), true).
-else.
-define(env_strict_check(),
        case application:get_env(?APP_NAME, is_strict_check) of
            {ok, EnvStrictCheck} ->
                EnvStrictCheck;
            _ ->
                false
        end).
-endif.

-define(env_enable_diagnosis_log(),
        case application:get_env(?APP_NAME,
                                 is_enable_diagnosis_log) of
            {ok, true} ->
                true;
            _ ->
                false
        end).

-define(env_num_of_obj_storage_read_procs(),
        case application:get_env(?APP_NAME,
                                 num_of_obj_storage_read_procs) of
            {ok, EnvNumOfObjStorageReadProcs} ->
                EnvNumOfObjStorageReadProcs;
            _ ->
                ?DEF_NUM_OF_OBJ_STORAGE_READ_PROCS
        end).

-define(get_obj_storage_read_proc(_PidL,_AddrId),
        begin
            lists:nth((_AddrId rem erlang:length(_PidL)) + 1,_PidL)
        end).


-define(env_limit_num_of_compaction_procs(),
        case application:get_env(?APP_NAME,
                                 limit_num_of_compaction_procs) of
            {ok, EnvLimitCompactionProcs} when is_integer(EnvLimitCompactionProcs) ->
                EnvLimitCompactionProcs;
            _ ->
                ?DEF_LIMIT_COMPACTION_PROCS
        end).

-define(env_threshold_slow_processing(),
        case application:get_env(?APP_NAME,
                                 threshold_slow_processing) of
            {ok, EnvThresholdSlowProc} when is_integer(EnvThresholdSlowProc) ->
                EnvThresholdSlowProc;
            _ ->
                ?DEF_THRESHOLD_SLOW_PROC
        end).

%% [Interval between batch processes]
-define(env_compaction_interval_reg(),
        case application:get_env(?APP_NAME,
                                 compaction_waiting_time_regular) of
            {ok, EnvRegCompactionWT} when is_integer(EnvRegCompactionWT) ->
                EnvRegCompactionWT;
            _ ->
                ?DEF_REG_COMPACTION_WT
        end).

-define(env_compaction_interval_max(),
        case application:get_env(?APP_NAME,
                                 compaction_waiting_time_max) of
            {ok, EnvMaxCompactionWT} when is_integer(EnvMaxCompactionWT) ->
                EnvMaxCompactionWT;
            _ ->
                ?DEF_MAX_COMPACTION_WT
        end).

%% [Number of batch processes]
-define(env_compaction_num_of_batch_procs_max(),
        case application:get_env(?APP_NAME,
                                 batch_procs_max) of
            {ok, EnvMaxCompactionBP} when is_integer(EnvMaxCompactionBP) ->
                EnvMaxCompactionBP;
            _ ->
                ?DEF_MAX_COMPACTION_BP
        end).

-define(env_compaction_num_of_batch_procs_reg(),
        case application:get_env(?APP_NAME,
                                 batch_procs_regular) of
            {ok, EnvRegCompactionBP} when is_integer(EnvRegCompactionBP) ->
                EnvRegCompactionBP;
            _ ->
                ?DEF_REG_COMPACTION_BP
        end).

-define(env_compaction_skip_prefetch_size(),
        case application:get_env(?APP_NAME,
                                 skip_prefetch_size) of
            {ok, EnvPrefetchSize} when is_integer(EnvPrefetchSize) ->
                EnvPrefetchSize;
            _ ->
                ?DEF_COMPACTION_SKIP_PS
        end).

-define(num_of_compaction_concurrency(_PrmNumOfConcurrency),
        begin
            _EnvNumOfConcurrency = ?env_limit_num_of_compaction_procs(),
            case (_PrmNumOfConcurrency > _EnvNumOfConcurrency) of
                true  -> _EnvNumOfConcurrency;
                false -> _PrmNumOfConcurrency
            end
        end).

-define(env_compaction_force_quit_in_bytes(),
        case application:get_env(?APP_NAME, force_quit_in_bytes) of
            {ok, ForceQuitInBytes} ->
                ForceQuitInBytes;
            _ ->
                ?DEF_COMPACTION_FQ_IN_BYTES
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

-define(env_diskspace_check_intervals(),
        case application:get_env(?APP_NAME, diskspace_check_intervals) of
            {ok, EnvDiskSpaceCheckIntervals} ->
                EnvDiskSpaceCheckIntervals;
            _ ->
                timer:minutes(1)
        end).

-define(env_enable_msg_collector(),
        case application:get_env(?APP_NAME, msg_collector) of
            {ok, true} ->
                true;
            _ ->
                false
        end).

%% custom-metadata's items for MDC-replication:
-define(PROP_CMETA_CLUSTER_ID, 'cluster_id').
-define(PROP_CMETA_NUM_OF_REPLICAS, 'num_of_replicas').
-define(PROP_CMETA_PREFERRED_R, 'preferred_r').
-define(PROP_CMETA_PREFERRED_W, 'preferred_w').
-define(PROP_CMETA_PREFERRED_D, 'preferred_d').
-define(PROP_CMETA_VER, 'ver').
-define(PROP_CMETA_UDM, 'udm'). %% user defined metadata: [{<KEY>, <VALUE>}]


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

%% @doc Default value of step of a batch-processing
-define(DEF_COMPACTION_NUM_OF_STEPS, 10).

%% @doc Compaction-related records:
-record(compaction_event_info, {
          id :: atom(),
          event = ?EVENT_RUN :: compaction_event(),
          controller_pid :: pid(),
          client_pid     :: pid(),
          is_diagnosing = false :: boolean(),
          is_recovering = false :: boolean(),
          is_forced_run = false :: boolean(),
          callback :: function()
         }).

-record(compaction_prms, {
          key_bin  = <<>> :: binary(),
          body_bin = <<>> :: binary(),
          metadata = #?METADATA{} :: #?METADATA{},
          next_offset = 0         :: non_neg_integer()|eof,
          start_lock_offset = 0   :: non_neg_integer(),
          callback_fun            :: function(),
          num_of_active_objs = 0  :: non_neg_integer(),
          size_of_active_objs = 0 :: non_neg_integer(),
          total_num_of_objs = 0   :: non_neg_integer(),
          total_size_of_objs = 0  :: non_neg_integer()
         }).

-record(compaction_skip_garbage, {
          buf = <<>> :: binary(),
          read_pos = 0 :: non_neg_integer(),
          prefetch_size = ?DEF_COMPACTION_SKIP_PS :: pos_integer(),
          is_skipping = false :: boolean(),
          is_close_eof = false :: boolean()
         }).

-record(compaction_worker_state, {
          id :: atom(),
          obj_storage_id :: atom(),
          obj_storage_id_read :: atom(),
          meta_db_id :: atom(),
          obj_storage_info = #backend_info{} :: #backend_info{},
          compact_cntl_pid :: pid(),
          diagnosis_log_id :: atom(),
          status = ?ST_IDLING :: compaction_state(),
          is_locked = false :: boolean(),
          is_diagnosing = false :: boolean(),
          is_recovering = false :: boolean(),
          is_forced_suspending = false :: boolean(),
          is_skipping_garbage = false :: boolean(),
          %% interval_between_batch_procs:
          interval = 0 :: non_neg_integer(),
          max_interval = 0 :: non_neg_integer(),
          %% batch-procs:
          count_procs = 0 :: non_neg_integer(),
          num_of_batch_procs = 0 :: non_neg_integer(),
          max_num_of_batch_procs = 0 :: non_neg_integer(),
          num_of_steps = ?DEF_COMPACTION_NUM_OF_STEPS :: pos_integer(),
          %% compaction-info:
          compaction_prms = #compaction_prms{} :: #compaction_prms{},
          compaction_skip_garbage = #compaction_skip_garbage{} :: #compaction_skip_garbage{},
          skipped_bytes = 0 :: non_neg_integer(),
          start_datetime = 0 :: non_neg_integer(),
          error_pos = 0 :: non_neg_integer(),
          set_errors :: otp_set(),
          acc_errors = [] :: [{pos_integer(), pos_integer()}],
          result :: compaction_ret()
         }).

%% @doc Retrieve compaction-proc's step parameters
-define(step_compaction_proc_values(_RegBatchProcs,_RegInterval,_NumOfSteps),
        begin
            _StepBatchOfProcs = leo_math:ceiling(_RegBatchProcs / _NumOfSteps),
            _StepInterval = leo_math:ceiling(_RegInterval / _NumOfSteps),
            {ok, {_StepBatchOfProcs,_StepInterval}}
        end).


%% @doc Retrieve the begining of statistics_wallclock
-define(begin_statistics_wallclock(),
        erlang:element(1, erlang:statistics(wall_clock))).

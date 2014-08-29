%%======================================================================
%%
%% Leo Object Storage
%%
%% Copyright (c) 2012-2014 Rakuten, Inc.
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

-define(ST_IDLING,     'idling').
-define(ST_RUNNING,    'running').
-define(ST_SUSPENDING, 'suspending').
-type(state_of_compaction() :: ?ST_IDLING  |
                               ?ST_RUNNING |
                               ?ST_SUSPENDING).

-define(EVENT_RUN,     'run').
-define(EVENT_SUSPEND, 'suspend').
-define(EVENT_RESUME,  'resume').
-define(EVENT_FINISH,  'finish').
%% -type(event_of_compaction() :: ?EVENT_RUN     |
%%                                ?EVENT_SUSPEND |
%%                                ?EVENT_RESUME  |
%%                                ?EVENT_FINISH).

%% @doc Compaction related definitions
-type(compaction_history() :: {integer(), integer()}).
-type(compaction_histories() :: [compaction_history()]).

-record(compaction_stats, {
          status = ?ST_IDLING :: state_of_compaction(),
          total_num_of_targets    = 0  :: non_neg_integer(),
          num_of_reserved_targets = 0  :: non_neg_integer(),
          num_of_pending_targets  = 0  :: non_neg_integer(),
          num_of_ongoing_targets  = 0  :: non_neg_integer(),
          reserved_targets = []        :: list(),
          pending_targets  = []        :: list(),
          ongoing_targets  = []        :: list(),
          latest_exec_datetime = 0     :: non_neg_integer()
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


%%--------------------------------------------------------------------
%% Records
%%--------------------------------------------------------------------
-record(backend_info, {
          backend                    :: atom(),
          avs_ver_cur = <<>> :: binary(),
          avs_ver_prv = <<>> :: binary(), %% need to know during compaction
          linked_path = []   :: string(),
          file_path   = []   :: string(),
          write_handler      :: pid(),
          read_handler       :: pid()
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
          file_path            = []    :: string(),
          total_sizes          = 0     :: non_neg_integer(),
          active_sizes         = 0     :: non_neg_integer(),
          total_num            = 0     :: non_neg_integer(),
          active_num           = 0     :: non_neg_integer(),
          compaction_histories = []    :: compaction_histories(),
          has_error            = false :: boolean()
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

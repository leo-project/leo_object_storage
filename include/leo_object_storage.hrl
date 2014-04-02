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
-define(DEF_METADATA_DB,              'bitcask').
-define(DEF_OBJECT_STORAGE_SUB_DIR,   "object/").
-define(DEF_METADATA_STORAGE_SUB_DIR, "metadata/").
-define(DEF_STATE_SUB_DIR,            "state/").

%% ETS-Table
-define(ETS_CONTAINERS_TABLE, 'leo_object_storage_containers').
-define(ETS_INFO_TABLE,       'leo_object_storage_info').

%% regarding compaction
-define(ENV_COMPACTION_STATUS, 'compaction_status').
-define(STATE_COMPACTING,      'compacting').
-define(STATE_ACTIVE,          'active').
-type(storage_status() :: ?STATE_COMPACTING | ?STATE_ACTIVE).

%% Error Constants
%%
-define(ERROR_FD_CLOSED,               "already closed file-descriptor").
-define(ERROR_FILE_OPEN,               "file open error").
-define(ERROR_INVALID_DATA,            "invalid data").
-define(ERROR_DATA_SIZE_DID_NOT_MATCH, "data-size did not match").
-define(ERROR_COMPACT_SUSPEND_FAILURE, "comaction-suspend filure").
-define(ERROR_COMPACT_RESUME_FAILURE,  "comaction-resume filure").


-define(DEL_TRUE,  1).
-define(DEL_FALSE, 0).

-type(del_flag() :: ?DEL_TRUE | ?DEL_FALSE).
-type(type_of_method() :: get | put | delete | head).


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
          backend             :: atom(),
          avs_version_bin_cur :: binary(),
          avs_version_bin_prv :: binary(), %% need to know during compaction
          file_path           :: string(),
          file_path_raw       :: string(),
          write_handler       :: pid(),
          read_handler        :: pid(),
          tmp_file_path_raw   :: string(),
          tmp_write_handler   :: pid(),
          tmp_read_handler    :: pid()
         }).

-record(metadata, { %% - leofs-v1.0.0-pre3
          key = <<>>          :: binary(),      %% filename
          addr_id    = 0      :: pos_integer(), %% ring-address id (MD5 > hex-to-integer)
          ksize      = 0      :: pos_integer(), %% file-path size
          dsize      = 0      :: pos_integer(), %% data size
          msize      = 0      :: pos_integer(), %% custom-metadata size

          csize      = 0      :: pos_integer(), %% * chunked data size    (for large-object)
          cnumber    = 0      :: pos_integer(), %% * # of chunked objects (for large-object)
          cindex     = 0      :: pos_integer(), %% * chunked object index (for large-object)

          offset     = 0      :: pos_integer(), %% object-container's offset
          clock      = 0      :: pos_integer(), %% clock
          timestamp  = 0      :: pos_integer(), %% timestamp
          checksum   = 0      :: pos_integer(), %% checksum (MD5 > hex-to-integer)
          ring_hash  = 0      :: pos_integer(), %% RING's Hash(CRC32) when write an object.
          del = ?DEL_FALSE    :: del_flag() %% [{0,not_deleted}, {1,deleted}]
         }).

-record(metadata_1, { %% leofs-v1.0.0 - current ver
          key = <<>>          :: binary(),      %% filename
          addr_id    = 0      :: pos_integer(), %% ring-address id (MD5 > hex-to-integer)
          ksize      = 0      :: pos_integer(), %% file-path size
          dsize      = 0      :: pos_integer(), %% data size
          msize      = 0      :: pos_integer(), %% custom-metadata size

          csize      = 0      :: pos_integer(), %% * chunked data size    (for large-object)
          cnumber    = 0      :: pos_integer(), %% * # of chunked objects (for large-object)
          cindex     = 0      :: pos_integer(), %% * chunked object index (for large-object)

          offset     = 0      :: pos_integer(), %% object-container's offset
          clock      = 0      :: pos_integer(), %% clock
          timestamp  = 0      :: pos_integer(), %% timestamp
          checksum   = 0      :: pos_integer(), %% checksum (MD5 > hex-to-integer)
          ring_hash  = 0      :: pos_integer(), %% RING's Hash(CRC32) when write an object.

          cluster_id          :: atom(),        %% cluster-id for the mdc-replication
          num_of_replicas = 0 :: pos_integer(), %% # of replicas for the mdc-replication
          ver = 0             :: pos_integer(), %% version number

          del = ?DEL_FALSE    :: del_flag() %% [{0,not_deleted}, {1,deleted}]
         }).
-define(METADATA, 'metadata_1').

-record(object, { %% - leofs-v1.0.0-pre3
          method,
          key        = <<>>   :: binary(),  %% filename
          addr_id    = 0      :: integer(), %% ring-address id (MD5 > hex-to-integer)
          data       = <<>>   :: binary(),  %% file
          meta       = <<>>   :: binary(),  %% custom-metadata
          ksize      = 0      :: pos_integer(), %% filename size
          dsize      = 0      :: pos_integer(), %% data size
          msize      = 0      :: pos_integer(), %% custom-metadata size

          csize      = 0      :: pos_integer(), %% * chunked data size    (for large-object)
          cnumber    = 0      :: pos_integer(), %% * # of chunked objects (for large-object)
          cindex     = 0      :: pos_integer(), %% * chunked object index (for large-object)

          offset     = 0      :: pos_integer(), %% object-container's offset
          clock      = 0      :: pos_integer(), %% clock
          timestamp  = 0      :: pos_integer(), %% timestamp
          checksum   = 0      :: pos_integer(), %% checksum (MD5 > hex-to-integer)
          ring_hash  = 0      :: pos_integer(), %% RING's Hash(CRC32) when write an object.
          req_id     = 0      :: pos_integer(), %% request id
          del = ?DEL_FALSE    :: del_flag() %% delete flag
         }).

-record(object_1, { %% leofs-v1.0.0 - current ver
          method,
          key        = <<>>   :: binary(),  %% filename
          addr_id    = 0      :: integer(), %% ring-address id (MD5 > hex-to-integer)
          data       = <<>>   :: binary(),  %% file
          meta       = <<>>   :: binary(),  %% custom-metadata
          ksize      = 0      :: pos_integer(), %% filename size
          dsize      = 0      :: pos_integer(), %% data size
          msize      = 0      :: pos_integer(), %% custom-metadata size

          csize      = 0      :: pos_integer(), %% * chunked data size    (for large-object)
          cnumber    = 0      :: pos_integer(), %% * # of chunked objects (for large-object)
          cindex     = 0      :: pos_integer(), %% * chunked object index (for large-object)

          offset     = 0      :: pos_integer(), %% object-container's offset
          clock      = 0      :: pos_integer(), %% clock
          timestamp  = 0      :: pos_integer(), %% timestamp
          checksum   = 0      :: pos_integer(), %% checksum (MD5 > hex-to-integer)
          ring_hash  = 0      :: pos_integer(), %% RING's Hash(CRC32) when write an object.
          req_id     = 0      :: pos_integer(), %% request id

          cluster_id = []     :: string(),      %% cluster-id for the mdc-replication
          num_of_replicas = 0 :: pos_integer(), %% # of replicas for the mdc-replication
          ver = 0             :: pos_integer(), %% version number

          del = ?DEL_FALSE    :: del_flag()     %% delete flag
         }).
-define(OBJECT, 'object_1').

-record(storage_stats, {
          file_path            = []    :: string(),
          total_sizes          = 0     :: pos_integer(),
          active_sizes         = 0     :: pos_integer(),
          total_num            = 0     :: pos_integer(),
          active_num           = 0     :: pos_integer(),
          compaction_histories = []    :: compaction_histories(),
          has_error            = false :: boolean()
         }).

%% @doc Compaction related definitions
-type(compaction_history() :: {calendar:datetime(), calendar:datetime()}).
-type(compaction_histories() :: list(compaction_history())).

-define(COMPACTION_STATUS_IDLE,    'idle').
-define(COMPACTION_STATUS_RUNNING, 'running').
-define(COMPACTION_STATUS_SUSPEND, 'suspend').
-type(compaction_status() :: ?COMPACTION_STATUS_IDLE |
                             ?COMPACTION_STATUS_RUNNING |
                             ?COMPACTION_STATUS_SUSPEND).

-record(compaction_stats, {
          status = ?COMPACTION_STATUS_IDLE :: compaction_status(),
          total_num_of_targets    = 0  :: pos_integer(),
          num_of_reserved_targets = 0  :: pos_integer(),
          num_of_pending_targets  = 0  :: pos_integer(),
          num_of_ongoing_targets  = 0  :: pos_integer(),
          reserved_targets = []        :: list(),
          pending_targets  = []        :: list(),
          ongoing_targets  = []        :: list(),
          latest_exec_datetime = 0     :: pos_integer()
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

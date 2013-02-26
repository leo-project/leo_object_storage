%%======================================================================
%%
%% Leo Object Storage
%%
%% Copyright (c) 2012
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
-define(DEF_METADATA_DB,               'bitcask').
-define(DEF_OBJECT_STORAGE_SUB_DIR,    "object/").
-define(DEF_METADATA_STORAGE_SUB_DIR,  "metadata/").
-define(DEF_STATE_SUB_DIR,             "state/").


%% ETS-Table
-define(ETS_CONTAINERS_TABLE,  'leo_object_storage_containers').
-define(ETS_INFO_TABLE,        'leo_object_storage_info').


%% regarding compaction
-define(ENV_COMPACTION_STATUS, 'compaction_status').
-define(STATE_COMPACTING,  'compacting').
-define(STATE_ACTIVE,      'active').
-type(storage_status() :: ?STATE_COMPACTING | ?STATE_ACTIVE).


%% Error Constants
%%
-define(ERROR_FD_CLOSED,               "already closed file-descriptor").
-define(ERROR_FILE_OPEN,               "file open error").
-define(ERROR_INVALID_DATA,            "invalid data").
-define(ERROR_DATA_SIZE_DID_NOT_MATCH, "data-size did not match").

-define(DEL_TRUE,  1).
-define(DEL_FALSE, 0).

-type(del_flag() :: ?DEL_TRUE | ?DEL_FALSE).
-type(type_of_method() :: get | put | delete | head).
-type(compaction_history() :: {calendar:datetime(), calendar:datetime()}).
-type(compaction_histories() :: list(compaction_history())).


-record(backend_info, {
          backend             :: atom(),
          file_path           :: string(),
          file_path_raw       :: string(),
          write_handler       :: pid(),
          read_handler        :: pid(),
          tmp_file_path_raw   :: string(),
          tmp_write_handler   :: pid(),
          tmp_read_handler    :: pid()
         }).

-record(metadata, {
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

-record(object, {
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

-record(storage_stats, {
          file_path            = ""    :: string(),
          total_sizes          = 0     :: integer(),
          active_sizes         = 0     :: integer(),
          total_num            = 0     :: integer(),
          active_num           = 0     :: integer(),
          compaction_histories = []    :: compaction_histories(),
          has_error            = false :: boolean()
         }).


-define(env_metadata_db(),
        case application:get_env(?APP_NAME, metadata_storage) of
            {ok, EnvMetadataDB} -> EnvMetadataDB;
            _ -> ?DEF_METADATA_DB
        end).


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
-include_lib("eunit/include/eunit.hrl").

-define(APP_NAME,                      'leo_object_storage').
-define(DEF_OBJECT_STORAGE,            'haystack').
-define(DEF_METADATA_DB,               'bitcask').
-define(DEF_OBJECT_STORAGE_SUB_DIR,    "object/").
-define(DEF_METADATA_STORAGE_SUB_DIR,  "metadata/").

-define(ERROR_FD_CLOSED,               "already closed file-descriptor").
-define(ERROR_FILE_OPEN,               "file open error").
-define(ERROR_INVALID_DATA,            "invalid data").
-define(ERROR_DATA_SIZE_DID_NOT_MATCH, "data-size did not match").

-type(type_of_method() :: get | put | delete | head).


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
          key                 :: string(),
          addr_id    = 0      :: integer(),
          dsize      = 0      :: integer(),
          ksize      = 0      :: integer(),
          msize      = 0      :: integer(),
          offset     = 0      :: integer(),
          divs       = 1      :: integer(), %% # of object-divides
          chunks     = []     :: list(),    %% list of chunk-data's sizes
          clock      = 0      :: integer(),
          timestamp  = 0      :: integer(),
          checksum   = 0      :: integer(),
          ring_hash  = 0      :: integer(), %% RING's Hash(CRC32) when write an object.
          del        = 0      :: integer()  %% [{0,not_deleted}, {1,deleted}]
         }).

-record(object, {
          method,
          key        = []     :: list(),
          addr_id    = 0      :: integer(), %% MD5 > hex-to-integer
          data       = <<>>   :: binary(),
          meta       = <<>>   :: binary(),
          dsize      = 0      :: integer(),
          msize      = 0      :: integer(),
          offset     = 0      :: integer(),
          clock      = 0      :: integer(),
          timestamp  = 0      :: integer(),
          checksum   = 0      :: integer(), %% MD5 > hex-to-integer
          req_id     = 0      :: integer(),
          ring_hash  = 0      :: integer(), %% RING's Hash(CRC32) when write an object.
          del        = 0      :: integer()
         }).

-record(storage_stats, {
          file_path           :: string(),
          total_sizes  = 0    :: integer(),
          active_sizes = 0    :: integer(),
          total_num    = 0    :: integer(),
          active_num   = 0    :: integer()
         }).


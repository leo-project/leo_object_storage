%%======================================================================
%%
%% Leo Object Storage
%%
%% Copyright (c) 2012-2018 Rakuten, Inc.
%% Copyright (c) 2019-2025 Lions Data, Ltd.
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
%% @doc Diagnosis log module using OTP standard logger
%% @end
%%======================================================================
-module(leo_object_storage_diagnosis_log).

-author('Yosuke Hara').

-include("leo_object_storage.hrl").

-export([new/4,
         append/2,
         force_rotation/1,
         stop/1]).

%% Logger formatter callback
-export([format/2]).

-define(HANDLER_PREFIX, "leo_diagnosis_handler_").

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Create a new diagnosis logger
-spec(new(GroupId, LoggerId, LogFilePath, LogFileName) ->
             ok when GroupId::atom(),
                     LoggerId::atom(),
                     LogFilePath::string(),
                     LogFileName::string()).
new(_GroupId, LoggerId, LogFilePath, LogFileName) ->
    HandlerId = handler_id(LoggerId),
    FilePath = filename:join(LogFilePath, LogFileName),

    %% Configure the logger handler for diagnosis logs
    %% Using logger_std_h with file output for broader OTP compatibility
    Config = #{config => #{file => FilePath,
                           max_no_bytes => 104857600,  %% 100MB
                           max_no_files => 10},
               level => info,
               formatter => {?MODULE, #{}}},

    case logger:add_handler(HandlerId, logger_std_h, Config) of
        ok ->
            ok;
        {error, {already_exist, _}} ->
            ok;
        {error, Reason} ->
            error_logger:error_msg("Failed to add diagnosis logger ~p: ~p~n",
                                   [HandlerId, Reason]),
            ok
    end.

%% @doc Append a diagnosis log entry
-spec(append(LoggerId, LogData) ->
             ok when LoggerId::atom(),
                     LogData::map()).
append(LoggerId, LogData) ->
    HandlerId = handler_id(LoggerId),
    #{offset := Offset,
      addr_id := AddrId,
      path := Path,
      chunk_index := ChunkIndex,
      data_size := DataSize,
      clock := Clock,
      timestamp := Timestamp,
      del := Del} = LogData,

    %% Format: offset, addr_id, path, chunk_index, data_size, clock, timestamp, del
    LogLine = io_lib:format("~w\t~w\t~s\t~w\t~w\t~w\t~s\t~w",
                            [Offset, AddrId, Path, ChunkIndex,
                             DataSize, Clock, Timestamp, Del]),

    logger:log(info, LogLine, #{handler_id => HandlerId, domain => [diagnosis]}),
    ok.

%% @doc Force log rotation
-spec(force_rotation(LoggerId) ->
             ok when LoggerId::atom()).
force_rotation(LoggerId) ->
    HandlerId = handler_id(LoggerId),
    case logger:get_handler_config(HandlerId) of
        {ok, #{config := #{file := FilePath}}} ->
            %% Trigger rotation by removing and re-adding handler
            logger:remove_handler(HandlerId),

            %% Rename current file with timestamp
            Timestamp = leo_date:now(),
            RotatedFile = FilePath ++ "." ++ integer_to_list(Timestamp),
            file:rename(FilePath, RotatedFile),

            %% Re-add handler
            Config = #{config => #{file => FilePath,
                                   max_no_bytes => 104857600,
                                   max_no_files => 10},
                       level => info,
                       formatter => {?MODULE, #{}}},
            logger:add_handler(HandlerId, logger_std_h, Config),
            ok;
        _ ->
            ok
    end.

%% @doc Stop the diagnosis logger
-spec(stop(LoggerId) ->
             ok when LoggerId::atom()).
stop(LoggerId) ->
    HandlerId = handler_id(LoggerId),
    logger:remove_handler(HandlerId),
    ok.

%%--------------------------------------------------------------------
%% Logger formatter callback
%%--------------------------------------------------------------------

%% @doc Format function for logger formatter behavior
-spec format(LogEvent, Config) -> unicode:chardata() when
      LogEvent :: logger:log_event(),
      Config :: logger:formatter_config().
format(#{msg := {string, Msg}}, _Config) ->
    [Msg, "\n"];
format(#{msg := {report, _Report}}, _Config) ->
    [];
format(#{msg := {Format, Args}}, _Config) ->
    io_lib:format(Format, Args) ++ "\n";
format(_LogEvent, _Config) ->
    [].

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

%% @private
handler_id(LoggerId) ->
    list_to_atom(?HANDLER_PREFIX ++ atom_to_list(LoggerId)).

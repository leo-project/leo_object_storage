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
%% Leo Object Storage - Haystack.
%% @doc
%% @end
%%======================================================================
-module(leo_object_storage_haystack, [MetaDBId, StorageInfo]).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').
-vsn('0.9.0').

-include("leo_object_storage.hrl").
-include_lib("kernel/include/file.hrl").

-export([open/1,
         close/2,
         put/1,
         get/1,
         delete/1,
         head/1,
         fetch/2]).

-export([compact_put/4,
         compact_get/1,
         compact_get/2
        ]).

-define(ERR_TYPE_TIMEOUT, timeout).
-define(VS_PART_OF_HEADER, <<"CRC:32,KSIZE:8,DSIZE:32,OFFSET:32,ADDRID:32,VCLOCK:32,TIMESTAMP:56,DEL:8",13,10>>).
-define(VS_PART_OF_BODY,   <<"KEY/binary,DATA/binary",13,10>>).
-define(VS_PART_OF_FOOTER, <<"PADDING:64",13,10>>).
-define(VS_SUPER_BLOCK,    <<?VS_PART_OF_HEADER/binary,
                             ?VS_PART_OF_BODY/binary,
                             ?VS_PART_OF_FOOTER/binary>>).
-define(BLEN_CRC,     32).
-define(BLEN_KSIZE,   16).
-define(BLEN_MSIZE,   32).
-define(BLEN_DSIZE,   32).
-define(BLEN_OFFSET,  64).
-define(BLEN_ADDRID,  128).
-define(BLEN_VCLOCK,  64).
-define(BLEN_TS_Y,    16).
-define(BLEN_TS_M,     8).
-define(BLEN_TS_D,     8).
-define(BLEN_TS_H,     8).
-define(BLEN_TS_N,     8).
-define(BLEN_TS_S,     8).
-define(BLEN_DEL,      8).
-define(BLEN_HEADER, 432).
-define(LEN_PADDING,   8).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Open and clreate a file.
%%
-spec(open(FilePath::string) ->
             {ok, port(), port()} | {error, any()}).
open(FilePath) ->
    case create_file(FilePath) of
        {ok, WriteHandler} ->
            case open_fun(FilePath) of
                {ok, ReadHandler} ->
                    {ok, [WriteHandler, ReadHandler]};
                {error, Why} ->
                    {error, Why}
            end;
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Close file handlers.
%%
-spec(close(Writehandler::port(), ReadHandler::port()) ->
             ok).
close(WriteHandler, ReadHandler) ->
    catch file:close(WriteHandler),
    catch file:close(ReadHandler),
    ok.


%% @doc Insert an object and a metadata into the object-storage
%%
-spec(put(pid()) ->
             ok | {error, any()}).
put(ObjectPool) ->
    put_fun(first, ObjectPool).


%% @doc Retrieve an object and a metadata from the object-storage
%%
-spec(get(KeyBin::binary()) ->
             {ok, #metadata{}, pid()} | {error, any()}).
get(KeyBin) ->
    get_fun(KeyBin).


%% @doc Remove an object and a metadata from the object-storage
%%
-spec(delete(ObjectPool::pid()) ->
             ok | {error, any()}).
delete(ObjectPool) ->
    put_fun(first, ObjectPool).


%% @doc Retrieve a metada from backend_db from the object-storage
%%
-spec(head(KeyBin::binary()) ->
             {ok, #metadata{}} | not_found | {error, any()}).
head(KeyBin) ->
    case catch leo_backend_db_api:get(MetaDBId, KeyBin) of
        {ok, MetadataBin} ->
            {ok, MetadataBin};
        not_found = Cause ->
            Cause;
        {_, Cause} ->
            {error, Cause}
    end.


%% @doc Fetch objects from the object-storage
%%
-spec(fetch(binary(), function()) ->
             ok | {error, any()}).
fetch(KeyBin, Fun) ->
    leo_backend_db_api:fetch(MetaDBId, KeyBin, Fun).


%%--------------------------------------------------------------------
%% INNER FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Create an object-container and metadata into the object-storage
%% @private
create_file(FilePath) ->
    case catch file:open(FilePath, [raw, write,  binary, append]) of
        {ok, PutFileHandler} ->
            case file:position(PutFileHandler, eof) of
                {ok, Offset} when Offset == 0 ->
                    put_super_block(PutFileHandler);
                {ok,_Offset} ->
                    {ok, PutFileHandler};
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "create_file/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "create_file/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Open an object-container (*.avs) from the object-storage
%% @private
open_fun(FilePath) ->
    open_fun(FilePath, 0).

open_fun(_FilePath, 3) ->
    {error, ?ERROR_FILE_OPEN};

open_fun(FilePath, RetryTimes) ->
    timer:sleep(100),

    case filelib:is_file(FilePath) of
        false ->
            case file:open(FilePath, [raw, write,  binary, append]) of
                {ok, FileHandler} ->
                    file:close(FileHandler);
                {error, _Cause} ->
                    open_fun(FilePath, RetryTimes+1)
            end;
        true ->
            case file:open(FilePath, [raw, read, binary]) of
                {ok, FileHandler} ->
                    {ok, FileHandler};
                {error, _Cause} ->
                    open_fun(FilePath, RetryTimes+1)
            end
    end.


%%--------------------------------------------------------------------
%% OBJECT OPERATIONS.
%%--------------------------------------------------------------------
%% @doc Retrieve an object from object-storage
%% @private
get_fun(KeyBin) ->
    case catch leo_backend_db_api:get(MetaDBId, KeyBin) of
        {ok, MetadataBin} ->
            Metadata = binary_to_term(MetadataBin),
            case (Metadata#metadata.del == 0) of
                true  -> get_fun1(Metadata);
                false -> not_found
            end;
        Error ->
            case Error of
                not_found  ->
                    Error;
                {_, Cause} ->
                    {error, Cause}
            end
    end.

get_fun1(#metadata{key      = Key,
                   ksize    = KeySize,
                   dsize    = ObjectSize,
                   addr_id  = AddrId,
                   offset   = Offset,
                   checksum = Checksum} = Metadata) ->
    #backend_info{read_handler = ReadHandler} = StorageInfo,
    HeaderSize = erlang:round(?BLEN_HEADER/8),
    TotalSize  = HeaderSize + KeySize + ObjectSize + ?LEN_PADDING,

    case file:pread(ReadHandler, Offset, TotalSize) of
        {ok, Bin} ->
            case byte_size(Bin) of
                TotalSize ->
                    <<_Header:HeaderSize/binary,
                      KeyBin:KeySize/binary, ValueBin:ObjectSize/binary, _Footer/binary>> = Bin,

                    case erlang:crc32(
                           term_to_binary(
                             {KeySize, ObjectSize, AddrId, KeyBin, ValueBin})) of
                        Checksum ->
                            ObjectPool = leo_object_storage_pool:new(#object{key     = Key,
                                                                             addr_id = AddrId,
                                                                             data    = ValueBin,
                                                                             dsize   = ObjectSize}),
                            {ok, Metadata, ObjectPool};
                        _ ->
                            {error, ?ERROR_INVALID_DATA}
                    end;
                _ ->
                    {error, ?ERROR_DATA_SIZE_DID_NOT_MATCH}
            end;
        eof = Cause ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "get_fun/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "get_fun/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Insert a super-block into an object container (*.avs)
%% @private
put_super_block(ObjectStorageWriteHandler) ->
    case file:pwrite(ObjectStorageWriteHandler, 0, ?VS_SUPER_BLOCK) of
        ok ->
            {ok, ObjectStorageWriteHandler};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "put_super_block/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Insert an object into the object-storage
%% @private
put_fun(first, ObjectPool) ->
    case catch leo_object_storage_pool:get(ObjectPool) of
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "put_fun/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, ?ERR_TYPE_TIMEOUT};
        not_found ->
            {error, ?ERR_TYPE_TIMEOUT};

        #object{addr_id = AddrId,
                key     = Key,
                data    = ValueBin,
                dsize   = Size} = Object ->
            #backend_info{write_handler = ObjectStorageWriteHandler} = StorageInfo,

            case file:position(ObjectStorageWriteHandler, eof) of
                {ok, Offset} ->
                    Checksum = erlang:crc32(
                                 term_to_binary(
                                   {length(Key), Size, AddrId, list_to_binary(Key), ValueBin})),

                    put_fun(next, Object#object{checksum = Checksum,
                                                offset   =  Offset});
                {error, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "put_fun/2"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end
    end;

put_fun(next, #object{addr_id    = AddrId,
                      key        = Key,
                      dsize      = DSize,
                      msize      = MSize,
                      meta       = _MBin,
                      data       = Body,
                      clock      = Clock,
                      offset     = Offset,
                      timestamp  = Timestamp,
                      checksum   = Checksum,
                      ring_hash  = RingHash,
                      del        = Del}) ->
    {{Year,Month,Day},{Hour,Min,Second}} =
        calendar:gregorian_seconds_to_datetime(Timestamp),

    KeyBin  = erlang:list_to_binary(Key),
    KSize   = erlang:byte_size(KeyBin),
    Padding = <<0:64>>,
    Bin = <<KeyBin/binary, Body/binary, Padding/binary>>,
    Needle = <<Checksum:?BLEN_CRC,
               KSize:?BLEN_KSIZE, DSize:?BLEN_DSIZE, MSize:?BLEN_MSIZE, Offset:?BLEN_OFFSET,
               AddrId:?BLEN_ADDRID,
               Clock:?BLEN_VCLOCK,
               Year:?BLEN_TS_Y, Month:?BLEN_TS_M, Day:?BLEN_TS_D,
               Hour:?BLEN_TS_H, Min:?BLEN_TS_N,   Second:?BLEN_TS_S,
               Del:?BLEN_DEL,
               Bin/binary>>,

    Meta = #metadata{key       = Key,
                     addr_id   = AddrId,
                     ksize     = KSize,
                     msize     = MSize,
                     dsize     = DSize,
                     offset    = Offset,
                     clock     = Clock,
                     timestamp = Timestamp,
                     checksum  = Checksum,
                     ring_hash = RingHash,
                     del       = Del},
    put_fun(finally, Needle, Meta).

put_fun(finally, Needle, #metadata{key      = Key,
                                   addr_id  = AddrId,
                                   offset   = Offset} = Meta) ->
    #backend_info{write_handler = WriteHandler} = StorageInfo,

    case file:pwrite(WriteHandler, Offset, Needle) of
        ok ->
            case catch leo_backend_db_api:put(
                         MetaDBId, term_to_binary({AddrId, Key}), term_to_binary(Meta)) of
                ok ->
                    ok;
                {'EXIT', Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "put_fun/3"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause};
                Error ->
                    Error
            end;
        {error, Cause} ->
            {error, Cause}
    end.

%%--------------------------------------------------------------------
%% COMPACTION FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Insert an object into the object-container when compacting
%% @private
-spec(compact_put(pid(), #metadata{}, binary(), binary()) ->
             ok | {error, any()}).
compact_put(WriteHandler, #metadata{key       = _Key,
                                    addr_id   = AddrId,
                                    ksize     = KSize,
                                    msize     = MSize,
                                    dsize     = DSize,
                                    clock     = NumOfClock,
                                    timestamp = Timestamp,
                                    checksum  = Checksum,
                                    del       = Del} = _Meta, KeyBin, BodyBin) ->
    case file:position(WriteHandler, eof) of
        {ok, Offset} ->
            {{Year,Month,Day},{Hour,Min,Second}} = calendar:gregorian_seconds_to_datetime(Timestamp),
            Padding = <<0:64>>,
            Bin = <<KeyBin/binary, BodyBin/binary, Padding/binary>>,
            Needle = <<Checksum:?BLEN_CRC,
                       KSize:?BLEN_KSIZE, DSize:?BLEN_DSIZE, MSize:?BLEN_MSIZE, Offset:?BLEN_OFFSET,
                       AddrId:?BLEN_ADDRID,
                       NumOfClock:?BLEN_VCLOCK,
                       Year:?BLEN_TS_Y, Month:?BLEN_TS_M, Day:?BLEN_TS_D,
                       Hour:?BLEN_TS_H, Min:?BLEN_TS_N,   Second:?BLEN_TS_S,
                       Del:?BLEN_DEL,
                       Bin/binary>>,
            case file:pwrite(WriteHandler, Offset, Needle) of
                ok ->
                    {ok, Offset};
                {error, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "compact_put/4"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "compact_put/4"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Retrieve a file from object-container when compacting.
%% @private
-spec(compact_get(pid()) ->
             ok | {error, any()}).
compact_get(ReadHandler) ->
    compact_get(ReadHandler, byte_size(?VS_SUPER_BLOCK)).

-spec(compact_get(pid(), integer()) ->
             ok | {error, any()}).
compact_get(ReadHandler, Offset) ->
    HeaderSize = erlang:round(?BLEN_HEADER/8),
    case file:pread(ReadHandler, Offset, HeaderSize) of
        {ok, HeaderBin} ->
            BinLen = byte_size(HeaderBin),
            case BinLen of
                HeaderSize ->
                    compact_get(ReadHandler, Offset, HeaderSize, HeaderBin);
                _ ->
                    {error, ?ERROR_DATA_SIZE_DID_NOT_MATCH}
            end;
        eof = Cause ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "compact_get/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "compact_get/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Retrieve a file from object-container when compacting.
%% @private
-spec(compact_get(pid(), integer(), integer(), binary()) ->
             ok | {error, any()}).
compact_get(ReadHandler, Offset, HeaderSize, HeaderBin) ->
    <<Checksum:?BLEN_CRC,
      KSize:?BLEN_KSIZE, DSize:?BLEN_DSIZE, MSize:?BLEN_MSIZE, OrgOffset:?BLEN_OFFSET,
      AddrId:?BLEN_ADDRID/integer,
      NumOfClock:?BLEN_VCLOCK,
      Year:?BLEN_TS_Y, Month:?BLEN_TS_M, Day:?BLEN_TS_D,
      Hour:?BLEN_TS_H, Min:?BLEN_TS_N,   Second:?BLEN_TS_S,
      Del:?BLEN_DEL>> = HeaderBin,
    RemainSize = KSize + DSize + ?LEN_PADDING,

    case file:pread(ReadHandler, Offset + HeaderSize, RemainSize) of
        {ok, RemainBin} ->
            RemainLen = byte_size(RemainBin),
            case RemainLen of
                RemainSize ->
                    <<KeyValue:KSize/binary, BodyValue:DSize/binary, _Footer/binary>> = RemainBin,
                    case erlang:crc32(term_to_binary(
                                        {KSize, DSize, AddrId, KeyValue, BodyValue})) of
                        Checksum ->
                            Timestamp = calendar:datetime_to_gregorian_seconds(
                                          {{Year, Month, Day}, {Hour, Min, Second}}),
                            Meta = #metadata{key       = binary_to_list(KeyValue),
                                             addr_id   = AddrId,
                                             ksize     = KSize,
                                             msize     = MSize,
                                             dsize     = DSize,
                                             offset    = OrgOffset,
                                             clock     = NumOfClock,
                                             timestamp = Timestamp,
                                             checksum  = Checksum,
                                             del       = Del},
                            {ok, Meta, [HeaderBin, KeyValue, BodyValue,
                                        Offset + HeaderSize + RemainSize]};
                        _ ->
                            {error, ?ERROR_INVALID_DATA}
                    end;
                _ ->
                    {error, ?ERROR_DATA_SIZE_DID_NOT_MATCH}
            end;
        eof = Cause ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "compact_get/4"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "compact_get/4"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


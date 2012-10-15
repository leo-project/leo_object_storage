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

-include("leo_object_storage.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([open/1, close/2,
         put/1, get/1, get/3, delete/1, head/1, fetch/2, store/2]).

-export([compact_put/4,
         compact_get/1,
         compact_get/2
        ]).

-define(ERR_TYPE_TIMEOUT, timeout).


%% ------------------------ %%
%%       AVS-Related
%% ------------------------ %%
-define(AVS_HEADER_VSN,     <<"LeoFS AVS-2.2",13,10>>).
-define(AVS_PART_OF_HEADER, <<"CHKSUM:128,KSIZE:16,BLEN_MSIZE:32,DSIZE:32,OFFSET:64,ADDRID:128,CLOCK:64,TIMESTAMP:42,DEL:1,BUF:517,CHUNK_SIZE:32,CHUNK_NUM:24,CHUNK_INDEX:24",13,10>>).
-define(AVS_PART_OF_BODY,   <<"KEY/binary,DATA/binary",13,10>>).
-define(AVS_PART_OF_FOOTER, <<"PADDING:64",13,10>>).
-define(AVS_SUPER_BLOCK,     <<?AVS_HEADER_VSN/binary,
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
%% ----------------------------- %%


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
                Error ->
                    Error
            end;
        Error ->
            Error
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
             {ok, integer()} | {error, any()}).
put(ObjectPool) ->
    put_fun0(ObjectPool).


%% @doc Retrieve an object and a metadata from the object-storage
%%
-spec(get(KeyBin::binary()) ->
             {ok, #metadata{}, pid()} | {error, any()}).
get(KeyBin) ->
    get(KeyBin, 0, 0).

get(KeyBin, StartPos, EndPos) ->
    get_fun(KeyBin, StartPos, EndPos).


%% @doc Remove an object and a metadata from the object-storage
%%
-spec(delete(ObjectPool::pid()) ->
             ok | {error, any()}).
delete(ObjectPool) ->
    case put_fun0(ObjectPool) of
        {ok, _Checksum} ->
            ok;
        {error, Cause} ->
            {error, Cause}
    end.


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


%% @doc Store metadata and binary
%%
-spec(store(#metadata{}, binary()) ->
             ok | {error, any()}).
store(Metadata, Bin) ->
    Key = Metadata#metadata.key,
    Checksum = leo_hex:binary_to_integer(erlang:md5(Bin)),

    Object = #object{addr_id    = Metadata#metadata.addr_id,
                     key        = Key,
                     key_bin    = list_to_binary(Key),
                     ksize      = Metadata#metadata.ksize,
                     dsize      = Metadata#metadata.dsize,
                     data       = Bin,
                     clock      = Metadata#metadata.clock,
                     timestamp  = Metadata#metadata.timestamp,
                     checksum   = Checksum,
                     ring_hash  = Metadata#metadata.ring_hash,
                     del        = Metadata#metadata.del},
    ObjectPool = leo_object_storage_pool:new(Key, Metadata, Object),
    case put_fun0(ObjectPool) of
        {ok, _Checksum} ->
            ok;
        {error, Cause} ->
            {error, Cause}
    end.


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
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "create_file/1"},
                                            {line, ?LINE}, {body, Cause}]),
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
get_fun(KeyBin, StartPos, EndPos) ->
    case catch leo_backend_db_api:get(MetaDBId, KeyBin) of
        {ok, MetadataBin} ->
            Metadata = binary_to_term(MetadataBin),
            case (Metadata#metadata.del == ?DEL_FALSE) of
                true  -> get_fun1(Metadata, StartPos, EndPos);
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
                   dsize    = ObjectSize,
                   addr_id  = AddrId} = Metadata, StartPos, _) when StartPos >= ObjectSize ->
    {ok, Metadata, leo_object_storage_pool:new(#object{key     = Key,
                                                       addr_id = AddrId,
                                                       data    = <<>>,
                                                       dsize   = 0})};
get_fun1(#metadata{key      = Key,
                   ksize    = KeySize,
                   dsize    = ObjectSize,
                   addr_id  = AddrId,
                   offset   = Offset} = Metadata, StartPos, EndPos) ->
    %% If end-position equal 0,
    %% Then actual end-position is object-size.
    NewEndPos = case (EndPos == 0) of
                    true ->
                        ObjectSize;
                    false ->
                        case (EndPos > ObjectSize) of
                            true ->
                                ObjectSize;
                            false ->
                                EndPos
                        end
                end,

    %% Calculate actual start-point and end-point
    NewOffset     = Offset + erlang:round(?BLEN_HEADER/8) + KeySize + StartPos,
    NewObjectSize = NewEndPos - StartPos,

    #backend_info{read_handler = ReadHandler} = StorageInfo,

    %% Retrieve the object
    case file:pread(ReadHandler, NewOffset, NewObjectSize) of
        {ok, Bin} ->
            {ok, Metadata, leo_object_storage_pool:new(#object{key     = Key,
                                                               addr_id = AddrId,
                                                               data    = Bin,
                                                               dsize   = NewObjectSize})};
        eof = Cause ->
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
    case file:pwrite(ObjectStorageWriteHandler, 0, ?AVS_SUPER_BLOCK) of
        ok ->
            {ok, ObjectStorageWriteHandler};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "put_super_block/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Create a needle
%% @private
create_needle(#object{addr_id    = AddrId,
                      key_bin    = KeyBin,
                      ksize      = KSize,
                      dsize      = DSize,
                      msize      = MSize,
                      meta       = _MBin,
                      csize      = CSize,
                      cnumber    = CNum,
                      cindex     = CIndex,
                      data       = Body,
                      clock      = Clock,
                      offset     = Offset,
                      timestamp  = Timestamp,
                      checksum   = Checksum,
                      del        = Del}) ->
    {{Year,Month,Day},{Hour,Min,Second}} =
        calendar:gregorian_seconds_to_datetime(Timestamp),

    Padding = <<0:64>>,
    Bin     = << KeyBin/binary, Body/binary, Padding/binary >>,
    Needle  = << Checksum:?BLEN_CHKSUM,
                 KSize:?BLEN_KSIZE,
                 DSize:?BLEN_DSIZE,
                 MSize:?BLEN_MSIZE,
                 Offset:?BLEN_OFFSET,
                 AddrId:?BLEN_ADDRID,
                 Clock:?BLEN_CLOCK,
                 Year:?BLEN_TS_Y,
                 Month:?BLEN_TS_M,
                 Day:?BLEN_TS_D,
                 Hour:?BLEN_TS_H,
                 Min:?BLEN_TS_N,
                 Second:?BLEN_TS_S,
                 Del:?BLEN_DEL,
                 CSize:?BLEN_CHUNK_SIZE,
                 CNum:?BLEN_CHUNK_NUM,
                 CIndex:?BLEN_CHUNK_INDEX,
                 0:?BLEN_BUF,
                 Bin/binary >>,
    Needle.


%% @doc Insert an object into the object-storage
%% @private
put_fun0(ObjectPool) ->
    case catch leo_object_storage_pool:get(ObjectPool) of
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "put_fun0/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, ?ERR_TYPE_TIMEOUT};
        not_found ->
            {error, ?ERR_TYPE_TIMEOUT};

        #object{addr_id  = AddrId,
                key      = Key,
                checksum = Checksum0,
                del      = DelFlag} = Object ->
            Ret = case DelFlag of
                      ?DEL_FALSE ->
                          case head(term_to_binary({AddrId, Key})) of
                              {ok, MetadataBin} ->
                                  #metadata{checksum = Checksum1} = binary_to_term(MetadataBin),
                                  case (Checksum0 == Checksum1) of
                                      true  -> match;
                                      false -> not_match
                                  end;
                              _ ->
                                  not_match
                          end;
                      ?DEL_TRUE ->
                          not_match
                  end,

            case Ret of
                match ->
                    {ok, Checksum0};
                not_match ->
                    #backend_info{write_handler = ObjectStorageWriteHandler} = StorageInfo,

                    case file:position(ObjectStorageWriteHandler, eof) of
                        {ok, Offset} ->
                            put_fun1(Object#object{offset = Offset});
                        {error, Cause} ->
                            error_logger:error_msg("~p,~p,~p,~p~n",
                                                   [{module, ?MODULE_STRING}, {function, "put_fun0/1"},
                                                    {line, ?LINE}, {body, Cause}]),
                            {error, Cause}
                    end
            end
    end.

put_fun1(#object{addr_id    = AddrId,
                 key        = Key,
                 ksize      = KSize,
                 dsize      = DSize,
                 msize      = MSize,
                 meta       = _MBin,
                 csize      = CSize,
                 cnumber    = CNum,
                 cindex     = CIndex,
                 offset     = Offset,
                 clock      = Clock,
                 timestamp  = Timestamp,
                 checksum   = Checksum,
                 ring_hash  = RingHash,
                 del        = Del} = Object) ->
    Needle = create_needle(Object),
    Meta = #metadata{key       = Key,
                     addr_id   = AddrId,
                     ksize     = KSize,
                     msize     = MSize,
                     dsize     = DSize,
                     csize     = CSize,
                     cnumber   = CNum,
                     cindex    = CIndex,
                     offset    = Offset,
                     clock     = Clock,
                     timestamp = Timestamp,
                     checksum  = Checksum,
                     ring_hash = RingHash,
                     del       = Del},
    put_fun2(Needle, Meta).

put_fun2(Needle, #metadata{key      = Key,
                           addr_id  = AddrId,
                           offset   = Offset,
                           checksum  = Checksum} = Meta) ->
    #backend_info{write_handler = WriteHandler} = StorageInfo,

    case file:pwrite(WriteHandler, Offset, Needle) of
        ok ->
            case catch leo_backend_db_api:put(
                         MetaDBId, term_to_binary({AddrId, Key}), term_to_binary(Meta)) of
                ok ->
                    {ok, Checksum};
                {'EXIT', Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "put_fun2/2"},
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
compact_put(WriteHandler, #metadata{key       = Key,
                                    addr_id   = AddrId,
                                    ksize     = KSize,
                                    msize     = MSize,
                                    dsize     = DSize,
                                    csize     = CSize,
                                    cnumber   = CNum,
                                    cindex    = CIndex,
                                    clock     = Clock,
                                    timestamp = Timestamp,
                                    checksum  = Checksum,
                                    del       = Del} = _Meta, KeyBin, BodyBin) ->
    case file:position(WriteHandler, eof) of
        {ok, Offset} ->
            Needle = create_needle(#object{addr_id    = AddrId,
                                           key        = Key,
                                           key_bin    = KeyBin,
                                           ksize      = KSize,
                                           dsize      = DSize,
                                           msize      = MSize,
                                           csize      = CSize,
                                           cnumber    = CNum,
                                           cindex     = CIndex,
                                           data       = BodyBin,
                                           clock      = Clock,
                                           offset     = Offset,
                                           timestamp  = Timestamp,
                                           checksum   = Checksum,
                                           del        = Del}),

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
    compact_get(ReadHandler, byte_size(?AVS_SUPER_BLOCK)).

-spec(compact_get(pid(), integer()) ->
             ok | {error, any()}).
compact_get(ReadHandler, Offset) ->
    HeaderSize = erlang:round(?BLEN_HEADER/8),

    case file:pread(ReadHandler, Offset, HeaderSize) of
        {ok, HeaderBin} ->
            case byte_size(HeaderBin) of
                HeaderSize ->
                    compact_get(ReadHandler, Offset, HeaderSize, HeaderBin);
                _ ->
                    {error, ?ERROR_DATA_SIZE_DID_NOT_MATCH}
            end;
        eof = Cause ->
            {error, Cause};
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Retrieve a file from object-container when compacting.
%% @private
-spec(compact_get(pid(), integer(), integer(), binary()) ->
             ok | {error, any()}).
compact_get(ReadHandler, Offset, HeaderSize, HeaderBin) ->
    << Checksum:?BLEN_CHKSUM,
       KSize:?BLEN_KSIZE,
       DSize:?BLEN_DSIZE,
       MSize:?BLEN_MSIZE,
       OrgOffset:?BLEN_OFFSET,
       AddrId:?BLEN_ADDRID,
       NumOfClock:?BLEN_CLOCK,
       Year:?BLEN_TS_Y,
       Month:?BLEN_TS_M,
       Day:?BLEN_TS_D,
       Hour:?BLEN_TS_H,
       Min:?BLEN_TS_N,
       Second:?BLEN_TS_S,
       Del:?BLEN_DEL,
       _CSize:?BLEN_CHUNK_SIZE,
       _CNum:?BLEN_CHUNK_NUM,
       _CIndex:?BLEN_CHUNK_INDEX,
       _:?BLEN_BUF
    >> = HeaderBin,
    RemainSize = KSize + DSize + ?LEN_PADDING,

    case file:pread(ReadHandler, Offset + HeaderSize, RemainSize) of
        {ok, RemainBin} ->
            RemainLen = byte_size(RemainBin),
            case RemainLen of
                RemainSize ->
                    <<KeyValue:KSize/binary, BodyValue:DSize/binary, _Footer/binary>> = RemainBin,
                    case leo_hex:binary_to_integer(erlang:md5(BodyValue)) of
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
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "compact_get/4"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


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
%% Leo Object Storage - Haystack.
%% @doc
%% @end
%%======================================================================
-module(leo_object_storage_haystack).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-include("leo_object_storage.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([open/1, close/2,
         put/3, get/3, get/6, delete/3, head/2, fetch/4, store/4]).

-export([calc_obj_size/1,
         calc_obj_size/2,
         compact_put/4,
         compact_get/1,
         compact_get/2
        ]).

-ifdef(TEST).
-export([add_incorrect_data/2]).
-endif.

-define(ERR_TYPE_TIMEOUT, timeout).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Open and clreate a file.
%%
-spec(calc_obj_size(#?METADATA{}|#?OBJECT{}) -> integer()).
calc_obj_size(#?METADATA{ksize = KSize, dsize = DSize}) ->
    calc_obj_size(KSize, DSize);
calc_obj_size(#?OBJECT{key = Key, dsize = DSize}) ->
    KSize = byte_size(Key),
    calc_obj_size(KSize, DSize).
-spec(calc_obj_size(integer(), integer()) -> integer()).
calc_obj_size(KSize, DSize) ->
    %% header + footer(padding) + ksize +dsize
    %%        + binary_to_term(Key, AddrId) + binary_to_term(Metadata)
    ?BLEN_HEADER/8 + KSize*3 + DSize + ?LEN_PADDING + 58.

-spec(open(FilePath::string) ->
             {ok, port(), port(), binary()} | {error, any()}).
open(FilePath) ->
    case create_file(FilePath) of
        {ok, WriteHandler} ->
            case open_fun(FilePath) of
                {ok, ReadHandler} ->
                    case file:read_line(ReadHandler) of
                        {ok, Bin} ->
                            {ok, [WriteHandler, ReadHandler, binary:part(Bin, 0, size(Bin) - 1)]};
                        Error ->
                            Error
                    end;
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
-spec(put(atom(), #backend_info{}, #?OBJECT{}) ->
             {ok, integer()} | {error, any()}).
put(MetaDBId, StorageInfo, Object) ->
    put_fun_1(MetaDBId, StorageInfo, Object).


%% @doc Retrieve an object and a metadata from the object-storage
%%
-spec(get(atom(), #backend_info{}, binary()) ->
             {ok, #?METADATA{}, #?OBJECT{}} | {error, any()}).
get(MetaDBId, StorageInfo, Key) ->
    get(MetaDBId, StorageInfo, Key, 0, 0, false).

get(MetaDBId, StorageInfo, Key, StartPos, EndPos, IsStrictCheck) ->
    get_fun(MetaDBId, StorageInfo, Key, StartPos, EndPos, IsStrictCheck).


%% @doc Remove an object and a metadata from the object-storage
%%
-spec(delete(atom(), #backend_info{}, #?OBJECT{}) ->
             ok | {error, any()}).
delete(MetaDBId, StorageInfo, Object) ->
    case put_fun_1(MetaDBId, StorageInfo, Object) of
        {ok, _Checksum} ->
            ok;
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Retrieve a metada from backend_db from the object-storage
%%
-spec(head(atom(), binary()) ->
             {ok, #?METADATA{}} | not_found | {error, any()}).
head(MetaDBId, Key) ->
    case catch leo_backend_db_api:get(MetaDBId, Key) of
        {ok, MetadataBin} ->
            case leo_object_storage_transformer:transform_metadata(
                   binary_to_term(MetadataBin)) of
                {error, Cause} ->
                    {error, Cause};
                Metadata->
                    {ok, term_to_binary(Metadata)}
            end;
        not_found = Cause ->
            Cause;
        {_, Cause} ->
            {error, Cause}
    end.


%% @doc Fetch objects from the object-storage
%%
-spec(fetch(atom(), binary(), function(), pos_integer()|undefined) ->
             ok | {error, any()}).
fetch(MetaDBId, Key, Fun, undefined) ->
    leo_backend_db_api:fetch(MetaDBId, Key, Fun);
fetch(MetaDBId, Key, Fun, MaxKeys) ->
    leo_backend_db_api:fetch(MetaDBId, Key, Fun, MaxKeys).

%% @doc Store metadata and binary
%%
-spec(store(atom(), #backend_info{}, #?METADATA{}, binary()) ->
             ok | {error, any()}).
store(MetaDBId, StorageInfo, Metadata, Bin) ->
    Checksum = leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin)),
    Object_1 = leo_object_storage_transformer:metadata_to_object(Metadata),
    Object_2 = Object_1#?OBJECT{data = Bin,
                                checksum = Checksum},
    case put_fun_1(MetaDBId, StorageInfo, Object_2) of
        {ok, _Checksum} ->
            ok;
        {error, Cause} ->
            {error, Cause}
    end.


%%--------------------------------------------------------------------
%% for TEST
%%--------------------------------------------------------------------
-ifdef(TEST).
%% @doc add a incorrect data to the AVS for making the AVS corrupted
%% @private
-spec(add_incorrect_data(#backend_info{}, binary()) ->
             ok | {error, any()}).
add_incorrect_data(StorageInfo, Data) ->
    #backend_info{write_handler = WriteHandler} = StorageInfo,
    case file:position(WriteHandler, eof) of
        {ok, Offset} ->
            add_incorrect_data(WriteHandler, Offset, Data);
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "add_incorrect_data/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.
-spec(add_incorrect_data(file:io_device(), integer(), binary()) ->
             ok | {error, any()}).
add_incorrect_data(WriteHandler, Offset, Data) ->
    case file:pwrite(WriteHandler, Offset, Data) of
        ok ->
            ok;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "add_incorrect_data/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.
-endif.


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
get_fun(MetaDBId, StorageInfo, Key, StartPos, EndPos, IsStrictCheck) ->
    case catch leo_backend_db_api:get(MetaDBId, Key) of
        {ok, MetadataBin} ->
            Metadata_1 = binary_to_term(MetadataBin),
            Metadata_2 = leo_object_storage_transformer:transform_metadata(Metadata_1),

            case Metadata_2#?METADATA.del of
                ?DEL_FALSE ->
                    get_fun_1(MetaDBId, StorageInfo, Metadata_2,
                              StartPos, EndPos, IsStrictCheck);
                _ ->
                    not_found
            end;
        Error ->
            case Error of
                not_found  ->
                    Error;
                {_, Cause} ->
                    {error, Cause}
            end
    end.


%% @doc When getting invalid positions,
%%      should return an identified status to reply 416 on HTTP
%%      for now dsize = -2 indicate invalid position
%% @private
get_fun_1(_MetaDBId,_StorageInfo, #?METADATA{dsize = DSize} = Metadata,
          StartPos, EndPos,_IsStrictCheck) when StartPos >= DSize orelse
                                                StartPos <  0 orelse
                                                EndPos   >= DSize ->
    Object_1 = leo_object_storage_transformer:metadata_to_object(Metadata),
    Object_2 = Object_1#?OBJECT{data  = <<>>,
                                dsize = -2},
    {ok, Metadata, Object_2};

get_fun_1(_MetaDBId, StorageInfo, #?METADATA{ksize    = KSize,
                                             dsize    = DSize,
                                             offset   = Offset,
                                             cnumber  = 0,
                                             checksum = Checksum} = Metadata,
          StartPos, EndPos, IsStrictCheck) ->
    %% Calculate actual start-point and end-point
    {StartPos_1, EndPos_1} = calc_pos(StartPos, EndPos, DSize),
    Offset_1 = Offset + erlang:round(?BLEN_HEADER/8) + KSize + StartPos_1,
    DSize_1  = EndPos_1 - StartPos_1 + 1,

    %% Retrieve the object
    #backend_info{read_handler = ReadHandler} = StorageInfo,
    Object_1 = leo_object_storage_transformer:metadata_to_object(Metadata),

    case file:pread(ReadHandler, Offset_1, DSize_1) of
        {ok, Bin} when IsStrictCheck == true,
                       StartPos == 0,
                       EndPos   == 0 ->
            case leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin)) of
                Checksum ->
                    {ok, Metadata, Object_1#?OBJECT{data  = Bin,
                                                    dsize = DSize_1}};
                _ ->
                    {error, invalid_object}
            end;
        {ok, Bin} ->
            {ok, Metadata, Object_1#?OBJECT{data  = Bin,
                                            dsize = DSize_1}};
        eof = Cause ->
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "get_fun/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end;

%% For parent of chunked object
get_fun_1(_MetaDBId,_StorageInfo, #?METADATA{} = Metadata, _,_,_) ->
    Object = leo_object_storage_transformer:metadata_to_object(Metadata),
    {ok, Metadata, Object#?OBJECT{data  = <<>>,
                                  dsize = 0}}.


%% @doc Retrieve start-position and endposition of an object
%% @private
calc_pos(_StartPos, EndPos, DSize) when EndPos < 0 ->
    StartPos_1 = DSize + EndPos,
    EndPos_1   = DSize - 1,
    {StartPos_1, EndPos_1};
calc_pos(StartPos, 0, DSize) ->
    {StartPos, DSize - 1};
calc_pos(StartPos, EndPos, _DSize) ->
    {StartPos, EndPos}.


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
create_needle(#?OBJECT{addr_id    = AddrId,
                       key        = Key,
                       ksize      = KSize,
                       dsize      = DSize,
                       msize      = MSize,
                       meta       = MBin,
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
    DataBin = case (MSize < 1) of
                  true  -> << Key/binary, Body/binary, Padding/binary >>;
                  false -> << Key/binary, Body/binary, MBin/binary, Padding/binary >>
              end,
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
                 DataBin/binary >>,
    Needle.



%% @doc Insert an object into the object-storage
%% @private
put_fun_1(MetaDBId, StorageInfo, Object) ->
    #backend_info{write_handler = ObjectStorageWriteHandler} = StorageInfo,

    case file:position(ObjectStorageWriteHandler, eof) of
        {ok, Offset} ->
            put_fun_2(MetaDBId, StorageInfo, Object#?OBJECT{offset = Offset});
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "put_fun_1/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.

put_fun_2(MetaDBId, StorageInfo, #?OBJECT{key      = Key,
                                          ksize    = KSize,
                                          data     = Bin,
                                          checksum = Checksum} = Object) ->
    KSize     = byte_size(Key),
    Checksum1 = case Checksum of
                    0 -> leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin));
                    _ -> Checksum
                end,
    Needle = create_needle(Object#?OBJECT{ksize    = KSize,
                                          checksum = Checksum1}),
    Metadata = leo_object_storage_transformer:object_to_metadata(Object),
    put_fun_3(MetaDBId, StorageInfo, Needle, Metadata).

put_fun_3(MetaDBId, StorageInfo, Needle, #?METADATA{key      = Key,
                                                    addr_id  = AddrId,
                                                    offset   = Offset,
                                                    checksum = Checksum} = Meta) ->
    #backend_info{write_handler       = WriteHandler,
                  avs_version_bin_cur = AVSVsnBin} = StorageInfo,

    Key4BackendDB = ?gen_backend_key(AVSVsnBin, AddrId, Key),
    case file:pwrite(WriteHandler, Offset, Needle) of
        ok ->
            case catch leo_backend_db_api:put(MetaDBId,
                                              Key4BackendDB,
                                              term_to_binary(Meta)) of
                ok ->
                    {ok, Checksum};
                {'EXIT', Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "put_fun_3/2"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause};
                {error, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "put_fun_3/2"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING}, {function, "put_fun_3/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.

%%--------------------------------------------------------------------
%% COMPACTION FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Insert an object into the object-container when compacting
%% @private
-spec(compact_put(pid(), #?METADATA{}, binary(), binary()) ->
             ok | {error, any()}).
compact_put(WriteHandler, Metadata, KeyBin, BodyBin) ->
    case file:position(WriteHandler, eof) of
        {ok, Offset} ->
            Metadata_1 = leo_object_storage_transformer: transform_metadata(Metadata),
            Object = leo_object_storage_transformer:metadata_to_object(Metadata_1),
            Needle = create_needle(Object#?OBJECT{key  = KeyBin,
                                                  data = BodyBin}),
            case catch file:pwrite(WriteHandler, Offset, Needle) of
                ok ->
                    {ok, Offset};
                {'EXIT', Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "compact_put/4"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause};
                {error, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "compact_put/4"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "compact_put/4"},
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
                    Cause = ?ERROR_DATA_SIZE_DID_NOT_MATCH,
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING}, {function, "compact_get/2"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end;
        eof = Cause ->
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "compact_get/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.

%% @private
-spec(compact_get(pid(), integer(), integer(), binary()) ->
             ok | {error, any()}).
compact_get(ReadHandler, Offset, HeaderSize, HeaderBin) ->
    case leo_object_storage_transformer:header_bin_to_metadata(HeaderBin) of
        {error, Cause} ->
            {error, Cause};
        Metadata ->
            compact_get(Metadata, ReadHandler,
                        Offset, HeaderSize, HeaderBin)
    end.

%% @private
compact_get(#?METADATA{ksize = KSize,
                       dsize = DSize,
                       msize = MSize,
                       cnumber = CNum} = Metadata, ReadHandler,
            Offset, HeaderSize, HeaderBin) ->
    DSize4Read = case (CNum > 0) of
                     true  -> 0;
                     false -> DSize
                 end,
    RemainSize = (KSize + DSize4Read
                  + MSize + ?LEN_PADDING),

    case (RemainSize > ?MAX_DATABLOCK_SIZE) of
        true ->
            Cause = ?ERROR_INVALID_DATA,
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "compact_get/4"},
                                    {line, ?LINE},
                                    {body, "Data size too large"}]),
            {error, Cause};
        false ->
            try
                case file:pread(ReadHandler, Offset + HeaderSize, RemainSize) of
                    {ok, RemainBin} ->
                        case byte_size(RemainBin) of
                            RemainSize ->
                                compact_get_1(HeaderBin, Metadata,
                                              DSize4Read, RemainBin,
                                              Offset + HeaderSize + RemainSize);
                            _ ->
                                Cause = ?ERROR_DATA_SIZE_DID_NOT_MATCH,
                                error_logger:error_msg("~p,~p,~p,~p~n",
                                                       [{module, ?MODULE_STRING},
                                                        {function, "compact_get/4"},
                                                        {line, ?LINE},
                                                        {body, Cause}]),
                                {error, Cause}
                        end;


                    eof = Cause ->
                        {error, Cause};
                    {error, Cause} ->
                        error_logger:error_msg("~p,~p,~p,~p~n",
                                               [{module, ?MODULE_STRING},
                                                {function, "compact_get/4"},
                                                {line, ?LINE}, {body, Cause}]),
                        {error, Cause}
                end
            catch
                _:Reason ->
                    {error, Reason}
            end
    end.

%% @private
compact_get_1(_HeaderBin, #?METADATA{ksize = 0},_DSize,_Bin,_TotalSize) ->
    {error, ?ERROR_DATA_SIZE_DID_NOT_MATCH};
compact_get_1(HeaderBin, #?METADATA{ksize = KSize,
                                    msize = 0
                                   } = Metadata, DSize, Bin, TotalSize) ->
    << KeyBin:KSize/binary,
       BodyBin:DSize/binary,
       _Footer/binary>> = Bin,
    compact_get_2(HeaderBin, Metadata, KeyBin, BodyBin, <<>>, TotalSize);
compact_get_1(HeaderBin, #?METADATA{ksize = KSize,
                                    msize = MSize
                                   } = Metadata, DSize, Bin, TotalSize) ->
    << KeyBin:KSize/binary,
       BodyBin:DSize/binary,
       CMetaBin:MSize/binary,
       _Footer/binary>> = Bin,
    compact_get_2(HeaderBin, Metadata, KeyBin, BodyBin, CMetaBin, TotalSize).

%% @private
compact_get_2(HeaderBin, Metadata, KeyBin, BodyBin, CMetaBin, TotalSize) ->
    Checksum = Metadata#?METADATA.checksum,
    Metadata_1 = leo_object_storage_transformer:cmeta_bin_into_metadata(
                   CMetaBin, Metadata),
    case leo_hex:raw_binary_to_integer(crypto:hash(md5, BodyBin)) of
        Checksum ->
            {ok, Metadata_1#?METADATA{key = KeyBin},
             [HeaderBin, KeyBin, BodyBin, TotalSize]};
        _Other ->
            Cause = ?ERROR_INVALID_DATA,
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "compact_get_2/4"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.

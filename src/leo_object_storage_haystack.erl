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
%% Leo Object Storage - Haystack.
%%
%% @doc The object storage implementation - haystack
%% @reference https://github.com/leo-project/leo_object_storage/blob/master/src/leo_object_storage_haystack.erl
%% @end
%%======================================================================
-module(leo_object_storage_haystack).

-author('Yosuke Hara').
-author('Yoshiyuki Kanno').

-include("leo_object_storage.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([open/1, open/2, close/2,
         put/3, get/3, get/6, delete/3,
         head/2, head/3,
         fetch/4]).

-export([head_with_calc_md5/4]).

-export([calc_obj_size/1,
         calc_obj_size/2,
         put_obj_to_new_cntnr/4,
         get_obj_for_new_cntnr/1,
         get_obj_for_new_cntnr/3,
         get_eof_offset/1
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
-spec(calc_obj_size(MetadataOrObject) ->
             non_neg_integer() when MetadataOrObject::#?METADATA{}|#?OBJECT{}).
calc_obj_size(#?METADATA{ksize = KSize,
                         dsize = DSize,
                         cnumber = 0}) ->
    calc_obj_size(KSize, DSize);
calc_obj_size(#?METADATA{ksize = KSize}) ->
    calc_obj_size(KSize, 0);

calc_obj_size(#?OBJECT{key  = Key,
                       dsize = DSize,
                       cnumber = 0}) ->
    KSize = byte_size(Key),
    calc_obj_size(KSize, DSize);
calc_obj_size(#?OBJECT{key  = Key}) ->
    KSize = byte_size(Key),
    calc_obj_size(KSize, 0).

-spec(calc_obj_size(KSize, DSize) ->
             non_neg_integer() when KSize::non_neg_integer(),
                                    DSize::non_neg_integer()).
calc_obj_size(KSize, DSize) ->
    erlang:round(?BLEN_HEADER/8 + KSize + DSize + ?LEN_PADDING).


%% @doc Open a new or existing datastore
-spec(open(FilePath) ->
             {ok, port(), port(), binary()} | {error, any()} when FilePath::string()).
open(FilePath) ->
    %% open(FilePath, read_and_write).
    open(FilePath, ?OBJ_PRV_READ_WRITE).

-spec(open(FilePath, Option) ->
             {ok, port(), port(), binary()} |
             {error, any()} when FilePath::string(),
                                 Option::read_and_write|read|write).
open(FilePath, ?OBJ_PRV_READ_WRITE) ->
    case create_file(FilePath) of
        {ok, WriteHandler} ->
            case open_read_handler(FilePath) of
                {ok, [ReadHandler, Bin]} ->
                    {ok, [WriteHandler, ReadHandler, Bin]};
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
open(FilePath, ?OBJ_PRV_READ_ONLY) ->
    case open_read_handler(FilePath) of
        {ok, [ReadHandler, Bin]} ->
            {ok, [undefined, ReadHandler, Bin]};
        Error ->
            Error
    end;
open(FilePath, ?OBJ_PRV_WRITE_ONLY) ->
    case create_file(FilePath) of
        {ok, WriteHandler} ->
            case open_read_handler(FilePath) of
                {ok, [ReadHandler, Bin]} ->
                    ok = close(undefined, ReadHandler),
                    {ok, [WriteHandler, undefined, Bin]};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% @private
open_read_handler(FilePath) ->
    case open_fun(FilePath) of
        {ok, ReadHandler} ->
            case file:read_line(ReadHandler) of
                {ok, Bin} ->
                    {ok, [ReadHandler,
                          binary:part(Bin, 0, size(Bin) - 1)]};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.


%% @doc Close file handlers.
%%
-spec(close(WriteHandler, ReadHandler) ->
             ok when WriteHandler::port()|any(),
                     ReadHandler::port()|any()).
close(WriteHandler, ReadHandler) ->
    case WriteHandler of
        undefined ->
            void;
        _ ->
            catch file:close(WriteHandler)
    end,
    case ReadHandler of
        undefined -> void;
        _ ->
            catch file:close(ReadHandler)
    end,
    ok.


%% @doc Insert an object and a metadata into the object-storage
%%
-spec(put(MetaDBId, StorageInfo, Object) ->
             {ok, integer()} | {error, any()} when MetaDBId::atom(),
                                                   StorageInfo::#backend_info{},
                                                   Object::#?OBJECT{}).
put(MetaDBId, StorageInfo, Object) ->
    put_fun_1(MetaDBId, StorageInfo, Object).


%% @doc Retrieve an object and a metadata
%%
-spec(get(MetaDBId, StorageInfo, Key) ->
             {ok, #?METADATA{}, #?OBJECT{}} |
             {error, any()} when MetaDBId::atom(),
                                 StorageInfo::#backend_info{},
                                 Key::binary()).
get(MetaDBId, StorageInfo, Key) ->
    get(MetaDBId, StorageInfo, Key, -1, -1, false).

%% @doc Retrieve part of an object and a metadata
%%
-spec(get(MetaDBId, StorageInfo, Key, StartPos, EndPos, IsStrictCheck) ->
             {ok, #?METADATA{}, #?OBJECT{}} |
             {error, any()} when MetaDBId::atom(),
                                 StorageInfo::#backend_info{},
                                 Key::binary(),
                                 StartPos::non_neg_integer(),
                                 EndPos::non_neg_integer(),
                                 IsStrictCheck::boolean()).
get(MetaDBId, StorageInfo, Key, StartPos, EndPos, IsStrictCheck) ->
    get_fun(MetaDBId, StorageInfo, Key, StartPos, EndPos, IsStrictCheck).


%% @doc Remove an object and a metadata from the object-storage
%%
-spec(delete(MetaDBId, StorageInfo, Object) ->
             ok | {error, any()} when MetaDBId::atom(),
                                      StorageInfo::#backend_info{},
                                      Object::#?OBJECT{}).
delete(MetaDBId, StorageInfo, Object) ->
    case put_fun_1(MetaDBId, StorageInfo, Object) of
        {ok, _Checksum} ->
            ok;
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Retrieve a metada from backend_db from the object-storage
%%
-spec(head(MetaDBId, Key) ->
             {ok, binary()} | not_found | {error, any()} when MetaDBId::atom(),
                                                              Key::binary()).
head(MetaDBId, Key) ->
    case catch leo_backend_db_api:get(MetaDBId, Key) of
        {ok, MetadataBin} ->
            case leo_object_storage_transformer:transform_metadata(
                   binary_to_term(MetadataBin)) of
                {error, Cause} ->
                    {error, Cause};
                Metadata ->
                    {ok, term_to_binary(Metadata)}
            end;
        not_found = Cause ->
            Cause;
        {_, Cause} ->
            {error, Cause}
    end.

-spec(head(MetaDBId, StorageInfo, Key) ->
             {ok, binary()} | not_found | {error, any()} when MetaDBId::atom(),
                                                              StorageInfo::#backend_info{},
                                                              Key::binary()).
head(MetaDBId, StorageInfo, Key) ->
    case catch leo_backend_db_api:get(MetaDBId, Key) of
        {ok, MetadataBin} ->
            case leo_object_storage_transformer:transform_metadata(
                   binary_to_term(MetadataBin)) of
                {error, Cause} ->
                    {error, Cause};
                #?METADATA{msize = MSize} = Metadata ->
                    case MSize > 0 of
                        true ->
                            case get_fun_1(MetaDBId, StorageInfo, Metadata,
                                           -1, -1, false) of
                                {ok, Metadata_1,_Object} ->
                                    {ok, term_to_binary(Metadata_1)};
                                {error, Cause} ->
                                    {error, Cause}
                            end;
                        false ->
                            {ok, term_to_binary(Metadata)}
                    end
            end;
        not_found = Cause ->
            Cause;
        {_, Cause} ->
            {error, Cause}
    end.



%% @doc Retrieve a metada/data from backend_db/object-storage
%%      AND calc MD5 based on the body data
%%
-spec(head_with_calc_md5(MetaDBId, StorageInfo, Key, MD5Context) ->
             {ok, #?METADATA{}} |
             not_found |
             {error, any()} when MetaDBId::atom(),
                                 StorageInfo::#backend_info{},
                                 Key:: binary(),
                                 MD5Context::any()).
head_with_calc_md5(MetaDBId, StorageInfo, Key, MD5Context) ->
    case get_fun(MetaDBId, StorageInfo, Key, -1, -1, false) of
        {ok, #?METADATA{cnumber = 0} = Meta, #?OBJECT{data = Bin}} ->
            %% calc MD5
            {ok, Meta, crypto:hash_update(MD5Context, Bin)};
        {ok, #?METADATA{cnumber = _N} = Meta, _Object} ->
            %% Not calc due to existing some grand childs
            {ok, Meta, MD5Context};
        Other -> Other
    end.


%% @doc Fetch objects from the object-storage
%%
-spec(fetch(MetaDBId, Key, Fun, MaxKeys) ->
             {ok, list()} |
             not_found |
             {error, any()} when MetaDBId::atom(),
                                 Key::binary(),
                                 Fun::function(),
                                 MaxKeys::pos_integer()|undefined).
fetch(MetaDBId, Key, Fun, undefined) ->
    leo_backend_db_api:fetch(MetaDBId, Key, Fun);
fetch(MetaDBId, Key, Fun, MaxKeys) ->
    leo_backend_db_api:fetch(MetaDBId, Key, Fun, MaxKeys).


%% @doc Get the EOF offset
-spec(get_eof_offset(#backend_info{}) ->
             {ok, non_neg_integer()} | {error, any()}).
get_eof_offset(#backend_info{write_handler = WriteHandler}) ->
    case file:position(WriteHandler, eof) of
        {ok, Offset} ->
            {ok, Offset};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "get_eof_offset/1"},
                                    {line, ?LINE}, {body, Cause}]),
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
                                   [{module, ?MODULE_STRING},
                                    {function, "add_incorrect_data/2"},
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
                                   [{module, ?MODULE_STRING},
                                    {function, "add_incorrect_data/2"},
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
            case catch filelib:file_size(FilePath) of
                {'EXIT', Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "create_file/1"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause};
                0 ->
                    put_super_block(PutFileHandler);
                _FileSize ->
                    {ok, PutFileHandler}
            end;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "create_file/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause};
        {'EXIT', Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "create_file/1"},
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

    case catch filelib:is_file(FilePath) of
        {'EXIT', Cause} ->
            {error, Cause};
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



%% @private
get_fun_1(_MetaDBId,_StorageInfo, #?METADATA{dsize = 0,
                                             msize = 0} = Metadata,
          _StartPos,_EndPos,_IsStrictCheck) ->
    Object_1 = leo_object_storage_transformer:metadata_to_object(Metadata),
    Object_2 = Object_1#?OBJECT{data = <<>>},
    {ok, Metadata, Object_2};

get_fun_1(_MetaDBId,_StorageInfo, #?METADATA{dsize = DSize} = Metadata,
          StartPos, EndPos,_IsStrictCheck) when StartPos >= DSize orelse
                                                StartPos <  -1 orelse
                                                EndPos   >= DSize ->
    %% When getting invalid positions,
    %%   should return an identified status to reply 416 on HTTP
    %%   for now dsize = -2 indicate invalid position
    Object_1 = leo_object_storage_transformer:metadata_to_object(Metadata),
    Object_2 = Object_1#?OBJECT{data  = <<>>,
                                dsize = -2},
    {ok, Metadata, Object_2};

get_fun_1(_MetaDBId, StorageInfo, #?METADATA{cnumber = 0,
                                             dsize = DSize,
                                             msize = MSize} = Metadata, -1, -1, IsStrictCheck) ->
    StartPos = 0,
    EndPos = DSize + MSize - 1,
    get_fun_2(StorageInfo, Metadata, StartPos, EndPos, IsStrictCheck, false);

get_fun_1(_MetaDBId, StorageInfo, #?METADATA{cnumber = 0,
                                             dsize = DSize} = Metadata,
          StartPos, EndPos, IsStrictCheck) ->
    %% Calculate actual start-point and end-point
    {StartPos_1, EndPos_1} = calc_pos(StartPos, EndPos, DSize),
    get_fun_2(StorageInfo, Metadata, StartPos_1, EndPos_1, IsStrictCheck, true);

%% @doc For parent of chunked object
%% @private
get_fun_1(_MetaDBId,_StorageInfo, #?METADATA{} = Metadata, _,_,_) ->
    Object = leo_object_storage_transformer:metadata_to_object(Metadata),
    {ok, Metadata, Object#?OBJECT{data  = <<>>,
                                  dsize = 0}}.

%% @private
get_fun_2(StorageInfo, #?METADATA{ksize = KSize,
                                  msize = MSize,
                                  offset = Offset,
                                  checksum = Checksum} = Metadata,
          StartPos, EndPos, IsStrictCheck, IsRangeQuery) ->
    %% Retrieve the object
    Offset_1 = Offset + erlang:round(?BLEN_HEADER/8) + KSize + StartPos,
    Length = EndPos - StartPos + 1,
    DSize_1 = Length - MSize,
    #backend_info{read_handler = ReadHandler} = StorageInfo,

    case leo_file:pread(ReadHandler, Offset_1, Length) of
        {ok, Bin} when IsStrictCheck == true,
                       IsRangeQuery  == false ->
            {ok, {Bin_1, CMetaBin}} = get_body_and_cmeta(Bin, Length, MSize),

            case leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin_1)) of
                Checksum ->
                    Metadata_1 = Metadata#?METADATA{meta = CMetaBin},
                    {ok, Metadata_1,
                     leo_object_storage_transformer:metadata_to_object(Bin_1, Metadata_1)};
                _ ->
                    {error, invalid_object}
            end;
        {ok, Bin} ->
            case IsRangeQuery of
                true ->
                    {ok, Metadata,
                     leo_object_storage_transformer:metadata_to_object(Bin, Metadata)};
                false ->
                    {ok, {Bin_1, CMetaBin}} = get_body_and_cmeta(Bin, Length, MSize),
                    Metadata_1 = Metadata#?METADATA{meta = CMetaBin},
                    {ok, Metadata_1,
                     leo_object_storage_transformer:metadata_to_object(Bin_1, Metadata_1)}
            end;
        eof = Cause ->
            {error, Cause};
        {error, Cause} ->
            error_logger:error_msg(
              "~p,~p,~p,~p~n",
              [{module, ?MODULE_STRING},
               {function, "get_fun_2/4"},
               {line, ?LINE}, [{offset, Offset_1},
                               {dsize, DSize_1},
                               {body, Cause}]]),
            case Cause of
                unexpected_len ->
                    {error, {abort, Cause}};
                _ ->
                    {error, Cause}
            end
    end.


%% @private
get_body_and_cmeta(Bin, Length, MSize) ->
    BodyLen = Length - MSize,
    Body = binary:part(Bin, 0, BodyLen),
    CMetaBin = case MSize > 0 of
                   true ->
                       binary:part(Bin, BodyLen, MSize);
                   _ ->
                       <<>>
               end,
    {ok, {Body, CMetaBin}}.


%% @private
calc_pos(_StartPos, EndPos, ObjectSize) when EndPos < 0 ->
    NewStartPos = ObjectSize + EndPos,
    NewEndPos   = ObjectSize - 1,
    {NewStartPos, NewEndPos};
calc_pos(StartPos, 0, ObjectSize) when StartPos > 0 ->
    {StartPos, ObjectSize - 1};
calc_pos(StartPos, EndPos, _ObjectSize) ->
    {StartPos, EndPos}.


%% @doc Insert a super-block into an object container (*.avs)
%% @private
put_super_block(ObjectStorageWriteHandler) ->
    case file:pwrite(ObjectStorageWriteHandler, 0, ?AVS_SUPER_BLOCK) of
        ok ->
            {ok, ObjectStorageWriteHandler};
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "put_super_block/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Create a needle
%% @private
create_needle(#?OBJECT{addr_id = AddrId,
                       key = Key,
                       ksize = KSize,
                       dsize = DSize,
                       msize = MSize,
                       meta = MBin,
                       csize = CSize,
                       cnumber = CNum,
                       cindex = CIndex,
                       data = Body,
                       clock = Clock,
                       offset = Offset,
                       timestamp = Timestamp,
                       checksum = Checksum,
                       del = Del}) ->
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
                                   [{module, ?MODULE_STRING},
                                    {function, "put_fun_1/1"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.

%% @private
put_fun_2(MetaDBId, StorageInfo, #?OBJECT{key = Key,
                                          data = Bin,
                                          checksum = Checksum,
                                          timestamp = Timestamp,
                                          del = DelFlag} = Object) ->
    Checksum_1 = case Checksum of
                     0 -> leo_hex:raw_binary_to_integer(crypto:hash(md5, Bin));
                     _ -> Checksum
                 end,
    Object_1 = Object#?OBJECT{ksize = byte_size(Key),
                              checksum = Checksum_1},
    Object_2 = case Timestamp =< 0 of
                   true ->
                       error_logger:error_msg("~p,~p,~p,~p~n",
                                              [{module, ?MODULE_STRING},
                                               {function, "put_fun_2/3"},
                                               {line, ?LINE}, {body, [{key, Key},
                                                                      {del, DelFlag},
                                                                      {timestamp, Timestamp},
                                                                      {cause, "Not set timestamp correctly"}
                                                                     ]}]),
                       Object_1#?OBJECT{timestamp = leo_date:now()};
                   false ->
                       Object_1
               end,
    Needle = create_needle(Object_2),
    Metadata = leo_object_storage_transformer:object_to_metadata(Object_2),
    put_fun_3(MetaDBId, StorageInfo, Needle, Metadata).

%% @private
put_fun_3(MetaDBId, StorageInfo, Needle, #?METADATA{key      = Key,
                                                    addr_id  = AddrId,
                                                    offset   = Offset,
                                                    checksum = Checksum} = Meta) ->
    #backend_info{write_handler = WriteHandler,
                  avs_ver_cur   = AVSVsnBin} = StorageInfo,

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
                                           [{module, ?MODULE_STRING},
                                            {function, "put_fun_3/2"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause};
                {error, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "put_fun_3/2"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "put_fun_3/2"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.

%%--------------------------------------------------------------------
%% COMPACTION FUNCTIONS
%%--------------------------------------------------------------------
%% @doc Insert an object into the object-container when compacting
%%
-spec(put_obj_to_new_cntnr(pid(), #?METADATA{}, binary(), binary()) ->
             ok | {error, any()}).
put_obj_to_new_cntnr(WriteHandler, Metadata, KeyBin, BodyBin) ->
    case file:position(WriteHandler, eof) of
        {ok, Offset} ->
            Metadata_1 = leo_object_storage_transformer:transform_metadata(Metadata),
            Object = leo_object_storage_transformer:metadata_to_object(Metadata_1),
            Needle = create_needle(Object#?OBJECT{key  = KeyBin,
                                                  data = BodyBin,
                                                  offset = Offset}),
            case catch file:pwrite(WriteHandler, Offset, Needle) of
                ok ->
                    {ok, Offset};
                {'EXIT', Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "put_obj_to_new_cntnr/4"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause};
                {error, Cause} ->
                    error_logger:error_msg("~p,~p,~p,~p~n",
                                           [{module, ?MODULE_STRING},
                                            {function, "put_obj_to_new_cntnr/4"},
                                            {line, ?LINE}, {body, Cause}]),
                    {error, Cause}
            end;
        {error, Cause} ->
            error_logger:error_msg("~p,~p,~p,~p~n",
                                   [{module, ?MODULE_STRING},
                                    {function, "put_obj_to_new_cntnr/4"},
                                    {line, ?LINE}, {body, Cause}]),
            {error, Cause}
    end.


%% @doc Retrieve a file from object-container when compacting.
%%
-spec(get_obj_for_new_cntnr(pid()) ->
             ok | {error, any()}).
get_obj_for_new_cntnr(ReadHandler) ->
    get_obj_for_new_cntnr(ReadHandler, byte_size(?AVS_SUPER_BLOCK), #compaction_skip_garbage{}).

-spec(get_obj_for_new_cntnr(pid(), integer(), #compaction_skip_garbage{}) ->
             ok | {error, any()}).
get_obj_for_new_cntnr(ReadHandler, Offset, #compaction_skip_garbage{
                                              is_skipping = IsSkipping,
                                              is_close_eof = IsCloseEOF} = _SkipInfo)
                                            when IsSkipping == false orelse
                                                 IsCloseEOF == true ->
    HeaderSize = erlang:round(?BLEN_HEADER/8),

    case leo_file:pread(ReadHandler, Offset, HeaderSize) of
        {ok, HeaderBin} ->
            get_obj_for_new_cntnr(ReadHandler, Offset, HeaderSize, HeaderBin);
        eof = Cause ->
            {error, Cause};
        {error, Cause} ->
            case Cause of
                unexpected_len ->
                    error_logger:error_msg(
                      "~p,~p,~p,~p~n",
                      [{module, ?MODULE_STRING},
                       {function, "get_obj_for_new_cntnr/2"},
                       {line, ?LINE}, [{offset, Offset},
                                       {header_size, HeaderSize},
                                       {body, Cause}]]),
                    %% no more records - https://github.com/leo-project/leofs/issues/523
                    %% enable the caller to handle the return value as EOF
                    {error, eof};
                _ ->
                    {error, Cause}
            end
    end;
%% @doc Used while skipping a garbage block to reduce # of file:pread(s)
%%
get_obj_for_new_cntnr(ReadHandler, Offset,
                      #compaction_skip_garbage{is_skipping = true,
                                               buf = Buf} = SkipInfo)
                      when byte_size(Buf) >= erlang:round(?BLEN_HEADER/8) ->
    HeaderSize = erlang:round(?BLEN_HEADER/8),
    <<HeaderBin:HeaderSize/binary, _/binary>> = Buf,
    case get_obj_for_new_cntnr(ReadHandler, Offset, HeaderSize, HeaderBin) of
        {ok, Meta, [H, K, B, T]} ->
            %% back to the normal exec path
            {ok, Meta, [H, K, B, T]};
        {error, Cause} ->
            %% keep skipiping
            <<_Skip:8, Remain/binary>> = Buf,
            {skip, SkipInfo#compaction_skip_garbage{buf = Remain}, Cause}
    end;
get_obj_for_new_cntnr(ReadHandler, Offset,
                      #compaction_skip_garbage{is_skipping = true,
                                               read_pos = 0} = SkipInfo) ->
    get_obj_for_new_cntnr(ReadHandler, Offset,
                          SkipInfo#compaction_skip_garbage{read_pos = Offset});
get_obj_for_new_cntnr(ReadHandler, Offset,
                      #compaction_skip_garbage{is_skipping = true,
                                               buf = Buf,
                                               read_pos = ReadPos,
                                               prefetch_size = PS} = SkipInfo) ->
    ReadLen = PS - byte_size(Buf),
    case leo_file:pread(ReadHandler, ReadPos, ReadLen) of
        {ok, ReadBin} ->
            get_obj_for_new_cntnr(ReadHandler, Offset, SkipInfo#compaction_skip_garbage{
                                                         read_pos = ReadPos + ReadLen,
                                                         buf = <<Buf/binary, ReadBin/binary>>});
        eof = Cause ->
            {error, Cause};
        {error, Cause} ->
            case Cause  of
                unexpected_len ->
                    %% close EOF
                    %% get the exec path back to the normal
                    get_obj_for_new_cntnr(ReadHandler, Offset, SkipInfo#compaction_skip_garbage{
                                                                         is_close_eof = true
                                                                       });
                _ ->
                    {skip, SkipInfo, Cause}
            end
    end.

%% @private
-spec(get_obj_for_new_cntnr(ReadHandler, Offset, HeaderSize, HeaderBin) ->
             {ok, Metadata, [binary()]} | {error, any()}
                 when ReadHandler::pid(),
                      Offset::integer(),
                      HeaderSize::integer(),
                      HeaderBin::binary(),
                      Metadata::#?METADATA{}
                                ).
get_obj_for_new_cntnr(ReadHandler, Offset, HeaderSize, HeaderBin) ->
    case leo_object_storage_transformer:header_bin_to_metadata(HeaderBin) of
        {error, Cause} ->
            {error, Cause};
        Metadata ->
            get_obj_for_new_cntnr(Metadata, ReadHandler,
                                  Offset, HeaderSize, HeaderBin)
    end.

%% @private
get_obj_for_new_cntnr(#?METADATA{ksize = KSize,
                                 dsize = DSize,
                                 msize = MSize,
                                 cnumber = CNum} = Metadata, ReadHandler,
                      Offset, HeaderSize, HeaderBin) ->
    %% 'cnum > 0' means a parent of a large size object
    DSize4Read = case (CNum > 0) of
                     true ->
                         0;
                     false ->
                         DSize
                 end,
    RemainSize = (KSize + DSize4Read + MSize + ?LEN_PADDING),

    case (RemainSize > ?MAX_DATABLOCK_SIZE) of
        true ->
            {error, {?LINE,?ERROR_INVALID_DATA}};
        false ->
            try
                case leo_file:pread(ReadHandler, Offset + HeaderSize, RemainSize) of
                    {ok, RemainBin} ->
                        TotalSize = Offset + HeaderSize + RemainSize,
                        get_obj_for_new_cntnr_1(HeaderBin, Metadata#?METADATA{offset = Offset},
                                                DSize4Read, RemainBin, TotalSize);
                    eof = Cause ->
                        {error, Cause};
                    {error, Cause} ->
                        case Cause of
                            unexpected_len ->
                                error_logger:error_msg(
                                  "~p,~p,~p,~p~n",
                                  [{module, ?MODULE_STRING},
                                   {function, "get_obj_for_new_cntnr/5"},
                                   {line, ?LINE}, [{offset, Offset + HeaderSize},
                                                   {length, RemainSize},
                                                   {body, Cause}]]),
                                {error, unexpected_len};
                            _ ->
                                {error, Cause}
                        end
                end
            catch
                _:Reason ->
                    {error, {?LINE, Reason}}
            end
    end.

%% @private
get_obj_for_new_cntnr_1(_HeaderBin, #?METADATA{ksize = 0},_DSize,_Bin,_TotalSize) ->
    {error, {?LINE, ?ERROR_DATA_SIZE_DID_NOT_MATCH}};
get_obj_for_new_cntnr_1(HeaderBin, #?METADATA{ksize = KSize,
                                              msize = MSize} = Metadata,
                        DSize, Bin, TotalSize) ->
    << KeyBin:KSize/binary,
       BodyBin:DSize/binary,
       CMetaBin:MSize/binary,
       _Footer/binary>> = Bin,
    get_obj_for_new_cntnr_2(HeaderBin, Metadata,
                            KeyBin, BodyBin, CMetaBin, TotalSize).

%% @private
get_obj_for_new_cntnr_2(HeaderBin, Metadata,
                        KeyBin, BodyBin, CMetaBin, TotalSize) ->
    Checksum = Metadata#?METADATA.checksum,
    Checksum_1 = leo_hex:raw_binary_to_integer(crypto:hash(md5, BodyBin)),
    Metadata_1 = Metadata#?METADATA{key = KeyBin},

    IsLargeObjParent = (Metadata#?METADATA.ksize =< ?MAX_KEY_SIZE andalso
                        Metadata#?METADATA.timestamp > 0 andalso
                        Metadata#?METADATA.cnumber   > 0 andalso
                        Metadata#?METADATA.cindex   == 0 andalso
                        Metadata#?METADATA.del      == 0 andalso
                        Checksum_1 == ?MD5_EMPTY_BIN),

    case (Checksum == Checksum_1 orelse
          IsLargeObjParent == true) of
        true ->
            {ok, Metadata_1#?METADATA{msize = byte_size(CMetaBin),
                                      meta = CMetaBin},
             [HeaderBin, KeyBin, BodyBin, TotalSize]};
        false ->
            Reason = ?ERROR_INVALID_DATA,
            {error, {?LINE, Reason}}
    end.

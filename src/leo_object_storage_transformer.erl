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
%% @doc The object storage's data transformer
%% @reference https://github.com/leo-project/leo_object_storage/blob/master/src/leo_object_storage_transformer.erl
%% @end
%%======================================================================
-module(leo_object_storage_transformer).
-author('Yosuke Hara').

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([metadata_to_object/1,
         metadata_to_object/2,
         object_to_metadata/1,
         transform_metadata/1,
         header_bin_to_metadata/1,
         cmeta_bin_into_metadata/2,
         get_udm_from_cmeta_bin/1,
         list_to_cmeta_bin/1
        ]).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
%% @doc Transform from a metadata to an object
-spec(metadata_to_object(Metadata) ->
             #?OBJECT{} | {error, invaid_record} when Metadata::#metadata{} | #metadata_1{} | #?METADATA{}).
metadata_to_object(#metadata{key = Key,
                             addr_id = AddrId,
                             ksize = KSize,
                             dsize = DSize,
                             msize = MSize,
                             csize = CSize,
                             cnumber = CNum,
                             cindex = CIndex,
                             offset = Offset,
                             clock = Clock,
                             timestamp = Timestamp,
                             checksum = Checksum,
                             ring_hash = RingHash,
                             del = Del}) ->
    #?OBJECT{key = Key,
             addr_id = AddrId,
             ksize = KSize,
             dsize = DSize,
             msize = MSize,
             csize = CSize,
             cnumber = CNum,
             cindex = CIndex,
             offset = Offset,
             clock = Clock,
             timestamp = Timestamp,
             checksum = Checksum,
             ring_hash = RingHash,
             del = Del};
metadata_to_object(#metadata_1{} = Metadata) ->
    #metadata_1{key = Key,
                addr_id = AddrId,
                ksize = KSize,
                dsize = DSize,
                msize = MSize,
                csize = CSize,
                cnumber = CNum,
                cindex = CIndex,
                offset = Offset,
                clock = Clock,
                timestamp = Timestamp,
                checksum = Checksum,
                ring_hash = RingHash,
                cluster_id = ClusterId,
                num_of_replicas = NumOfReplicas,
                ver = Ver,
                del = Del} = Metadata,
    #?OBJECT{key = Key,
             addr_id = AddrId,
             ksize = KSize,
             dsize = DSize,
             msize = MSize,
             csize = CSize,
             cnumber = CNum,
             cindex = CIndex,
             offset = Offset,
             clock = Clock,
             timestamp = Timestamp,
             checksum = Checksum,
             ring_hash = RingHash,
             cluster_id = ClusterId,
             num_of_replicas = NumOfReplicas,
             ver = Ver,
             del = Del};

metadata_to_object(#metadata_2{} = Metadata) ->
    #metadata_2{key = Key,
                addr_id = AddrId,
                ksize = KSize,
                dsize = DSize,
                meta = CMeta,
                msize = MSize,
                csize = CSize,
                cnumber = CNum,
                cindex = CIndex,
                offset = Offset,
                clock = Clock,
                timestamp = Timestamp,
                checksum = Checksum,
                ring_hash = RingHash,
                cluster_id = ClusterId,
                num_of_replicas = NumOfReplicas,
                ver = Ver,
                del = Del} = Metadata,
    #?OBJECT{key = Key,
             addr_id = AddrId,
             ksize = KSize,
             dsize = DSize,
             meta = CMeta,
             msize = MSize,
             csize = CSize,
             cnumber = CNum,
             cindex = CIndex,
             offset = Offset,
             clock = Clock,
             timestamp = Timestamp,
             checksum = Checksum,
             ring_hash = RingHash,
             cluster_id = ClusterId,
             num_of_replicas = NumOfReplicas,
             ver = Ver,
             del = Del};

metadata_to_object(#?METADATA{} = Metadata) ->
    #?METADATA{key = Key,
               addr_id = AddrId,
               ksize = KSize,
               dsize = DSize,
               meta = CMeta,
               msize = MSize,
               csize = CSize,
               cnumber = CNum,
               cindex = CIndex,
               offset = Offset,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum,
               ring_hash = RingHash,
               cluster_id = ClusterId,
               num_of_replicas = NumOfReplicas,
               preferred_r = Preferred_R,
               preferred_w = Preferred_W,
               preferred_d = Preferred_D,
               ver = Ver,
               del = Del} = Metadata,
    #?OBJECT{key = Key,
             addr_id = AddrId,
             ksize = KSize,
             dsize = DSize,
             meta = CMeta,
             msize = MSize,
             csize = CSize,
             cnumber = CNum,
             cindex = CIndex,
             offset = Offset,
             clock = Clock,
             timestamp = Timestamp,
             checksum = Checksum,
             ring_hash = RingHash,
             cluster_id = ClusterId,
             num_of_replicas = NumOfReplicas,
             preferred_r = Preferred_R,
             preferred_w = Preferred_W,
             preferred_d = Preferred_D,
             ver = Ver,
             del = Del};
metadata_to_object(_M) ->
    {error, invaid_record}.


%% @doc a metadata to an object info
%%
-spec(metadata_to_object(Bin, Metadata) ->
             Object when Bin::binary(),
                         Metadata::#?METADATA{},
                         Object::#?OBJECT{}).
metadata_to_object(Bin, Metadata) ->
    Object_1 = leo_object_storage_transformer:metadata_to_object(Metadata),
    Object_2 = Object_1#?OBJECT{data = Bin},
    case (Metadata#?METADATA.cnumber > 0) of
        true ->
            Object_2#?OBJECT{checksum = Metadata#?METADATA.checksum};
        false ->
            Checksum = leo_hex:raw_binary_to_integer(
                         crypto:hash(md5, Bin)),
            Object_2#?OBJECT{checksum = Checksum}
    end.


%% @doc Transfer object to metadata
-spec(object_to_metadata(Object) ->
             #?METADATA{} when Object::#object{}|#object_1{}).
object_to_metadata(#object{key = Key,
                           addr_id = AddrId,
                           ksize = KSize,
                           dsize = DSize,
                           msize = MSize,
                           csize = CSize,
                           cnumber = CNum,
                           cindex = CIndex,
                           offset = Offset,
                           clock = Clock,
                           timestamp = Timestamp,
                           checksum = Checksum,
                           ring_hash = RingHash,
                           del = Del}) ->
    #?METADATA{key = Key,
               addr_id = AddrId,
               ksize = KSize,
               dsize = DSize,
               msize = MSize,
               csize = CSize,
               cnumber = CNum,
               cindex = CIndex,
               offset = Offset,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum,
               ring_hash = RingHash,
               del = Del};
object_to_metadata(#object_1{key = Key,
                             addr_id = AddrId,
                             ksize = KSize,
                             dsize = DSize,
                             meta = CMeta,
                             msize = MSize,
                             csize = CSize,
                             cnumber = CNum,
                             cindex = CIndex,
                             offset = Offset,
                             clock = Clock,
                             timestamp = Timestamp,
                             checksum = Checksum,
                             ring_hash = RingHash,
                             cluster_id = ClusterId,
                             num_of_replicas = NumOfReplicas,
                             ver = Ver,
                             del = Del}) ->
    #?METADATA{key = Key,
               addr_id = AddrId,
               ksize = KSize,
               dsize = DSize,
               meta = CMeta,
               msize = MSize,
               csize = CSize,
               cnumber = CNum,
               cindex = CIndex,
               offset = Offset,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum,
               ring_hash = RingHash,
               cluster_id = ClusterId,
               num_of_replicas = NumOfReplicas,
               ver = Ver,
               del = Del};
object_to_metadata(#?OBJECT{key = Key,
                            addr_id = AddrId,
                            ksize = KSize,
                            dsize = DSize,
                            meta = CMeta,
                            msize = MSize,
                            csize = CSize,
                            cnumber = CNum,
                            cindex = CIndex,
                            offset = Offset,
                            clock = Clock,
                            timestamp = Timestamp,
                            checksum = Checksum,
                            ring_hash = RingHash,
                            cluster_id = ClusterId,
                            num_of_replicas = NumOfReplicas,
                            preferred_r = Preferred_R,
                            preferred_w = Preferred_W,
                            preferred_d = Preferred_D,
                            ver = Ver,
                            del = Del}) ->
    #?METADATA{key = Key,
               addr_id = AddrId,
               ksize = KSize,
               dsize = DSize,
               meta = CMeta,
               msize = MSize,
               csize = CSize,
               cnumber = CNum,
               cindex = CIndex,
               offset = Offset,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum,
               ring_hash = RingHash,
               cluster_id = ClusterId,
               num_of_replicas = NumOfReplicas,
               preferred_r = Preferred_R,
               preferred_w = Preferred_W,
               preferred_d = Preferred_D,
               ver = Ver,
               del = Del};
object_to_metadata(_) ->
    {error, invaid_record}.


%% @doc Transform old-type metadata to current-type
-spec(transform_metadata(Metadata) ->
             #metadata_1{} |
             {error, invaid_record} when Metadata::#metadata{} | #metadata_1{}).
transform_metadata(#metadata{key = Key,
                             addr_id = AddrId,
                             ksize = KSize,
                             dsize = DSize,
                             msize = MSize,
                             csize = CSize,
                             cnumber = CNum,
                             cindex = CIndex,
                             offset = Offset,
                             clock = Clock,
                             timestamp = Timestamp,
                             checksum = Checksum,
                             ring_hash = RingHash,
                             del = Del}) ->
    #?METADATA{key = Key,
               addr_id = AddrId,
               ksize = KSize,
               dsize = DSize,
               msize = MSize,
               csize = CSize,
               cnumber = CNum,
               cindex = CIndex,
               offset = Offset,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum,
               ring_hash = RingHash,
               del = Del};

transform_metadata(#metadata_1{key = Key,
                               addr_id = AddrId,
                               ksize = KSize,
                               dsize = DSize,
                               msize = MSize,
                               csize = CSize,
                               cnumber = CNum,
                               cindex = CIndex,
                               offset = Offset,
                               clock = Clock,
                               timestamp = Timestamp,
                               checksum = Checksum,
                               ring_hash = RingHash,
                               cluster_id = ClusterId,
                               num_of_replicas = NumOfReplicas,
                               ver = Ver,
                               del = Del}) ->
    #?METADATA{key = Key,
               addr_id = AddrId,
               ksize = KSize,
               dsize = DSize,
               msize = MSize,
               csize = CSize,
               cnumber = CNum,
               cindex = CIndex,
               offset = Offset,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum,
               ring_hash = RingHash,
               cluster_id = ClusterId,
               num_of_replicas = NumOfReplicas,
               ver = Ver,
               del = Del};
transform_metadata(#metadata_2{key = Key,
                               addr_id = AddrId,
                               ksize = KSize,
                               dsize = DSize,
                               meta = CMeta,
                               msize = MSize,
                               csize = CSize,
                               cnumber = CNum,
                               cindex = CIndex,
                               offset = Offset,
                               clock = Clock,
                               timestamp = Timestamp,
                               checksum = Checksum,
                               ring_hash = RingHash,
                               cluster_id = ClusterId,
                               num_of_replicas = NumOfReplicas,
                               ver = Ver,
                               del = Del}) ->
    #?METADATA{key = Key,
               addr_id = AddrId,
               ksize = KSize,
               dsize = DSize,
               meta = CMeta,
               msize = MSize,
               csize = CSize,
               cnumber = CNum,
               cindex = CIndex,
               offset = Offset,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum,
               ring_hash = RingHash,
               cluster_id = ClusterId,
               num_of_replicas = NumOfReplicas,
               ver = Ver,
               del = Del};
transform_metadata(#?METADATA{} = Metadata) ->
    Metadata;
transform_metadata(_) ->
    {error, invaid_record}.

%% @private
%% check the header
%% according to https://github.com/leo-project/leofs/issues/527#issuecomment-262163109
is_invalid_header(Meta) when Meta#?METADATA.ksize > ?MAX_KEY_SIZE;
                             Meta#?METADATA.msize > ?MAX_MSIZE;
                             Meta#?METADATA.csize > ?MAX_DATABLOCK_SIZE;
                             Meta#?METADATA.offset > ?MAX_OFFSET;
                             Meta#?METADATA.clock > ?MAX_CLOCK ->
    true;
is_invalid_header(_) ->
    false.

%% @doc Transport a header-bin to a metadata
-spec(header_bin_to_metadata(HeaderBin) ->
             #?METADATA{} | {error, invaid_record} when HeaderBin::binary()).
header_bin_to_metadata(Bin) ->
    try
        << Checksum:?BLEN_CHKSUM,
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
           _:?BLEN_BUF >> = Bin,
        Timestamp = case catch calendar:datetime_to_gregorian_seconds(
                                 {{Year, Month, Day}, {Hour, Min, Second}}) of
                        {'EXIT',_} ->
                            0;
                        Val when Val < 63113904000;
                                 Val > 66301199999 ->
                            0;
                        Val ->
                            Val
                    end,
        case (Timestamp /= 0) of
            true ->
                Meta = #?METADATA{addr_id   = AddrId,
                                  ksize = KSize,
                                  msize = MSize,
                                  dsize = DSize,
                                  csize = CSize,
                                  cnumber = CNum,
                                  cindex = CIndex,
                                  offset = Offset,
                                  clock = Clock,
                                  checksum = Checksum,
                                  timestamp = Timestamp,
                                  del = Del},
                case is_invalid_header(Meta) of
                    true ->
                        {error, {invalid_format, over_limit}};
                    false ->
                        Meta
                end;
            false ->
                {error, {invalid_format, unexpected_time_format}}
        end
    catch
        _:_Cause ->
            {error, {invalid_format,_Cause}}
    end.


%% @doc Set values from a custome-metadata
-spec(cmeta_bin_into_metadata(CustomMetadataBin, Metadata)->
             {ok, {Metadata, UDM}} | {error, any()} when CustomMetadataBin::binary(),
                                                         Metadata::#?METADATA{},
                                                         UDM::[{UDM_Key, UDM_Val}],
                                                         UDM_Key::binary(),
                                                         UDM_Val::binary()).
cmeta_bin_into_metadata(<<>>, Metadata) ->
    {ok, {Metadata, []}};
cmeta_bin_into_metadata(CustomMetadataBin, Metadata) ->
    try
        CustomMetadata = binary_to_term(CustomMetadataBin),
        ClusterId = leo_misc:get_value(?PROP_CMETA_CLUSTER_ID, CustomMetadata, []),
        NumOfReplicas = leo_misc:get_value(?PROP_CMETA_NUM_OF_REPLICAS, CustomMetadata, 0),
        Preferred_R = leo_misc:get_value(?PROP_CMETA_PREFERRED_R, CustomMetadata, 0),
        Preferred_W = leo_misc:get_value(?PROP_CMETA_PREFERRED_W, CustomMetadata, 0),
        Preferred_D = leo_misc:get_value(?PROP_CMETA_PREFERRED_D, CustomMetadata, 0),
        Ver = leo_misc:get_value(?PROP_CMETA_VER, CustomMetadata, 0),
        UDM =leo_misc:get_value(?PROP_CMETA_UDM, CustomMetadata, []),
        {ok, {Metadata#?METADATA{cluster_id = ClusterId,
                                 num_of_replicas = NumOfReplicas,
                                 preferred_r = Preferred_R,
                                 preferred_w = Preferred_W,
                                 preferred_d = Preferred_D,
                                 ver = Ver},
              UDM}}
    catch
        _:_Cause ->
            {error, invalid_format}
    end.


%% @doc Get user defined metadata(s) from 'cmeta_bin'
-spec(get_udm_from_cmeta_bin(CustomMetadataBin)->
             {ok, UDM} | {error, any()} when CustomMetadataBin::binary(),
                                             UDM::[{UDM_Key, UDM_Val}],
                                             UDM_Key::binary(),
                                             UDM_Val::binary()).
get_udm_from_cmeta_bin(CustomMetadataBin)->
    try
        CustomMetadata = binary_to_term(CustomMetadataBin),
        UDM = leo_misc:get_value(?PROP_CMETA_UDM, CustomMetadata, []),
        {ok, UDM}
    catch
        _:_Cause ->
            {error, invalid_format}
    end.


%% @doc List to a custome-metadata(binary)
-spec(list_to_cmeta_bin(CustomMetadata) ->
             binary() when CustomMetadata::[{atom(), any()}]).
list_to_cmeta_bin(CustomMetadata) ->
    ClusterId = leo_misc:get_value(?PROP_CMETA_CLUSTER_ID, CustomMetadata, []),
    NumOfReplicas = leo_misc:get_value(?PROP_CMETA_NUM_OF_REPLICAS, CustomMetadata, 0),
    Preferred_R = leo_misc:get_value(?PROP_CMETA_PREFERRED_R, CustomMetadata, 0),
    Preferred_W = leo_misc:get_value(?PROP_CMETA_PREFERRED_W, CustomMetadata, 0),
    Preferred_D = leo_misc:get_value(?PROP_CMETA_PREFERRED_D, CustomMetadata, 0),
    Ver = leo_misc:get_value(?PROP_CMETA_VER, CustomMetadata, 0),
    UDM = leo_misc:get_value(?PROP_CMETA_UDM, CustomMetadata, []),

    term_to_binary([{?PROP_CMETA_CLUSTER_ID, ClusterId},
                    {?PROP_CMETA_NUM_OF_REPLICAS, NumOfReplicas},
                    {?PROP_CMETA_PREFERRED_R, Preferred_R},
                    {?PROP_CMETA_PREFERRED_W, Preferred_W},
                    {?PROP_CMETA_PREFERRED_D, Preferred_D},
                    {?PROP_CMETA_VER, Ver},
                    {?PROP_CMETA_UDM, UDM}
                   ]).

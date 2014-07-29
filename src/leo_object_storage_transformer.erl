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
%%======================================================================
-module(leo_object_storage_transformer).
-author('Yosuke Hara').

-include("leo_object_storage.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([metadata_to_object/1,
         object_to_metadata/1,
         transform_metadata/1,
         header_bin_to_metadata/1,
         cmeta_bin_into_metadata/2,
         list_to_cmeta_bin/1
        ]).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec(metadata_to_object(#metadata{} | #?METADATA{}) ->
             #?OBJECT{} | {error, invaid_record}).
metadata_to_object(#metadata{key     = Key,
                             addr_id = AddrId,
                             ksize   = KSize,
                             dsize   = DSize,
                             msize   = MSize,
                             csize   = CSize,
                             cnumber = CNum,
                             cindex  = CIndex,
                             offset  = Offset,
                             clock   = Clock,
                             timestamp = Timestamp,
                             checksum  = Checksum,
                             ring_hash = RingHash,
                             del = Del}) ->
    #?OBJECT{key     = Key,
             addr_id = AddrId,
             ksize   = KSize,
             dsize   = DSize,
             msize   = MSize,
             csize   = CSize,
             cnumber = CNum,
             cindex  = CIndex,
             offset  = Offset,
             clock   = Clock,
             timestamp = Timestamp,
             checksum  = Checksum,
             ring_hash = RingHash,
             del = Del};
metadata_to_object(#?METADATA{} = Metadata) ->
    #?METADATA{key     = Key,
               addr_id = AddrId,
               ksize   = KSize,
               dsize   = DSize,
               msize   = MSize,
               csize   = CSize,
               cnumber = CNum,
               cindex  = CIndex,
               offset  = Offset,
               clock   = Clock,
               timestamp = Timestamp,
               checksum  = Checksum,
               ring_hash = RingHash,
               cluster_id = ClusterId,
               num_of_replicas = NumOfReplicas,
               ver = Ver,
               del = Del} = Metadata,
    #?OBJECT{key     = Key,
             addr_id = AddrId,
             ksize   = KSize,
             dsize   = DSize,
             msize   = MSize,
             csize   = CSize,
             cnumber = CNum,
             cindex  = CIndex,
             offset  = Offset,
             clock   = Clock,
             timestamp = Timestamp,
             checksum  = Checksum,
             ring_hash = RingHash,
             cluster_id = ClusterId,
             num_of_replicas = NumOfReplicas,
             ver = Ver,
             del = Del};
metadata_to_object(_M) ->
    {error, invaid_record}.


%% @doc Transfer object to metadata
-spec(object_to_metadata(#object{}|#object_1{}) ->
             #metadata_1{}).
object_to_metadata(#object{key     = Key,
                           addr_id = AddrId,
                           ksize   = KSize,
                           dsize   = DSize,
                           msize   = MSize,
                           csize   = CSize,
                           cnumber = CNum,
                           cindex  = CIndex,
                           offset  = Offset,
                           clock   = Clock,
                           timestamp = Timestamp,
                           checksum  = Checksum,
                           ring_hash = RingHash,
                           del       = Del}) ->
    #?METADATA{key     = Key,
               addr_id = AddrId,
               ksize   = KSize,
               dsize   = DSize,
               msize   = MSize,
               csize   = CSize,
               cnumber = CNum,
               cindex  = CIndex,
               offset  = Offset,
               clock   = Clock,
               timestamp = Timestamp,
               checksum  = Checksum,
               ring_hash = RingHash,
               del       = Del};
object_to_metadata(#?OBJECT{key     = Key,
                            addr_id = AddrId,
                            ksize   = KSize,
                            dsize   = DSize,
                            msize   = MSize,
                            csize   = CSize,
                            cnumber = CNum,
                            cindex  = CIndex,
                            offset  = Offset,
                            clock   = Clock,
                            timestamp = Timestamp,
                            checksum  = Checksum,
                            ring_hash = RingHash,
                            cluster_id = ClusterId,
                            num_of_replicas = NumOfReplicas,
                            ver = Ver,
                            del = Del}) ->
    #?METADATA{key     = Key,
               addr_id = AddrId,
               ksize   = KSize,
               dsize   = DSize,
               msize   = MSize,
               csize   = CSize,
               cnumber = CNum,
               cindex  = CIndex,
               offset  = Offset,
               clock   = Clock,
               timestamp = Timestamp,
               checksum  = Checksum,
               ring_hash = RingHash,
               cluster_id = ClusterId,
               num_of_replicas = NumOfReplicas,
               ver = Ver,
               del = Del};
object_to_metadata(_) ->
    {error, invaid_record}.


%% @doc Transform old-type metadata to current-type
-spec(transform_metadata(#metadata{} | #metadata_1{}) ->
             #metadata_1{} | {error, invaid_record}).
transform_metadata(#metadata{key     = Key,
                             addr_id = AddrId,
                             ksize   = KSize,
                             dsize   = DSize,
                             msize   = MSize,
                             csize   = CSize,
                             cnumber = CNum,
                             cindex  = CIndex,
                             offset  = Offset,
                             clock   = Clock,
                             timestamp = Timestamp,
                             checksum  = Checksum,
                             ring_hash = RingHash,
                             del = Del}) ->
    #?METADATA{key     = Key,
               addr_id = AddrId,
               ksize   = KSize,
               dsize   = DSize,
               msize   = MSize,
               csize   = CSize,
               cnumber = CNum,
               cindex  = CIndex,
               offset  = Offset,
               clock   = Clock,
               timestamp = Timestamp,
               checksum  = Checksum,
               ring_hash = RingHash,
               del = Del};
transform_metadata(#?METADATA{} = Metadata) ->
    Metadata;
transform_metadata(_) ->
    {error, invaid_record}.


%% @doc Transport a header-bin to a metadata
-spec(header_bin_to_metadata(binary()) ->
             #?METADATA{} | {error, invaid_record}).
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
        Timestamp =
            case catch calendar:datetime_to_gregorian_seconds(
                         {{Year, Month, Day}, {Hour, Min, Second}}) of
                {'EXIT',_Cause} ->
                    0;
                Val ->
                    Val
            end,
        #?METADATA{addr_id   = AddrId,
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
                   del       = Del}
    catch
        _:_ ->
            {error, invalid_format}
    end.


%% @doc Set values from a custome-metadata
-spec(cmeta_bin_into_metadata(binary(), #?METADATA{})->
             #?METADATA{} | {error, any()}).
cmeta_bin_into_metadata(<<>>, Metadata) ->
    Metadata;
cmeta_bin_into_metadata(CustomMetaBin, Metadata) ->
    try
        CustomeMeta = binary_to_term(CustomMetaBin),
        ClusterId     = leo_misc:get_value(?PROP_CMETA_CLUSTER_ID,      CustomeMeta, []),
        NumOfReplicas = leo_misc:get_value(?PROP_CMETA_NUM_OF_REPLICAS, CustomeMeta, 0),
        Version       = leo_misc:get_value(?PROP_CMETA_VER,             CustomeMeta, 0),
        Metadata#?METADATA{cluster_id = ClusterId,
                           num_of_replicas = NumOfReplicas,
                           ver = Version}
    catch
        _:_Cause ->
            {error, invalid_format}
    end.

%% @doc List to a custome-metadata(binary)
-spec(list_to_cmeta_bin(list(tuple())) ->
             binary()).
list_to_cmeta_bin(CustomeMeta) ->
    ClusterId     = leo_misc:get_value(?PROP_CMETA_CLUSTER_ID,      CustomeMeta, []),
    NumOfReplicas = leo_misc:get_value(?PROP_CMETA_NUM_OF_REPLICAS, CustomeMeta, 0),
    Version       = leo_misc:get_value(?PROP_CMETA_VER,             CustomeMeta, 0),
    term_to_binary([{?PROP_CMETA_CLUSTER_ID, ClusterId},
                    {?PROP_CMETA_NUM_OF_REPLICAS, NumOfReplicas},
                    {?PROP_CMETA_VER, Version}
                   ]).

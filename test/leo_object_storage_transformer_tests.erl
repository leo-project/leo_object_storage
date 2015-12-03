%%====================================================================
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
%%====================================================================
-module(leo_object_storage_transformer_tests).
-author('yosuke hara').

-include_lib("eunit/include/eunit.hrl").
-include("leo_object_storage.hrl").

-ifdef(EUNIT).

%%======================================================================
%% Compaction TEST
%%======================================================================

-define(KEY_1, <<"KEY">>).
-define(BIN_1, <<"VALUE_1">>).
-define(BIN_2, <<"VALUE_2">>).
-define(CLOCK, leo_date:clock()).
-define(TIMESTAMP, leo_date:now()).

all_transformer_test_() ->
    {setup,
     fun ( ) ->
             ?debugVal("*** Transformer Test: START ***"),
             ok
     end,
     fun (_) ->
             ?debugVal("*** Transformer Test: END ***"),
             ok
     end,
     [
      {"suite test",
       {timeout, 1000, fun suite/0}}
     ]}.


suite() ->
    Key_1_Len = byte_size(?KEY_1),
    Bin_1_Len = byte_size(?BIN_1),
    Checksum_1 = leo_hex:raw_binary_to_integer(crypto:hash(md5, ?BIN_1)),
    Checksum_2 = leo_hex:raw_binary_to_integer(crypto:hash(md5, ?BIN_2)),
    Clock = ?CLOCK,
    Timestamp = ?TIMESTAMP,
    ClusterId = 'leofs-1',
    NumOfReplicas = 2,
    Ver = '1.4.0',
    RedMethod = ?RED_COPY,
    ECMethod = undefined,
    ECParams = undefined,

    %% metadata_to_object/1
    Metadata_1 = #metadata{key = ?KEY_1,
                           addr_id = 1,
                           ksize = Key_1_Len,
                           dsize = Bin_1_Len,
                           msize = 0,
                           csize = 0,
                           cnumber = 0,
                           cindex = 0,
                           offset = 256,
                           clock = Clock,
                           timestamp = Timestamp,
                           checksum = Checksum_1,
                           ring_hash = 1,
                           del = 0},
    #?OBJECT{key = ?KEY_1,
             addr_id = 1,
             ksize = Key_1_Len,
             dsize = Bin_1_Len,
             msize = 0,
             csize = 0,
             cnumber = 0,
             cindex = 0,
             offset = 256,
             clock = Clock,
             timestamp = Timestamp,
             checksum = Checksum_1,
             ring_hash = 1,
             del = 0} = leo_object_storage_transformer:metadata_to_object(Metadata_1),

    Metadata_2 = #metadata_1{key = ?KEY_1,
                             addr_id = 1,
                             ksize = Key_1_Len,
                             dsize = Bin_1_Len,
                             msize = 0,
                             csize = 0,
                             cnumber = 0,
                             cindex = 0,
                             offset = 256,
                             clock = Clock,
                             timestamp = Timestamp,
                             checksum = Checksum_1,
                             ring_hash = 1,
                             cluster_id = ClusterId,
                             num_of_replicas = NumOfReplicas,
                             ver = Ver,
                             del = 0},
    #?OBJECT{key = ?KEY_1,
             addr_id = 1,
             ksize = Key_1_Len,
             dsize = Bin_1_Len,
             msize = 0,
             csize = 0,
             cnumber = 0,
             cindex = 0,
             offset = 256,
             clock = Clock,
             timestamp = Timestamp,
             checksum = Checksum_1,
             ring_hash = 1,
             cluster_id = ClusterId,
             num_of_replicas = NumOfReplicas,
             ver = Ver,
             del = 0} = leo_object_storage_transformer:metadata_to_object(Metadata_2),

    Metadata_3 = #metadata_2{key = ?KEY_1,
                             addr_id = 1,
                             ksize = Key_1_Len,
                             dsize = Bin_1_Len,
                             msize = 0,
                             csize = 0,
                             cnumber = 0,
                             cindex = 0,
                             redundancy_method = RedMethod,
                             ec_method = ECMethod,
                             ec_params = ECParams,
                             offset = 256,
                             clock = Clock,
                             timestamp = Timestamp,
                             checksum = Checksum_1,
                             ring_hash = 1,
                             cluster_id = ClusterId,
                             num_of_replicas = NumOfReplicas,
                             ver = Ver,
                             del = 0},
    #?OBJECT{key = ?KEY_1,
             addr_id = 1,
             ksize = Key_1_Len,
             dsize = Bin_1_Len,
             msize = 0,
             csize = 0,
             cnumber = 0,
             cindex = 0,
             offset = 256,
             clock = Clock,
             timestamp = Timestamp,
             checksum = Checksum_1,
             ring_hash = 1,
             cluster_id = ClusterId,
             num_of_replicas = NumOfReplicas,
             ver = Ver,
             redundancy_method = RedMethod,
             ec_method = ECMethod,
             ec_params = ECParams,
             del = 0} = leo_object_storage_transformer:metadata_to_object(Metadata_3),

    %% metadata_to_object/2
    #?OBJECT{key = ?KEY_1,
             addr_id = 1,
             ksize = Key_1_Len,
             dsize = Bin_1_Len,
             msize = 0,
             csize = 0,
             cnumber = 0,
             cindex = 0,
             offset = 256,
             clock = Clock,
             timestamp = Timestamp,
             checksum = Checksum_2,
             ring_hash = 1,
             cluster_id = ClusterId,
             num_of_replicas = NumOfReplicas,
             ver = Ver,
             redundancy_method = RedMethod,
             ec_method = ECMethod,
             ec_params = ECParams,
             del = 0} = leo_object_storage_transformer:metadata_to_object(?BIN_2, Metadata_3),

    %% object_to_metadata/1
    Object_1 = #object{key = ?KEY_1,
                       addr_id = 1,
                       data = ?BIN_1,
                       ksize = Key_1_Len,
                       dsize = Bin_1_Len,
                       msize = 0,
                       csize = 0,
                       cnumber = 0,
                       cindex = 0,
                       offset = 256,
                       clock = Clock,
                       timestamp = Timestamp,
                       checksum = Checksum_1,
                       ring_hash = 1,
                       del = 0},
    #?METADATA{key = ?KEY_1,
               addr_id = 1,
               ksize = Key_1_Len,
               dsize = Bin_1_Len,
               msize = 0,
               csize = 0,
               cnumber = 0,
               cindex = 0,
               redundancy_method = ?RED_COPY,
               ec_method = undefined,
               ec_params = undefined,
               offset = 256,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum_1,
               ring_hash = 1,
               cluster_id = undefined,
               num_of_replicas = 0,
               ver = 0,
               del = 0} = leo_object_storage_transformer:object_to_metadata(Object_1),

    Object_2 = #object_1{key = ?KEY_1,
                         addr_id = 1,
                         data = ?BIN_1,
                         ksize = Key_1_Len,
                         dsize = Bin_1_Len,
                         msize = 0,
                         csize = 0,
                         cnumber = 0,
                         cindex = 0,
                         offset = 256,
                         clock = Clock,
                         timestamp = Timestamp,
                         checksum = Checksum_1,
                         ring_hash = 1,
                         cluster_id = ClusterId,
                         num_of_replicas = NumOfReplicas,
                         ver = Ver,
                         del = 0},
    #?METADATA{key = ?KEY_1,
               addr_id = 1,
               ksize = Key_1_Len,
               dsize = Bin_1_Len,
               msize = 0,
               csize = 0,
               cnumber = 0,
               cindex = 0,
               redundancy_method = ?RED_COPY,
               ec_method = undefined,
               ec_params = undefined,
               offset = 256,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum_1,
               ring_hash = 1,
               cluster_id = ClusterId,
               num_of_replicas = NumOfReplicas,
               ver = Ver,
               del = 0} = leo_object_storage_transformer:object_to_metadata(Object_2),

    Object_3 = #?OBJECT{key = ?KEY_1,
                        addr_id = 1,
                        data = ?BIN_1,
                        ksize = Key_1_Len,
                        dsize = Bin_1_Len,
                        msize = 0,
                        csize = 0,
                        cnumber = 0,
                        cindex = 0,
                        offset = 256,
                        clock = Clock,
                        timestamp = Timestamp,
                        checksum = Checksum_1,
                        ring_hash = 1,
                        cluster_id = ClusterId,
                        num_of_replicas = NumOfReplicas,
                        ver = Ver,
                        redundancy_method = RedMethod,
                        ec_method = ECMethod,
                        ec_params = ECParams,
                        del = 0},
    #?METADATA{key = ?KEY_1,
               addr_id = 1,
               ksize = Key_1_Len,
               dsize = Bin_1_Len,
               msize = 0,
               csize = 0,
               cnumber = 0,
               cindex = 0,
               redundancy_method = RedMethod,
               ec_method = ECMethod,
               ec_params = ECParams,
               offset = 256,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum_1,
               ring_hash = 1,
               cluster_id = ClusterId,
               num_of_replicas = NumOfReplicas,
               ver = Ver,
               del = 0} = leo_object_storage_transformer:object_to_metadata(Object_3),

    %% transform_metadata/1
    #?METADATA{key = ?KEY_1,
               addr_id = 1,
               ksize = Key_1_Len,
               dsize = Bin_1_Len,
               msize = 0,
               csize = 0,
               cnumber = 0,
               cindex = 0,
               redundancy_method = ?RED_COPY,
               ec_method = undefined,
               ec_params = undefined,
               offset = 256,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum_1,
               ring_hash = 1,
               cluster_id = undefined,
               num_of_replicas = 0,
               ver = 0,
               del = 0} = leo_object_storage_transformer:transform_metadata(Metadata_1),
    #?METADATA{key = ?KEY_1,
               addr_id = 1,
               ksize = Key_1_Len,
               dsize = Bin_1_Len,
               msize = 0,
               csize = 0,
               cnumber = 0,
               cindex = 0,
               redundancy_method = ?RED_COPY,
               ec_method = undefined,
               ec_params = undefined,
               offset = 256,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum_1,
               ring_hash = 1,
               cluster_id = ClusterId,
               num_of_replicas = NumOfReplicas,
               ver = Ver,
               del = 0} = leo_object_storage_transformer:transform_metadata(Metadata_2),
    Metadata_3 = leo_object_storage_transformer:transform_metadata(Metadata_3),


    %% transform_object/1
    Object_11 = #object{method = 'put',
                        key = ?KEY_1,
                        addr_id = 1234567890,
                        data = ?BIN_1,
                        meta = <<>>,
                        ksize = byte_size(?KEY_1),
                        dsize = byte_size(?BIN_1),
                        msize = 0,
                        csize = 10,
                        cnumber = 5,
                        cindex = 1,
                        offset = 707,
                        clock = Clock,
                        timestamp = Timestamp,
                        checksum = 808,
                        ring_hash = 909,
                        req_id = 1,
                        del = ?DEL_FALSE},
    ?assertEqual(#?OBJECT{method = 'put',
                          key = ?KEY_1,
                          addr_id = 1234567890,
                          data = ?BIN_1,
                          meta = <<>>,
                          ksize = byte_size(?KEY_1),
                          dsize = byte_size(?BIN_1),
                          msize = 0,
                          csize = 10,
                          cnumber = 5,
                          cindex = 1,
                          offset = 707,
                          clock = Clock,
                          timestamp = Timestamp,
                          checksum = 808,
                          ring_hash = 909,
                          req_id = 1,
                          del = ?DEL_FALSE}, leo_object_storage_transformer:transform_object(Object_11)),

    Object_12 = #object_1{method = 'put',
                          key = ?KEY_1,
                          addr_id = 1234567890,
                          data = ?BIN_1,
                          meta = <<>>,
                          ksize = byte_size(?KEY_1),
                          dsize = byte_size(?BIN_1),
                          msize = 0,
                          csize = 10,
                          cnumber = 5,
                          cindex = 1,
                          offset = 707,
                          clock = Clock,
                          timestamp = Timestamp,
                          checksum = 808,
                          ring_hash = 909,
                          req_id = 1,
                          cluster_id = 'leofs',
                          num_of_replicas = 3,
                          ver = 1,
                          del = ?DEL_TRUE},
    ?assertEqual(#?OBJECT{method = 'put',
                          key = ?KEY_1,
                          addr_id = 1234567890,
                          data = ?BIN_1,
                          meta = <<>>,
                          ksize = byte_size(?KEY_1),
                          dsize = byte_size(?BIN_1),
                          msize = 0,
                          csize = 10,
                          cnumber = 5,
                          cindex = 1,
                          offset = 707,
                          clock = Clock,
                          timestamp = Timestamp,
                          checksum = 808,
                          ring_hash = 909,
                          req_id = 1,
                          cluster_id = 'leofs',
                          num_of_replicas = 3,
                          ver = 1,
                          del = ?DEL_TRUE}, leo_object_storage_transformer:transform_object(Object_12)),

    Object_13 = #object_2{method = 'put',
                          key = ?KEY_1,
                          addr_id = 1234567890,
                          data = ?BIN_1,
                          meta = <<>>,
                          ksize = byte_size(?KEY_1),
                          dsize = byte_size(?BIN_1),
                          msize = 0,
                          csize = 10,
                          cnumber = 5,
                          cindex = 1,
                          offset = 707,
                          clock = Clock,
                          timestamp = Timestamp,
                          checksum = 808,
                          ring_hash = 909,
                          req_id = 1,
                          cluster_id = 'leofs',
                          num_of_replicas = 3,
                          ver = 1,
                          redundancy_method = ?RED_ERASURE_CODE,
                          ec_method = 'vandrs',
                          ec_params = {10,4,8},
                          del = ?DEL_TRUE},
    ?assertEqual(#?OBJECT{method = 'put',
                          key = ?KEY_1,
                          addr_id = 1234567890,
                          data = ?BIN_1,
                          meta = <<>>,
                          ksize = byte_size(?KEY_1),
                          dsize = byte_size(?BIN_1),
                          msize = 0,
                          csize = 10,
                          cnumber = 5,
                          cindex = 1,
                          offset = 707,
                          clock = Clock,
                          timestamp = Timestamp,
                          checksum = 808,
                          ring_hash = 909,
                          req_id = 1,
                          cluster_id = 'leofs',
                          num_of_replicas = 3,
                          ver = 1,
                          redundancy_method = ?RED_ERASURE_CODE,
                          ec_method = 'vandrs',
                          ec_params = {10,4,8},
                          del = ?DEL_TRUE}, leo_object_storage_transformer:transform_object(Object_13)),


    %% header_bin_to_metadata/1
    Bin_3 = create_needle(Object_3),
    #?METADATA{key = <<>>,
               addr_id = 1,
               ksize = Key_1_Len,
               dsize = Bin_1_Len,
               msize = 0,
               csize = 0,
               cnumber = 0,
               cindex = 0,
               redundancy_method = ?RED_COPY,
               ec_method = undefined,
               ec_params = undefined,
               offset = 256,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum_1,
               ring_hash = 0,
               cluster_id = undefined,
               num_of_replicas = 0,
               ver = 0,
               del = 0} = leo_object_storage_transformer:header_bin_to_metadata(Bin_3),

    %% cmeta_bin_into_metadata/2
    ListMeta = [{?PROP_CMETA_CLUSTER_ID, ClusterId},
                {?PROP_CMETA_NUM_OF_REPLICAS, NumOfReplicas},
                {?PROP_CMETA_VER, Ver},
                {?PROP_CMETA_RED_METHOD, RedMethod},
                {?PROP_CMETA_EC_METHOD, ECMethod},
                {?PROP_CMETA_EC_PARAMS, ECParams}
               ],
    CustomMetaBin = term_to_binary(ListMeta),
    #?METADATA{key = ?KEY_1,
               addr_id = 1,
               ksize = Key_1_Len,
               dsize = Bin_1_Len,
               msize = 0,
               csize = 0,
               cnumber = 0,
               cindex = 0,
               redundancy_method = RedMethod,
               ec_method = ECMethod,
               ec_params = ECParams,
               offset = 256,
               clock = Clock,
               timestamp = Timestamp,
               checksum = Checksum_1,
               ring_hash = 1,
               cluster_id = ClusterId,
               num_of_replicas = NumOfReplicas,
               ver = Ver,
               del = 0} = leo_object_storage_transformer:cmeta_bin_into_metadata(
                            CustomMetaBin,
                            #?METADATA{key = ?KEY_1,
                                       addr_id = 1,
                                       ksize = Key_1_Len,
                                       dsize = Bin_1_Len,
                                       msize = 0,
                                       csize = 0,
                                       cnumber = 0,
                                       cindex = 0,
                                       offset = 256,
                                       clock = Clock,
                                       timestamp = Timestamp,
                                       checksum = Checksum_1,
                                       ring_hash = 1,
                                       del = 0}),

    %% list_to_cmeta_bin/1
    CustomMetaBin = leo_object_storage_transformer:list_to_cmeta_bin(ListMeta),
    ok.


%% @private
create_needle(#?OBJECT{addr_id    = AddrId,
                       ksize      = KSize,
                       dsize      = DSize,
                       msize      = MSize,
                       csize      = CSize,
                       cnumber    = CNum,
                       cindex     = CIndex,
                       clock      = Clock,
                       offset     = Offset,
                       timestamp  = Timestamp,
                       checksum   = Checksum,
                       del        = Del}) ->
    {{Year,Month,Day},{Hour,Min,Second}} =
        calendar:gregorian_seconds_to_datetime(Timestamp),
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
                 0:?BLEN_BUF >>,
    Needle.

-endif.

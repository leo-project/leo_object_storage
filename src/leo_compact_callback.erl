%%======================================================================
%%
%% Leo Compaction Callback
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
%% @doc The ordning-reda's behaviour.
%% @end
%%======================================================================
-module(leo_compact_callback).
-author('Yosuke Hara').

-include("leo_object_storage.hrl").

%% @doc Check the owner of the object by key
-callback(has_charge_of_node(Metadata::#?METADATA{}, NumOfReplicas::pos_integer()) ->
                 boolean()).

%% @doc Update a metadata (during the data-compaction processing)
-callback(update_metadata(Method::put|delete, Key::binary(), Metadata::#?METADATA{}) ->
                 ok | {error, any()}).

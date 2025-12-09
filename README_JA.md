# leo_object_storage

[![Erlang/OTP](https://img.shields.io/badge/Erlang%2FOTP-19+-blue.svg)](https://www.erlang.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

[English](README.md)

## 概要

**leo_object_storage** は、Erlang/OTPアプリケーション向けのログ構造型オブジェクト/BLOBストレージライブラリです。Facebook の Haystack に着想を得た追記専用ストレージフォーマットを実装し、非構造化データの効率的な保存と取得を提供します。

### 主な特徴

- **ログ構造型ストレージ**: 高い書き込み性能を実現する追記専用設計
- **Haystack フォーマット**: MD5チェックサム付きの AVS（Append-only Versioned Storage）ファイルフォーマット
- **データコンパクション**: 並行数を設定可能なバックグラウンドガベージコレクション
- **メタデータ管理**: メタデータバックエンドとして LevelDB または Bitcask をサポート
- **チャンク分割オブジェクト**: 複数チャンクに分割された大容量オブジェクトのサポート
- **診断・リカバリ**: データ診断とメタデータリカバリのための組み込みツール
- **OTP標準ロギング**: 診断出力に OTP logger を使用

## 動作要件

- Erlang/OTP 19 以降
- rebar3

## インストール

`rebar.config` に以下を追加してください：

```erlang
{deps, [
    {leo_object_storage, {git, "https://github.com/leo-project/leo_object_storage.git", {tag, "2.1.0"}}}
]}.
```

## ビルド

```bash
$ rebar3 compile
```

## テスト

```bash
$ rebar3 eunit
```

## アーキテクチャ

### モジュール構成

| モジュール | 説明 |
|-----------|------|
| `leo_object_storage_api` | 公開APIエントリーポイント |
| `leo_object_storage_server` | ストレージ操作を管理する Gen_server |
| `leo_object_storage_haystack` | ログ構造型ストレージの実装 |
| `leo_object_storage_sup` | プロセス管理用スーパーバイザー |
| `leo_compact_fsm_controller` | コンパクション制御用FSMコントローラー |
| `leo_compact_fsm_worker` | コンパクション実行用FSMワーカー |
| `leo_object_storage_transformer` | データ変換ユーティリティ |

### ストレージフォーマット

AVS（Append-only Versioned Storage）フォーマットは、以下を含む1024バイトのヘッダーを使用します：

- MD5チェックサム（128ビット）
- キーサイズ、データサイズ、メタデータサイズ
- オフセット、アドレスID、クロックタイムスタンプ
- 削除フラグ、チャンク情報

## 設定

アプリケーション環境で以下の設定オプションを指定できます：

| オプション | デフォルト | 説明 |
|-----------|-----------|------|
| `metadata_storage` | `leveldb` | メタデータバックエンド（`leveldb` または `bitcask`） |
| `object_storage` | `haystack` | オブジェクトストレージフォーマット |
| `sync_mode` | `none` | 同期モード（`none`、`periodic`、`writethrough`） |
| `sync_interval_in_ms` | `1000` | 同期間隔（ミリ秒） |
| `is_strict_check` | `false` | 厳格なデータ検証を有効化 |
| `is_enable_diagnosis_log` | `true` | 診断ログを有効化 |

## 基本的な使用方法

### ストレージの初期化

```erlang
%% アプリケーションを起動
application:start(leo_object_storage),

%% 設定を指定してオブジェクトストレージを起動
%% フォーマット: [{コンテナ数, パス}]
leo_object_storage_api:start([{1, "/path/to/storage"}]).
```

### オブジェクトの保存

```erlang
%% メタデータを作成してオブジェクトを保存
Object = #?OBJECT{
    key = <<"my_key">>,
    data = <<"my_data">>,
    addr_id = 1
},
leo_object_storage_api:put({1, <<"my_key">>}, Object).
```

### オブジェクトの取得

```erlang
%% アドレスIDとキーでオブジェクトを取得
{ok, Metadata, Object} = leo_object_storage_api:get({AddrId, Key}).
```

### オブジェクトの削除

```erlang
%% 論理削除（削除マークを付与）
leo_object_storage_api:delete({AddrId, Key}, Object).
```

### データコンパクション

```erlang
%% 並行数を指定してコンパクションを実行
leo_object_storage_api:compact_data(NumOfConcurrency).

%% コンパクション状態を確認
{ok, Status} = leo_object_storage_api:compact_state().
```

### 診断

```erlang
%% 全コンテナの診断を実行
leo_object_storage_api:diagnose_data().

%% AVSファイルからメタデータをリカバリ
leo_object_storage_api:recover_metadata().
```

## LeoFSでの利用

**leo_object_storage** は [LeoFS](https://github.com/leo-project/leofs) のコアコンポーネントであり、[leo_storage](https://github.com/leo-project/leofs/tree/v1/apps/leo_storage) で分散オブジェクトストレージにおける非構造化データの保存と管理に使用されています。

## ライセンス

Apache License, Version 2.0

## スポンサー

- LeoProject/LeoFS 2019年1月- [Lions Data, Ltd.](https://lions-data.com/)
- LeoProject/LeoFS 2012年 - 2018年12月末 [楽天株式会社](https://global.rakuten.com/corp/)

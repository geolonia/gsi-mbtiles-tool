# @geolonia/gsi-mbtiles-tool

国土地理院が公開している地図タイルをmbtilesに直接ダウンロードし、差分更新できるツールです。

このツールは地理院が公開しているいくつかの資料のもとに作られています。

* [地理院タイルダウンロードツール](https://github.com/gsi-cyberjapan/tdlmn)
* [mokuroku-spec](https://github.com/gsi-cyberjapan/mokuroku-spec)

このツールはまだベータ版です。今後追加していきたい[機能や改善は GitHub Issues でご確認ください](https://github.com/geolonia/gsi-mbtiles-tool/issues?q=is%3Aissue+is%3Aopen+label%3Aenhancement)。

## 使い方

```
npm install -g @geolonia/gsi-mbtiles-tool
gsi-mbtiles-tool [id]
```

または

```
npx @geolonia/gsi-mbtiles-tool [id]
```

詳細なオプション（出力ファイル指定、ズームレベルフィルター）は `gsi-mbtiles-tool --help` を参照してください。

`[id]` には[タイル一覧](https://maps.gsi.go.jp/development/ichiran.html)の `xyz/[id]/{z}/{x}/{y}.png` の `[id]` と指します。

現在、あらかじめメタ情報を指定する必要があるので、 `src/etc/gsi_tilesets.ts` をご確認ください。

**注意**: mokuroku をメモリー内に保存するので、特にタイル数が多いタイルセットはOOMで死ぬ場合もあります。その場合、 `env NODE_OPTIONS="--max_old_space_size=10000" gsi-mbtiles-tool ...` などで領域解放できます。

## 仕組み

地理院が公開しているタイルには、全て `mokuroku.csv` というファイルが公開されていて、全てのタイルやそのタイルの
MD5ハッシュを格納しています。

このツールは、 mbtiles に md5 をキーとしてタイル情報を格納し、 z/x/y を md5 にマッピングした形で保存されます。このため、重複するタイル（例えば100%白や100%透明など）が二重にダウンロードや保管されず、保存とダウンロード容量が節約できます。

mbtiles のファイルが既に存在する場合は、既に入っていない md5 のタイルのみダウンロードし、 z/x/y と md5 のマッピングを同期します。

`earthhillshade` (z0-8, 1.5GB) の場合、著者の環境では新しくダウンロードするために約4分かかり、その同じ mbtiles に更新をかける時は5秒ぐらいかかります。

### 新規作成時の例

```
> yarn build && node dist/bin/run.js earthhillshade
earthhillshade: 0.028ms Starting up 陰影起伏図（全球版）...
earthhillshade: 437.914ms mokuroku に 87381 件のタイルが認識しました
earthhillshade: 452.885ms mokuroku に 34039 件のユニークなタイルを認識しました。
earthhillshade: 10.464s [タイルダウンロード] remaining=32752 newlyInserted=1267 total=34039
earthhillshade: 20.464s [タイルダウンロード] remaining=31585 newlyInserted=2434 total=34039
earthhillshade: 30.464s [タイルダウンロード] remaining=30248 newlyInserted=3771 total=34039
earthhillshade: 40.464s [タイルダウンロード] remaining=28770 newlyInserted=5249 total=34039
earthhillshade: 50.464s [タイルダウンロード] remaining=27392 newlyInserted=6627 total=34039
earthhillshade: 1:00.464 (m:ss.mmm) [タイルダウンロード] remaining=25952 newlyInserted=8067 total=34039
earthhillshade: 1:10.463 (m:ss.mmm) [タイルダウンロード] remaining=24740 newlyInserted=9279 total=34039
earthhillshade: 1:20.464 (m:ss.mmm) [タイルダウンロード] remaining=22684 newlyInserted=11335 total=34039
earthhillshade: 1:30.464 (m:ss.mmm) [タイルダウンロード] remaining=21157 newlyInserted=12862 total=34039
earthhillshade: 1:40.465 (m:ss.mmm) [タイルダウンロード] remaining=19785 newlyInserted=14234 total=34039
earthhillshade: 1:50.465 (m:ss.mmm) [タイルダウンロード] remaining=18313 newlyInserted=15706 total=34039
earthhillshade: 2:00.466 (m:ss.mmm) [タイルダウンロード] remaining=16687 newlyInserted=17332 total=34039
earthhillshade: 2:10.466 (m:ss.mmm) [タイルダウンロード] remaining=15221 newlyInserted=18798 total=34039
earthhillshade: 2:20.466 (m:ss.mmm) [タイルダウンロード] remaining=13400 newlyInserted=20619 total=34039
earthhillshade: 2:30.467 (m:ss.mmm) [タイルダウンロード] remaining=11596 newlyInserted=22423 total=34039
earthhillshade: 2:40.467 (m:ss.mmm) [タイルダウンロード] remaining=9844 newlyInserted=24175 total=34039
earthhillshade: 2:50.467 (m:ss.mmm) [タイルダウンロード] remaining=7902 newlyInserted=26117 total=34039
earthhillshade: 3:00.467 (m:ss.mmm) [タイルダウンロード] remaining=5790 newlyInserted=28229 total=34039
earthhillshade: 3:10.467 (m:ss.mmm) [タイルダウンロード] remaining=3990 newlyInserted=30029 total=34039
earthhillshade: 3:20.467 (m:ss.mmm) [タイルダウンロード] remaining=2053 newlyInserted=31966 total=34039
earthhillshade: 3:30.467 (m:ss.mmm) [タイルダウンロード] remaining=423 newlyInserted=33596 total=34039
earthhillshade: 3:33.243 (m:ss.mmm) 34039 件の新しいタイルを mbtiles に格納しました
earthhillshade: 3:43.243 (m:ss.mmm) [タイル同期] current=32889 total=87381
earthhillshade: 3:53.242 (m:ss.mmm) [タイル同期] current=65498 total=87381
earthhillshade: 4:00.123 (m:ss.mmm)
```

`[タイルダウンロード]` と `[タイル同期]` はそれぞれ、進捗を毎10秒に報告します。

### 更新時の例

```
> yarn build && node dist/bin/run.js earthhillshade
earthhillshade: 0.027ms Starting up 陰影起伏図（全球版）...
earthhillshade: 278.526ms mokuroku に 87381 件のタイルが認識しました
earthhillshade: 294.656ms mokuroku に 34039 件のユニークなタイルを認識しました。
earthhillshade: 1.057s 0 件の新しいタイルを mbtiles に格納しました
earthhillshade: 4.744s
```

## ライセンス

* MIT

このリポジトリに含まれるソースコードのライセンスは MIT としますが、成果物のタイルをご利用になる場合は、測量法第２９条（複製）、第３０条（使用）に基づき国土地理院長への承認申請が必要になる可能性がありますので、ご注意ください。

https://www.gsi.go.jp/LAW/2930-index.html

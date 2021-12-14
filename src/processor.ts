import async from 'async';
import sqlite3 from 'sqlite3';
import https from 'https';
import zlib from 'zlib';
import { parse as csvParse } from 'csv-parse';
import concat from 'concat-stream';
import { pipeline } from 'stream';
import tilesets from './etc/gsi_tilesets';
import { createDbSql } from './etc/schema';

const sqlite3v = sqlite3.verbose();

const initDb = (path: string) => new Promise<sqlite3.Database>((res, rej) => {
  const db = new sqlite3v.Database(path, (err) => {
    if (err) return rej(err);
    db.exec(createDbSql, (err) => {
      if (err) return rej(err);
      res(db);
    });
  });
});

const closeDb = (db: sqlite3.Database) => new Promise<void>((res, rej) => {
  db.close((err) => {
    if (err) return rej(err);
    res();
  });
});

const verifyTilesetMetadata = (db: sqlite3.Database, id: string) => new Promise<void>((res, rej) => {
  db.get('SELECT value FROM metadata WHERE name = \'_gsi_tileset_id\'', (err, row) => {
    if (err) return rej(err);
    if (row) {
      if (row.value === id) return res();
      return rej(`この mbtiles は ${row.value} と同期していますが、 ${id} と同期しようとしているので、中断します。`);
    }

    db.get('SELECT count(*) AS "count" FROM tiles', (err, row) => {
      if (err) return rej(err);
      if (row.count === 0) return res();
      rej(`この mbtiles は既に他のタイルが入っているため、中断します。`);
    });
  });
})

type MokurokuRow = [string, string, string, string];
type MokurokuArray = MokurokuRow[];
const getMokuroku = (id: string) => new Promise<MokurokuArray>((res, rej) => {
  const url = `https://cyberjapandata.gsi.go.jp/xyz/${id}/mokuroku.csv.gz`;
  https.get(url, (resp) => {
    resp.on('error', rej);

    const rows: MokurokuArray = [];
    const csvParser = csvParse();
    pipeline(
      resp,
      zlib.createGunzip(),
      (err) => {
        if (err) return rej(err);
      }
    )
      .pipe(csvParser)
      .on('data', (row) => {
        rows.push(row);
      })
      .on('error', (err) => rej(err))
      .on('close', () => res(rows));
  });
});

const mokurokuUniqueTiles = (input: MokurokuArray) => {
  const outputMd5s = new Set<string>([]);
  const output: MokurokuArray = [];
  for (const row of input) {
    const md5 = row[3];
    if (outputMd5s.has(md5)) continue;
    outputMd5s.add(md5);
    output.push(row);
  }
  return output;
}

const checkImageTile = (db: sqlite3.Database, md5: string) => new Promise<boolean>((res, rej) => {
  db.get('SELECT 1 FROM images WHERE md5 = ?', [ md5 ], (err, row) => {
    if (err) return rej(err);
    if (row) return res(true);
    res(false);
  });
});

const getTileData = (url: string) => new Promise<Buffer>((res, rej) => {
  https
    .get(url, (resp) => {
      const out: Buffer[] = [];
      resp.on('data', (d) => out.push(d));
      resp.on('end', () => {
        res(Buffer.concat(out));
      });
    })
    .on('error', rej);
});

const putTileImageData = (db: sqlite3.Database, md5: string, size: number, data: Buffer) => new Promise<void>((res, rej) => {
  db.run(
    'INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)',
    [ md5, size, data ],
    (err) => {
      if (err) return rej(err);
      res();
    }
  );
});

const syncImagesTable = async (db: sqlite3.Database, id: string, um: MokurokuArray) => {
  const totalCount = um.length;
  let insertCount = 0;
  const queue = async.queue<MokurokuRow>(
    async (row) => {
      const exists = await checkImageTile(db, row[3]);
      if (exists) return;
      const tileData = await getTileData(`https://cyberjapandata.gsi.go.jp/xyz/${id}/${row[0]}`);
      await putTileImageData(
        db,
        row[3],
        parseInt(row[2], 10),
        tileData,
      );
      insertCount += 1;
    },
    20,
  );
  queue.push(um);
  const watcher = setInterval(() => {
    console.timeLog(id, `[タイルダウンロード] remaining=${queue.length()} newlyInserted=${insertCount} total=${totalCount}`);
  }, 10_000);
  await queue.drain();
  clearInterval(watcher);
  return insertCount;
};

const insertTileRef = (db: sqlite3.Database, z: number, x: number, y: number, md5: string, updated: number) => new Promise<void>((res, rej) => {
  const flippedY = y = (1 << z) - 1 - y;
  db.run(`
    INSERT INTO tile_ref (zoom_level, tile_column, tile_row, image_md5, updated_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(zoom_level, tile_column, tile_row) DO UPDATE SET
      image_md5 = excluded.image_md5,
      updated_at = excluded.updated_at
    WHERE updated_at <> excluded.updated_at
  `,
  [ z, x, flippedY, md5, updated ],
  (err) => {
    if (err) return rej(err);
    res();
  })
});

const syncTileRefTable = async (db: sqlite3.Database, id: string, moku: MokurokuArray) => {
  let currentRow = 0;
  const mokuLen = moku.length;
  const watcher = setInterval(() => {
    console.timeLog(id, `[タイル同期] current=${currentRow} total=${mokuLen}`);
  }, 10_000);
  for (const row of moku) {
    const [z, x, y] = row[0].substring(0, row[0].length - 4).split('/');
    await insertTileRef(db,
      parseInt(z, 10),
      parseInt(x, 10),
      parseInt(y, 10),
      row[3],
      parseInt(row[1], 10)
    );
    currentRow += 1;
  }
  clearInterval(watcher);
}

const writeMetadata = (db: sqlite3.Database, name: string, value: string) => new Promise<void>((res, rej) => {
  db.run(
    `INSERT INTO metadata (name, value) VALUES (?, ?)
    ON CONFLICT (name) DO UPDATE SET
      value = excluded.value`,
    [ name, value ],
    (err) => {
      if (err) return rej();
      res();
    }
  );
});

const processor = async (id: string, output: string) => {
  // sqlite を用意する
  const db = await initDb(output);

  const meta = tilesets[id];
  if (!meta) throw new Error(`expected ${id} to be in tilesets`);

  // metadataを確認する（idが一致するか確認。存在しない、かつ、tilesが空の場合は作成。設定しているが、一致しない場合はエラー。）
  await verifyTilesetMetadata(db, id);

  // mokuroku.csv をダウンロード
  const mokuroku = await getMokuroku(id);
  console.timeLog(id, `mokuroku に ${mokuroku.length} 件のタイルが認識しました`);
  // md5でユニークをかける
  const uniqueMokuroku = mokurokuUniqueTiles(mokuroku);
  console.timeLog(id, `mokuroku に ${uniqueMokuroku.length} 件のユニークなタイルを認識しました。`);

  // imagesテーブルに入っていないmd5をダウンロードし、imagesに挿入
  const newImages = await syncImagesTable(db, id, uniqueMokuroku);
  console.timeLog(id, `${newImages} 件の新しいタイルを mbtiles に格納しました`)

  // [TODO] mokurokuに入っていないimagesを削除（依存関係を確認する必要がある）

  // tile_ref と mokuroku を同期する
  await syncTileRefTable(db, id, mokuroku);

  // metadataテーブル用意
  await writeMetadata(db, '_gsi_tileset_id', id);
  await writeMetadata(db, 'name', meta.name);
  await writeMetadata(db, 'format', 'png');
  await writeMetadata(db, 'minzoom', meta.minZoom.toString());
  await writeMetadata(db, 'maxzoom', meta.maxZoom.toString());
  await writeMetadata(db, 'version', '1');
  await writeMetadata(db, 'attribution', '<a href="https://www.gsi.go.jp/" target="_blank">&copy; GSI Japan</a>');

  await closeDb(db);
}

export default processor;

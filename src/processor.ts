import async from 'async';
import sqlite3 from 'better-sqlite3';
import https from 'https';
import zlib from 'zlib';
import SphericalMercator from '@mapbox/sphericalmercator';
import { parse as csvParse } from 'csv-parse';
import { pipeline } from 'stream';
import tilesets, { TilesetSpec, TileTransformer } from './etc/gsi_tilesets';
import { createDbSql } from './etc/schema';

const initDb = (path: string) => {
  const db = new sqlite3(path);
  db.pragma('journal_mode = MEMORY');
  db.exec(createDbSql);
  return db;
};

const verifyTilesetMetadata = (db: sqlite3.Database, id: string) => {
  const tilesetIdRow = db.prepare('SELECT value FROM metadata WHERE name = \'_gsi_tileset_id\'').get();
  if (tilesetIdRow?.value === id) return; // OK to proceed
  if (tilesetIdRow) {
    throw new Error(`この mbtiles は ${tilesetIdRow.value} と同期していますが、 ${id} と同期しようとしているので、中断します。`)
  }

  const tileCountRow = db.prepare('SELECT count(*) AS "count" FROM tiles').get();
  if (tileCountRow?.count === 0) return; // OK to proceed
  throw new Error(`この mbtiles は既に他のタイルが入っているため、中断します。`);
};

type MokurokuRow = [string, string, string, string];
type MokurokuArray = MokurokuRow[];
const getMokuroku = (ctx: ProcessorCtx) => new Promise<MokurokuArray>((res, rej) => {
  const {
    id,
    gsiId,
    minZoom,
    maxZoom,
  } = ctx;
  const url = `https://cyberjapandata.gsi.go.jp/xyz/${gsiId}/mokuroku.csv.gz`;
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
        const zoom = parseInt(row[0].split('/', 2)[0], 10);
        if (zoom >= minZoom && zoom <= maxZoom) {
          rows.push(row);
        }
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

let _preparedImageTileQuery: sqlite3.Statement | undefined;
const checkImageTile = (db: sqlite3.Database, md5: string) => {
  if (!_preparedImageTileQuery) {
    _preparedImageTileQuery = db.prepare('SELECT 1 FROM images WHERE md5 = ?');
  }
  const row = _preparedImageTileQuery.get(md5);
  if (row) return true;
  return false;
};

const getTileData = (url: string) => new Promise<Buffer>((res, rej) => {
  https
    .get(url, {
      headers: {
        // QGIS expects gzip-encoded vector tiles. If we don't pass this header,
        // the tiles will not be gzipped.
        'accept-encoding': 'gzip'
      }
    }, (resp) => {
      const out: Buffer[] = [];
      resp.on('data', (d) => out.push(d));
      resp.on('end', () => {
        res(Buffer.concat(out));
      });
    })
    .on('error', rej);
});

let _preparedInsertTileDataQuery: sqlite3.Statement | undefined;
const putTileImageData = (db: sqlite3.Database, md5: string, size: number, data: Buffer) => {
  if (!_preparedInsertTileDataQuery) {
    _preparedInsertTileDataQuery = db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)');
  }
  _preparedInsertTileDataQuery.run(md5, size, data);
};

const syncImagesTable = async (ctx: ProcessorCtx, um: MokurokuArray) => {
  const {
    db,
    gsiId,
    id,
    tileTransformer,
  } = ctx;
  const totalCount = um.length;
  let insertCount = 0;
  let skipCount = 0;
  const queue = async.queue<MokurokuRow>(
    async (row) => {
      const exists = checkImageTile(db, row[3]);
      if (exists) {
        skipCount += 1;
        return;
      }
      let tileData = await getTileData(`https://cyberjapandata.gsi.go.jp/xyz/${gsiId}/${row[0]}`);
      if (tileTransformer) {
        tileData = await tileTransformer(tileData);
      }
      putTileImageData(
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
    console.timeLog(id, `[タイルダウンロード] remaining=${queue.length()} newlyInserted=${insertCount} skipped=${skipCount} total=${totalCount}`);
  }, 10_000);
  await queue.drain();
  clearInterval(watcher);
  return insertCount;
};

let _preparedInsertTileRefQuery: sqlite3.Statement | undefined;
const insertTileRef = (db: sqlite3.Database, z: number, x: number, y: number, md5: string, updated: number) => {
  const flippedY = y = (1 << z) - 1 - y;
  if (!_preparedInsertTileRefQuery) {
    _preparedInsertTileRefQuery = db.prepare(`
      INSERT INTO tile_ref (zoom_level, tile_column, tile_row, image_md5, updated_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(zoom_level, tile_column, tile_row) DO UPDATE SET
        image_md5 = excluded.image_md5,
        updated_at = excluded.updated_at
      WHERE updated_at <> excluded.updated_at
    `);
  }
  _preparedInsertTileRefQuery.run(z, x, flippedY, md5, updated);
};

const syncTileRefTable = async (ctx: ProcessorCtx, moku: MokurokuArray) => {
  const { db, id } = ctx;
  let currentRow = 0;
  const mokuLen = moku.length;
  for (const row of moku) {
    const [z, x, y] = row[0].substring(0, row[0].length - 4).split('/');
    insertTileRef(db,
      parseInt(z, 10),
      parseInt(x, 10),
      parseInt(y, 10),
      row[3],
      parseInt(row[1], 10)
    );
    currentRow += 1;

    if (currentRow % 10_000 === 0) {
      console.timeLog(id, `[タイル同期] current=${currentRow} total=${mokuLen}`);
    }
  }
}

const writeMetadata = (db: sqlite3.Database, name: string, value: string) => {
  db.prepare(
    `INSERT INTO metadata (name, value) VALUES (?, ?)
    ON CONFLICT (name) DO UPDATE SET
      value = excluded.value`
  ).run(name, value);
};

const setBoundsCenter = (db: sqlite3.Database, minzoom: number, maxzoom: number) => {
  const row = db.prepare(`
    SELECT MAX(tile_column) AS maxx,
    MIN(tile_column) AS minx, MAX(tile_row) AS maxy,
    MIN(tile_row) AS miny FROM tiles
    WHERE zoom_level = ?
  `).get(minzoom);
  const sm = new SphericalMercator({});
  // adapted from https://github.com/mapbox/node-mbtiles/blob/03220bc2fade2ba197ea2bab9cc44033f3a0b37e/lib/mbtiles.js#L347
  const urTile = sm.bbox(row.maxx, row.maxy, minzoom, true);
  const llTile = sm.bbox(row.minx, row.miny, minzoom, true);
  const bounds = [
    llTile[0] > -180 ? llTile[0] : -180,
    llTile[1] > -90 ? llTile[1] : -90,
    urTile[2] < 180 ? urTile[2] : 180,
    urTile[3] < 90 ? urTile[3] : 90
  ] as const;
  const range = maxzoom - minzoom;
  const center = [
    (bounds[2] - bounds[0]) / 2 + bounds[0],
    (bounds[3] - bounds[1]) / 2 + bounds[1],
    range <= 1 ? maxzoom : Math.floor(range * 0.5) + minzoom
  ] as const;
  writeMetadata(db, 'bounds', bounds.join(','));
  writeMetadata(db, 'center', center.join(','));
}

type ProcessorCtx = {
  db: sqlite3.Database;
  id: string;
  gsiId: string;
  minZoom: number;
  maxZoom: number;
  tileTransformer?: TileTransformer;
}

const processor = async (id: string, meta: TilesetSpec, output: string) => {
  // sqlite を用意する
  const db = initDb(output);

  const ctx: ProcessorCtx = {
    db,
    id,
    gsiId: meta.gsiId || id,
    minZoom: meta.minZoom,
    maxZoom: meta.maxZoom,
    tileTransformer: meta.tileTransformer,
  }

  // metadataを確認する（idが一致するか確認。存在しない、かつ、tilesが空の場合は作成。設定しているが、一致しない場合はエラー。）
  verifyTilesetMetadata(db, id);

  // mokuroku.csv をダウンロード
  const mokuroku = await getMokuroku(ctx);
  console.timeLog(id, `mokuroku に ${mokuroku.length} 件のタイルが認識しました`);
  // md5でユニークをかける
  const uniqueMokuroku = mokurokuUniqueTiles(mokuroku);
  console.timeLog(id, `mokuroku に ${uniqueMokuroku.length} 件のユニークなタイルを認識しました。`);

  // imagesテーブルに入っていないmd5をダウンロードし、imagesに挿入
  const newImages = await syncImagesTable(ctx, uniqueMokuroku);
  console.timeLog(id, `${newImages} 件の新しいタイルを mbtiles に格納しました`)

  // [TODO] mokurokuに入っていないimagesを削除（依存関係を確認する必要がある）

  // tile_ref と mokuroku を同期する
  await syncTileRefTable(ctx, mokuroku);

  // metadataテーブル用意
  writeMetadata(db, '_gsi_tileset_id', id);
  writeMetadata(db, 'name', meta.name);

  const fileFormat = mokuroku[0][0].split('.')[1];
  writeMetadata(db, 'format', fileFormat);
  writeMetadata(db, 'minzoom', meta.minZoom.toString());
  writeMetadata(db, 'maxzoom', meta.maxZoom.toString());
  writeMetadata(db, 'version', '1');
  writeMetadata(db, 'attribution', '<a href="https://www.gsi.go.jp/" target="_blank">&copy; GSI Japan</a>');
  setBoundsCenter(db, meta.minZoom, meta.maxZoom);

  db.close();
}

export default processor;

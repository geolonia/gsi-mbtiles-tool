import async from 'async';
import sqlite3 from 'better-sqlite3';
import https from 'https';
import zlib from 'zlib';
import SphericalMercator from '@mapbox/sphericalmercator';
import dayjs from 'dayjs';
import { parse as csvParse } from 'csv-parse';
import { pipeline } from 'stream';
import { TilesetSpec, TileTransformer } from './etc/gsi_tilesets';
import { createDbSql } from './etc/schema';

const initDb = (path: string) => {
  const db = new sqlite3(path);
  db.pragma('journal_mode = WAL');
  db.exec(createDbSql);
  return db;
};

const verifyTilesetMetadata = (db: sqlite3.Database, id: string) => {
  const inputTileset = getMetadata(db, '_gsi_tileset_id');
  if (inputTileset === id) return; // OK to proceed
  if (inputTileset) {
    throw new Error(`この mbtiles は ${inputTileset} と同期していますが、 ${id} と同期しようとしているので、中断します。`)
  }

  const tileCountRow = db.prepare('SELECT count(*) AS "count" FROM tiles').get() as { count: number } | undefined;
  if (tileCountRow?.count === 0) return; // OK to proceed
  throw new Error(`この mbtiles は既に他のタイルが入っているため、中断します。`);
};

type MokurokuRow = [string, string, string, string];
type MokurokuArray = MokurokuRow[];
type MokurokuResp = {
  rows: MokurokuArray,
  lastModified: dayjs.Dayjs,
  status: 'needsUpdate',
} | {
  status: 'upToDate',
}
const getMokuroku = (ctx: ProcessorCtx) => new Promise<MokurokuResp>((res, rej) => {
  const {
    mokurokuId,
    minZoom,
    maxZoom,
  } = ctx;
  const url = `https://cyberjapandata.gsi.go.jp/xyz/${mokurokuId}/mokuroku.csv.gz`;
  https.get(url, (resp) => {
    resp.on('error', rej);

    if (resp.statusCode !== 200) {
      return rej(new Error(`HTTP ${resp.statusCode} ${resp.statusMessage}`));
    }

    const lastModifiedStr = resp.headers['last-modified'];
    const lastModified = lastModifiedStr ? new Date(lastModifiedStr) : new Date();

    if (ctx.inputLastModified && (lastModified <= new Date(ctx.inputLastModified))) {
      return res({
        status: 'upToDate',
      });
    }

    const rows: MokurokuArray = [];
    const csvParser = csvParse();
    pipeline(
      resp,
      zlib.createGunzip(),
      csvParser,
      (err) => {
        if (err) return rej(err);
      },
    )
      .on('data', (row) => {
        const zoom = parseInt(row[0].split('/', 2)[0], 10);
        if (zoom >= minZoom && zoom <= maxZoom) {
          rows.push(row);
        }
      })
      .on('error', (err) => rej(err))
      .on('close', () => res({
        status: 'needsUpdate',
        rows,
        lastModified: dayjs(lastModified),
      }));
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

const syncTileRefTable = (ctx: ProcessorCtx, moku: MokurokuArray) => {
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

const deleteUnusedTiles = (ctx: ProcessorCtx) => {
  const { db, id } = ctx;
  console.timeLog(id, '[タイル削除] 開始');
  db.prepare(`
    DELETE FROM images WHERE md5 IN (
      SELECT i.md5
        FROM images i
        LEFT JOIN tile_ref tr ON tr.image_md5 = i.md5
        WHERE tr.image_md5 IS NULL
    );
  `).run();
  console.timeLog(id, `[タイル削除] 完了`);

  console.timeLog(id, `[VACUUM] 開始`);
  db.prepare('VACUUM').run();
  console.timeLog(id, `[VACUUM] 完了`);
}

const writeMetadata = (db: sqlite3.Database, name: string, value: string) => {
  db.prepare(
    `INSERT INTO metadata (name, value) VALUES (?, ?)
    ON CONFLICT (name) DO UPDATE SET
      value = excluded.value`
  ).run(name, value);
};

const getMetadata = (db: sqlite3.Database, name: string) => {
  const row = db.prepare('SELECT value FROM metadata WHERE name = ?').get(name) as { value: string } | undefined;
  if (!row) {
    return undefined;
  }
  return row.value as string;
};

const setBoundsCenter = (db: sqlite3.Database, minzoom: number, maxzoom: number) => {
  const row = db.prepare(`
    SELECT MAX(tile_column) AS maxx,
    MIN(tile_column) AS minx, MAX(tile_row) AS maxy,
    MIN(tile_row) AS miny FROM tiles
    WHERE zoom_level = ?
  `).get(minzoom) as { maxx: number, minx: number, maxy: number, miny: number };
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
  mokurokuId: string;
  inputLastModified?: string;
}

type ProcessorResult = {
  updated: false;
} | {
  updated: true;
  lastModified: string;
}

async function processor(id: string, meta: TilesetSpec, output: string): Promise<ProcessorResult> {
  // sqlite を用意する
  const db = initDb(output);

  const inputLastModified = getMetadata(db, 'lastModified');

  const ctx: ProcessorCtx = {
    db,
    id,
    gsiId: meta.gsiId || id,
    minZoom: meta.minZoom,
    maxZoom: meta.maxZoom,
    tileTransformer: meta.tileTransformer,
    mokurokuId: meta.mokurokuId || meta.gsiId || id,
    inputLastModified,
  }

  // metadataを確認する（idが一致するか確認。存在しない、かつ、tilesが空の場合は作成。設定しているが、一致しない場合はエラー。）
  verifyTilesetMetadata(db, id);

  // mokuroku.csv をダウンロード
  const mokuroku = await getMokuroku(ctx);

  if (mokuroku.status === 'upToDate') {
    console.timeLog(id, 'mbtiles が mokuroku と同期済みため、処理をスキップします。');
    db.close();
    return { updated: false };
  }

  const mokurokuVersionStr = mokuroku.lastModified.format('YYYYMMDDHHmmss');

  console.timeLog(id, `mokuroku に ${mokuroku.rows.length} 件のタイルが認識しました`);
  // md5でユニークをかける
  const uniqueMokuroku = mokurokuUniqueTiles(mokuroku.rows);
  console.timeLog(id, `mokuroku に ${uniqueMokuroku.length} 件のユニークなタイルを認識しました。`);

  // imagesテーブルに入っていないmd5をダウンロードし、imagesに挿入
  const newImages = await syncImagesTable(ctx, uniqueMokuroku);
  console.timeLog(id, `${newImages} 件の新しいタイルを mbtiles に格納しました`)

  // tile_ref と mokuroku を同期する
  syncTileRefTable(ctx, mokuroku.rows);

  // 使わなくなったタイルを削除する
  deleteUnusedTiles(ctx);

  // metadataテーブル用意
  writeMetadata(db, '_gsi_tileset_id', id);
  writeMetadata(db, 'name', meta.name);

  const fileFormat = mokuroku.rows[0][0].split('.')[1];
  writeMetadata(db, 'format', fileFormat);
  writeMetadata(db, 'minzoom', meta.minZoom.toString());
  writeMetadata(db, 'maxzoom', meta.maxZoom.toString());
  writeMetadata(db, 'version', `1.0.0+${mokurokuVersionStr}`);
  writeMetadata(db, 'lastModified', mokuroku.lastModified.toISOString());
  writeMetadata(db, 'attribution', '<a href="https://www.gsi.go.jp/" target="_blank">&copy; GSI Japan</a>');
  setBoundsCenter(db, meta.minZoom, meta.maxZoom);

  db.pragma('journal_mode = DELETE');
  db.close();

  return { updated: true, lastModified: mokuroku.lastModified.toISOString() };
}

export default processor;

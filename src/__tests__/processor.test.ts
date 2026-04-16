import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import zlib from 'node:zlib';
import { Readable } from 'node:stream';
import {
  initDb,
  verifyTilesetMetadata,
  mokurokuUniqueTiles,
  writeMetadata,
  getMetadata,
  setBoundsCenter,
  checkImageTile,
  putTileImageData,
  insertTileRef,
  getMaxUpdatedAt,
  syncTileRefTable,
  deleteUnusedTiles,
  _resetPreparedStatements,
} from '../processor';
import type { Database } from 'better-sqlite3';
import type { MokurokuArray, ProcessorCtx } from '../processor';

vi.mock('undici', () => ({
  request: vi.fn(),
}));

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'gsi-mbtiles-test-'));
  _resetPreparedStatements();
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function createTestDb(): Database {
  const dbPath = path.join(tmpDir, `test-${Date.now()}-${Math.random().toString(36).slice(2)}.mbtiles`);
  return initDb(dbPath);
}

describe('initDb', () => {
  it('should create a database with the correct schema', () => {
    const db = createTestDb();
    const tables = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`).all() as { name: string }[];
    const tableNames = tables.map(t => t.name);

    expect(tableNames).toContain('metadata');
    expect(tableNames).toContain('images');
    expect(tableNames).toContain('tile_ref');
    db.close();
  });
});

describe('writeMetadata / getMetadata', () => {
  it('should write and read metadata', () => {
    const db = createTestDb();
    writeMetadata(db, 'test_key', 'test_value');
    expect(getMetadata(db, 'test_key')).toBe('test_value');
    db.close();
  });

  it('should return undefined for missing metadata', () => {
    const db = createTestDb();
    expect(getMetadata(db, 'nonexistent')).toBeUndefined();
    db.close();
  });

  it('should upsert metadata on conflict', () => {
    const db = createTestDb();
    writeMetadata(db, 'key', 'value1');
    writeMetadata(db, 'key', 'value2');
    expect(getMetadata(db, 'key')).toBe('value2');
    db.close();
  });
});

describe('verifyTilesetMetadata', () => {
  it('should pass when DB is empty (no tileset id, no tiles)', () => {
    const db = createTestDb();
    expect(() => verifyTilesetMetadata(db, 'test_tileset')).not.toThrow();
    db.close();
  });

  it('should pass when tileset id matches', () => {
    const db = createTestDb();
    writeMetadata(db, '_gsi_tileset_id', 'my_tileset');
    expect(() => verifyTilesetMetadata(db, 'my_tileset')).not.toThrow();
    db.close();
  });

  it('should throw when tileset id does not match', () => {
    const db = createTestDb();
    writeMetadata(db, '_gsi_tileset_id', 'other_tileset');
    expect(() => verifyTilesetMetadata(db, 'my_tileset')).toThrow(/other_tileset/);
    db.close();
  });

  it('should throw when DB has tiles but no tileset id', () => {
    const db = createTestDb();
    const tileData = Buffer.from('data');
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5', 4, tileData);
    db.prepare('INSERT INTO tile_ref (zoom_level, tile_column, tile_row, image_md5, updated_at) VALUES (?, ?, ?, ?, ?)').run(10, 1, 1, 'md5', 123);
    expect(() => verifyTilesetMetadata(db, 'test_tileset')).toThrow(/既に他のタイル/);
    db.close();
  });
});

describe('mokurokuUniqueTiles', () => {
  it('should return empty array for empty input', () => {
    expect(mokurokuUniqueTiles([])).toEqual([]);
  });

  it('should deduplicate rows by md5 (4th column)', () => {
    const input: MokurokuArray = [
      ['10/100/200.png', '1234', '100', 'aaa'],
      ['10/101/201.png', '1235', '100', 'bbb'],
      ['10/102/202.png', '1236', '100', 'aaa'], // duplicate md5
      ['10/103/203.png', '1237', '100', 'ccc'],
    ];
    const result = mokurokuUniqueTiles(input);
    expect(result).toHaveLength(3);
    expect(result.map(r => r[3])).toEqual(['aaa', 'bbb', 'ccc']);
  });

  it('should keep the first occurrence of duplicate md5', () => {
    const input: MokurokuArray = [
      ['10/100/200.png', '1234', '100', 'same_md5'],
      ['10/101/201.png', '1235', '200', 'same_md5'],
    ];
    const result = mokurokuUniqueTiles(input);
    expect(result).toHaveLength(1);
    expect(result[0][0]).toBe('10/100/200.png');
  });
});

describe('checkImageTile / putTileImageData', () => {
  it('should return false when image does not exist', () => {
    const db = createTestDb();
    expect(checkImageTile(db, 'nonexistent')).toBe(false);
    db.close();
  });

  it('should return true after putting image data', () => {
    const db = createTestDb();
    const data = Buffer.from('tile-data');
    putTileImageData(db, 'test_md5', data.length, data);
    expect(checkImageTile(db, 'test_md5')).toBe(true);
    db.close();
  });

  it('should store tile data correctly', () => {
    const db = createTestDb();
    const data = Buffer.from('my-tile-content');
    putTileImageData(db, 'content_md5', data.length, data);

    const row = db.prepare('SELECT * FROM images WHERE md5 = ?').get('content_md5') as { md5: string; tile_size: number; tile_data: Buffer };
    expect(row.md5).toBe('content_md5');
    expect(row.tile_size).toBe(data.length);
    expect(Buffer.from(row.tile_data).toString()).toBe('my-tile-content');
    db.close();
  });
});

describe('insertTileRef', () => {
  it('should insert a tile reference with flipped Y coordinate (TMS)', () => {
    const db = createTestDb();
    const data = Buffer.from('data');
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('ref_md5', 4, data);

    // At zoom 10, y=100 should flip to (2^10 - 1 - 100) = 923
    insertTileRef(db, 10, 50, 100, 'ref_md5', 999);

    const row = db.prepare('SELECT * FROM tile_ref WHERE zoom_level = 10').get() as {
      zoom_level: number; tile_column: number; tile_row: number; image_md5: string; updated_at: number;
    };
    expect(row.tile_column).toBe(50);
    expect(row.tile_row).toBe(923); // (1 << 10) - 1 - 100
    expect(row.image_md5).toBe('ref_md5');
    expect(row.updated_at).toBe(999);
    db.close();
  });

  it('should upsert when updated_at changes', () => {
    const db = createTestDb();
    const data1 = Buffer.from('data1');
    const data2 = Buffer.from('data2');
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('old_md5', 5, data1);
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('new_md5', 5, data2);

    insertTileRef(db, 5, 10, 20, 'old_md5', 100);
    insertTileRef(db, 5, 10, 20, 'new_md5', 200);

    const row = db.prepare('SELECT image_md5, updated_at FROM tile_ref WHERE zoom_level = 5 AND tile_column = 10').get() as {
      image_md5: string; updated_at: number;
    };
    expect(row.image_md5).toBe('new_md5');
    expect(row.updated_at).toBe(200);
    db.close();
  });
});

describe('getMaxUpdatedAt', () => {
  it('should return 0 when tile_ref is empty', () => {
    const db = createTestDb();
    expect(getMaxUpdatedAt(db)).toBe(0);
    db.close();
  });

  it('should return the maximum updated_at value', () => {
    const db = createTestDb();
    const data = Buffer.from('data');
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5_a', 4, data);
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5_b', 4, data);
    db.prepare('INSERT INTO tile_ref (zoom_level, tile_column, tile_row, image_md5, updated_at) VALUES (?, ?, ?, ?, ?)').run(5, 1, 1, 'md5_a', 1000);
    db.prepare('INSERT INTO tile_ref (zoom_level, tile_column, tile_row, image_md5, updated_at) VALUES (?, ?, ?, ?, ?)').run(5, 2, 2, 'md5_b', 2000);
    expect(getMaxUpdatedAt(db)).toBe(2000);
    db.close();
  });
});

describe('syncTileRefTable', () => {
  it('should insert all tile refs on first sync', () => {
    const db = createTestDb();
    const data = Buffer.from('data');
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5_a', 4, data);
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5_b', 4, data);

    console.time('sync_test');
    const moku: MokurokuArray = [
      ['10/50/100.png', '1000', '256', 'md5_a'],
      ['10/51/101.png', '1001', '256', 'md5_b'],
    ];
    const ctx = { db, id: 'sync_test' } as ProcessorCtx;
    syncTileRefTable(ctx, moku);
    console.timeEnd('sync_test');

    const count = db.prepare('SELECT count(*) AS c FROM tile_ref').get() as { c: number };
    expect(count.c).toBe(2);
    db.close();
  });

  it('should skip rows with updated_at <= max(updated_at)', () => {
    const db = createTestDb();
    const data = Buffer.from('data');
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5_a', 4, data);
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5_b', 4, data);
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5_c', 4, data);

    // First sync: insert 2 rows
    console.time('sync_skip1');
    const moku1: MokurokuArray = [
      ['10/50/100.png', '1000', '256', 'md5_a'],
      ['10/51/101.png', '2000', '256', 'md5_b'],
    ];
    const ctx = { db, id: 'sync_skip1' } as ProcessorCtx;
    syncTileRefTable(ctx, moku1);
    console.timeEnd('sync_skip1');

    // Second sync: include old rows + one new row
    _resetPreparedStatements();
    console.time('sync_skip2');
    const moku2: MokurokuArray = [
      ['10/50/100.png', '1000', '256', 'md5_a'], // old, should be skipped
      ['10/51/101.png', '2000', '256', 'md5_b'], // old, should be skipped
      ['10/52/102.png', '3000', '256', 'md5_c'], // new, should be processed
    ];
    const ctx2 = { db, id: 'sync_skip2' } as ProcessorCtx;
    syncTileRefTable(ctx2, moku2);
    console.timeEnd('sync_skip2');

    const count = db.prepare('SELECT count(*) AS c FROM tile_ref').get() as { c: number };
    expect(count.c).toBe(3);

    // Verify the new row was inserted
    const flippedY = (1 << 10) - 1 - 102;
    const newRow = db.prepare('SELECT * FROM tile_ref WHERE zoom_level = 10 AND tile_column = 52').get() as any;
    expect(newRow).toBeDefined();
    expect(newRow.image_md5).toBe('md5_c');
    expect(newRow.updated_at).toBe(3000);

    db.close();
  });

  it('should update existing tile when updated_at changes', () => {
    const db = createTestDb();
    const data = Buffer.from('data');
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5_old', 4, data);
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5_new', 4, data);

    // First sync
    console.time('sync_update1');
    const moku1: MokurokuArray = [
      ['10/50/100.png', '1000', '256', 'md5_old'],
    ];
    const ctx = { db, id: 'sync_update1' } as ProcessorCtx;
    syncTileRefTable(ctx, moku1);
    console.timeEnd('sync_update1');

    // Second sync: same tile, new md5 and updated_at
    _resetPreparedStatements();
    console.time('sync_update2');
    const moku2: MokurokuArray = [
      ['10/50/100.png', '2000', '256', 'md5_new'],
    ];
    const ctx2 = { db, id: 'sync_update2' } as ProcessorCtx;
    syncTileRefTable(ctx2, moku2);
    console.timeEnd('sync_update2');

    const count = db.prepare('SELECT count(*) AS c FROM tile_ref').get() as { c: number };
    expect(count.c).toBe(1);

    const row = db.prepare('SELECT image_md5, updated_at FROM tile_ref WHERE zoom_level = 10').get() as any;
    expect(row.image_md5).toBe('md5_new');
    expect(row.updated_at).toBe(2000);

    db.close();
  });
});

describe('deleteUnusedTiles', () => {
  it('should delete images not referenced by tile_ref', () => {
    const db = createTestDb();
    const data = Buffer.from('data');
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('used_md5', 4, data);
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('unused_md5', 4, data);
    db.prepare('INSERT INTO tile_ref (zoom_level, tile_column, tile_row, image_md5, updated_at) VALUES (?, ?, ?, ?, ?)').run(10, 1, 1, 'used_md5', 123);

    console.time('delete_test');
    const ctx = { db, id: 'delete_test' } as ProcessorCtx;
    deleteUnusedTiles(ctx);
    console.timeEnd('delete_test');

    const images = db.prepare('SELECT md5 FROM images').all() as { md5: string }[];
    expect(images).toHaveLength(1);
    expect(images[0].md5).toBe('used_md5');
    db.close();
  });
});

describe('setBoundsCenter', () => {
  it('should calculate and write bounds and center metadata', () => {
    const db = createTestDb();
    const data = Buffer.from('data');
    // Insert some tiles at zoom 5
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5_1', 4, data);
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5_2', 4, data);
    db.prepare('INSERT INTO tile_ref (zoom_level, tile_column, tile_row, image_md5, updated_at) VALUES (?, ?, ?, ?, ?)').run(5, 10, 12, 'md5_1', 100);
    db.prepare('INSERT INTO tile_ref (zoom_level, tile_column, tile_row, image_md5, updated_at) VALUES (?, ?, ?, ?, ?)').run(5, 15, 18, 'md5_2', 100);

    setBoundsCenter(db, 5, 10);

    const bounds = getMetadata(db, 'bounds');
    const center = getMetadata(db, 'center');

    expect(bounds).toBeDefined();
    expect(center).toBeDefined();

    // bounds should be 4 comma-separated numbers
    const boundsArr = bounds!.split(',').map(Number);
    expect(boundsArr).toHaveLength(4);
    expect(boundsArr[0]).toBeLessThan(boundsArr[2]); // west < east
    expect(boundsArr[1]).toBeLessThan(boundsArr[3]); // south < north

    // center should be 3 comma-separated numbers (lon, lat, zoom)
    const centerArr = center!.split(',').map(Number);
    expect(centerArr).toHaveLength(3);
    expect(centerArr[2]).toBeGreaterThanOrEqual(5);
    expect(centerArr[2]).toBeLessThanOrEqual(10);

    db.close();
  });
});

describe('processor (integration)', () => {
  it('should process mokuroku and create mbtiles', async () => {
    const { request } = await import('undici');
    const { default: processor } = await import('../processor');

    // Create a fake mokuroku CSV (gzipped)
    const csvContent = [
      '5/28/12.png,1700000000,256,fakeMd5abc123',
      '5/29/13.png,1700000001,256,fakeMd5def456',
    ].join('\n');
    const gzippedCsv = zlib.gzipSync(Buffer.from(csvContent));

    // Create a fake 1x1 PNG tile
    const { PNG } = await import('pngjs');
    const png = new PNG({ width: 1, height: 1 });
    png.data[0] = 255; png.data[1] = 0; png.data[2] = 0; png.data[3] = 255;
    const fakeTile = PNG.sync.write(png);

    const mockedRequest = vi.mocked(request);
    mockedRequest.mockImplementation(async (url: any) => {
      const urlStr = url.toString();
      if (urlStr.includes('mokuroku.csv.gz')) {
        return {
          statusCode: 200,
          headers: {
            'last-modified': 'Wed, 15 Nov 2023 00:00:00 GMT',
          },
          body: Readable.from([gzippedCsv]),
        } as any;
      }
      // Tile request
      return {
        statusCode: 200,
        headers: {},
        body: Readable.from([fakeTile]),
      } as any;
    });

    const outputPath = path.join(tmpDir, 'output.mbtiles');
    console.time('std');
    const result = await processor('std', {
      name: 'テスト地図',
      minZoom: 5,
      maxZoom: 5,
      type: 'raster',
    }, outputPath);
    console.timeEnd('std');

    expect(result.updated).toBe(true);
    if (result.updated) {
      expect(result.lastModified).toBeDefined();
    }

    // Verify the output mbtiles
    const sqlite3 = (await import('better-sqlite3')).default;
    const db = new sqlite3(outputPath);

    const tileCount = db.prepare('SELECT count(*) AS c FROM tile_ref').get() as { c: number };
    expect(tileCount.c).toBe(2);

    const imageCount = db.prepare('SELECT count(*) AS c FROM images').get() as { c: number };
    expect(imageCount.c).toBe(2);

    const name = db.prepare('SELECT value FROM metadata WHERE name = ?').get('name') as { value: string };
    expect(name.value).toBe('テスト地図');

    const format = db.prepare('SELECT value FROM metadata WHERE name = ?').get('format') as { value: string };
    expect(format.value).toBe('png');

    db.close();

    vi.restoreAllMocks();
  });
});

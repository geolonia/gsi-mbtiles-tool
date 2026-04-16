import { describe, it, expect } from 'vitest';
import sqlite3 from 'better-sqlite3';
import { createDbSql } from '../etc/schema';

describe('schema', () => {
  it('should create all tables and views in an in-memory database', () => {
    const db = new sqlite3(':memory:');
    db.exec(createDbSql);

    const tables = db.prepare(
      `SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`
    ).all() as { name: string }[];
    const tableNames = tables.map(t => t.name);

    expect(tableNames).toContain('metadata');
    expect(tableNames).toContain('images');
    expect(tableNames).toContain('tile_ref');

    const views = db.prepare(
      `SELECT name FROM sqlite_master WHERE type='view'`
    ).all() as { name: string }[];
    expect(views.map(v => v.name)).toContain('tiles');

    db.close();
  });

  it('should be idempotent (running twice should not error)', () => {
    const db = new sqlite3(':memory:');
    db.exec(createDbSql);
    db.exec(createDbSql);
    db.close();
  });

  it('should allow inserting and querying metadata', () => {
    const db = new sqlite3(':memory:');
    db.exec(createDbSql);

    db.prepare('INSERT INTO metadata (name, value) VALUES (?, ?)').run('test_key', 'test_value');
    const row = db.prepare('SELECT value FROM metadata WHERE name = ?').get('test_key') as { value: string };
    expect(row.value).toBe('test_value');

    db.close();
  });

  it('should allow inserting and querying images', () => {
    const db = new sqlite3(':memory:');
    db.exec(createDbSql);

    const tileData = Buffer.from('fake-tile-data');
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('abc123', tileData.length, tileData);

    const row = db.prepare('SELECT * FROM images WHERE md5 = ?').get('abc123') as { md5: string; tile_size: number; tile_data: Buffer };
    expect(row.md5).toBe('abc123');
    expect(row.tile_size).toBe(tileData.length);

    db.close();
  });

  it('should enforce unique md5 on images', () => {
    const db = new sqlite3(':memory:');
    db.exec(createDbSql);

    const data = Buffer.from('data');
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('dup', 4, data);
    expect(() => {
      db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('dup', 4, data);
    }).toThrow();

    db.close();
  });

  it('should join tile_ref and images through the tiles view', () => {
    const db = new sqlite3(':memory:');
    db.exec(createDbSql);

    const tileData = Buffer.from('tile-content');
    db.prepare('INSERT INTO images (md5, tile_size, tile_data) VALUES (?, ?, ?)').run('md5hash', tileData.length, tileData);
    db.prepare('INSERT INTO tile_ref (zoom_level, tile_column, tile_row, image_md5, updated_at) VALUES (?, ?, ?, ?, ?)').run(10, 100, 200, 'md5hash', 1234567890);

    const tile = db.prepare('SELECT * FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?').get(10, 100, 200) as { zoom_level: number; tile_column: number; tile_row: number; tile_data: Buffer };
    expect(tile).toBeDefined();
    expect(tile.tile_data.toString()).toBe('tile-content');

    db.close();
  });
});

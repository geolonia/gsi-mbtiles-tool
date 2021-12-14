export const createDbSql = `
CREATE TABLE IF NOT EXISTS metadata (
  name text,
  value text
);

CREATE TABLE IF NOT EXISTS images (
  md5 text,
  tile_size integer,
  tile_data blob
);

CREATE TABLE IF NOT EXISTS tile_ref (
  zoom_level INTEGER,
  tile_column INTEGER,
  tile_row INTEGER,
  image_md5 text,
  updated_at integer
);

CREATE UNIQUE INDEX IF NOT EXISTS md5 ON images (md5);
CREATE UNIQUE INDEX IF NOT EXISTS name ON metadata (name);
CREATE UNIQUE INDEX IF NOT EXISTS xyz ON tile_ref (zoom_level, tile_column, tile_row);

CREATE VIEW IF NOT EXISTS tiles AS
  SELECT
    tile_ref.zoom_level AS zoom_level,
    tile_ref.tile_column AS tile_column,
    tile_ref.tile_row AS tile_row,
    images.tile_data AS tile_data
  FROM
    tile_ref
  JOIN images ON images.md5 = tile_ref.image_md5;
`;

import { describe, it, expect } from 'vitest';
import tilesets from '../etc/gsi_tilesets';
import type { TilesetSpec } from '../etc/gsi_tilesets';

describe('gsi_tilesets', () => {
  it('should export a non-empty tilesets object', () => {
    expect(Object.keys(tilesets).length).toBeGreaterThan(0);
  });

  it('should have valid zoom ranges for all tilesets', () => {
    for (const [id, spec] of Object.entries(tilesets)) {
      expect(spec.minZoom, `${id} minZoom`).toBeGreaterThanOrEqual(0);
      expect(spec.maxZoom, `${id} maxZoom`).toBeGreaterThan(spec.minZoom);
      expect(spec.maxZoom, `${id} maxZoom`).toBeLessThanOrEqual(22);
    }
  });

  it('should have a valid type for all tilesets', () => {
    for (const [id, spec] of Object.entries(tilesets)) {
      expect(['vector', 'raster'], `${id} type`).toContain(spec.type);
    }
  });

  it('should have a name for all tilesets', () => {
    for (const [id, spec] of Object.entries(tilesets)) {
      expect(spec.name, `${id} name`).toBeTruthy();
    }
  });

  it('should contain known tileset IDs', () => {
    expect(tilesets).toHaveProperty('experimental_bvmap');
    expect(tilesets).toHaveProperty('dem_png');
    expect(tilesets).toHaveProperty('std');
    expect(tilesets).toHaveProperty('pale');
  });

  it('should have gsiId referencing a valid tileset when specified', () => {
    for (const [id, spec] of Object.entries(tilesets)) {
      if (spec.gsiId) {
        expect(tilesets, `${id} gsiId "${spec.gsiId}" should exist`).toHaveProperty(spec.gsiId);
      }
    }
  });

  it('dem_png_terrain_rgb should have a tileTransformer', () => {
    const spec = tilesets['dem_png_terrain_rgb'];
    expect(spec).toBeDefined();
    expect(spec.tileTransformer).toBeTypeOf('function');
    expect(spec.gsiId).toBe('dem_png');
  });
});

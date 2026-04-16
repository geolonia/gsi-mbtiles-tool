import { describe, it, expect } from 'vitest';
import { PNG } from 'pngjs';
import { transcode } from '../etc/terrain_rgb';

function createGsiDemPng(elevationCm: number): Buffer {
  const png = new PNG({ width: 1, height: 1 });
  // GSI DEM encoding: value = r * 2^16 + g * 2^8 + b
  // For positive elevation in cm: value = elevationCm
  // For negative: value = elevationCm + 2^24
  let value = elevationCm >= 0 ? elevationCm : elevationCm + (2 ** 24);
  const r = (value >> 16) & 0xff;
  const g = (value >> 8) & 0xff;
  const b = value & 0xff;
  png.data[0] = r;
  png.data[1] = g;
  png.data[2] = b;
  png.data[3] = 255; // alpha
  return PNG.sync.write(png);
}

describe('terrain_rgb transcode', () => {
  it('should transcode a 1x1 GSI DEM tile to Terrain RGB format', async () => {
    // 100m = 10000cm
    const input = createGsiDemPng(10000);
    const output = await transcode(input);

    const outPng = PNG.sync.read(output);
    expect(outPng.width).toBe(1);
    expect(outPng.height).toBe(1);

    // Terrain RGB: value = (h + 10000) * 10, encoded as hex RGB
    // h = 10000 * 0.01 = 100m
    // box = (100 + 10000) * 10 = 101000 => hex = "18A88"
    // r=0x01, g=0x8A, b=0x88
    const r = outPng.data[0];
    const g = outPng.data[1];
    const b = outPng.data[2];

    const terrainValue = r * 65536 + g * 256 + b;
    const expectedValue = Math.round(10 * (100 + 10000));
    expect(terrainValue).toBe(expectedValue);
  });

  it('should handle zero elevation', async () => {
    const input = createGsiDemPng(0);
    const output = await transcode(input);

    const outPng = PNG.sync.read(output);
    const r = outPng.data[0];
    const g = outPng.data[1];
    const b = outPng.data[2];

    const terrainValue = r * 65536 + g * 256 + b;
    // h = 0 * 0.01 = 0m, box = (0 + 10000) * 10 = 100000
    expect(terrainValue).toBe(100000);
  });

  it('should handle the special nodata value (-2^23)', async () => {
    // -2^23 = -8388608, which maps to h=0 in the transcode
    const value = -(2 ** 23) + (2 ** 24); // = 2^23 = 8388608
    const input = createGsiDemPng(-(2 ** 23));
    const output = await transcode(input);

    const outPng = PNG.sync.read(output);
    const r = outPng.data[0];
    const g = outPng.data[1];
    const b = outPng.data[2];

    const terrainValue = r * 65536 + g * 256 + b;
    // nodata => h=0, box = (0 + 10000) * 10 = 100000
    expect(terrainValue).toBe(100000);
  });

  it('should handle negative elevation', async () => {
    // -50m = -5000cm
    const input = createGsiDemPng(-5000);
    const output = await transcode(input);

    const outPng = PNG.sync.read(output);
    const r = outPng.data[0];
    const g = outPng.data[1];
    const b = outPng.data[2];

    const terrainValue = r * 65536 + g * 256 + b;
    // h = -5000 * 0.01 = -50m, box = (-50 + 10000) * 10 = 99500
    expect(terrainValue).toBe(99500);
  });
});

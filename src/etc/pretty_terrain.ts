import { PNG } from 'pngjs';
import { streamToBuffer } from '@jorgeferrero/stream-to-buffer';
import { promisify } from 'util';

function mortonDecode3D(mortonCode: number): [number, number, number] {
  function extractBits(value: number, shift: number): number {
      let result = 0;
      for (let i = 0; i < 8; i++) { // each color channel is 8 bits
          result |= ((value >> (i * 3 + shift)) & 1) << i;
      }
      return result;
  }

  const x = extractBits(mortonCode, 0); // Extract bits for X (shift 0)
  const y = extractBits(mortonCode, 1); // Extract bits for Y (shift 1)
  const z = extractBits(mortonCode, 2); // Extract bits for Z (shift 2)

  return [x, y, z];
}

export const transcode = async (buffer: Buffer) => {
  const pngObj = new PNG();
  const png = await promisify(pngObj.parse.bind(pngObj))(buffer);

  for (let y = 0; y < png.height; y++) {
    for (let x = 0; x < png.width; x++) {
      let i = (png.width * y + x) * 4
      let d = png.data[i] * 2 ** 16 +
        png.data[i + 1] * 2 ** 8 +
        png.data[i + 2]
      let h = (d < 2 ** 23) ? d : d - 2 ** 24
      if (h == - (2 ** 23)) {
        h = 0
      } else {
        h *= 0.01
      }

      let val = Math.round(10 * (h + 10000));
      let [boxr, boxg, boxb] = mortonDecode3D(val);

      // let box = Math.round(10 * (h + 10000)).toString(16)
      // let boxr = parseInt(box.slice(-6, -4), 16)
      // let boxg = parseInt(box.slice(-4, -2), 16)
      // let boxb = parseInt(box.slice(-2), 16)

      // let encoded = mortonEncode3D(boxr, boxg, boxb);
      // boxr = (encoded >> 16) & 0xff;
      // boxg = (encoded >> 8) & 0xff;
      // boxb = encoded & 0xff;

      png.data[i] = boxr
      png.data[i + 1] = boxg
      png.data[i + 2] = boxb
    }
  }

  const outBuffer = await streamToBuffer(pngObj.pack());
  return outBuffer;
};

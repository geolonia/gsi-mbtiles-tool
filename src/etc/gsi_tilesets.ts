export interface TilesetSpec {
  name: string
  minZoom: number
  maxZoom: number
  type: "vector" | "raster"
}

/**
 * 国土地理院が提供するタイルセットのIDをキーに、メタデータを保管しています。
 */
const tilesets: { [id: string]: TilesetSpec } = {
  "experimental_bvmap": {
    name: "地理院地図Vector",
    minZoom: 4,
    maxZoom: 16,
    type: "vector",
  },
  "relief": {
    name: "色別標高図",
    minZoom: 5,
    maxZoom: 15,
    type: "raster",
  },
  "hillshademap": {
    name: "陰影起伏図",
    minZoom: 2,
    maxZoom: 16,
    type: "raster",
  },
  "earthhillshade": {
    name: "陰影起伏図（全球版）",
    minZoom: 0,
    maxZoom: 8,
    type: "raster",
  },
  "20150911dol": {
    name: "口永良部島の火山活動 UAV撮影による正射画像（2015年9月8,11,12日撮影）",
    minZoom: 14,
    maxZoom: 18,
    type: "raster",
  }
};

export default tilesets;

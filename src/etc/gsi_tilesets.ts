import { transcode as terrainRgbTranscode } from "./terrain_rgb"

export type TileTransformer = (tile: Buffer) => Promise<Buffer>

export interface TilesetSpec {
  name: string
  minZoom: number
  maxZoom: number
  type: "vector" | "raster"

  /** `tilesets`のIDがGSI IDと一致しない時に利用（エイリアス） */
  gsiId?: string

  /** タイルデータを保存する前に処理する場合 */
  tileTransformer?: TileTransformer
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
  "dem_png": {
    name: "標高タイル（基盤地図情報数値標高モデル）",
    minZoom: 1,
    maxZoom: 14,
    type: "raster",
  },
  "dem_png_terrain_rgb": {
    name: "標高タイル（基盤地図情報数値標高モデル） - Terrain RGB",
    minZoom: 1,
    maxZoom: 14,
    type: "raster",
    gsiId: "dem_png",
    tileTransformer: terrainRgbTranscode,
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

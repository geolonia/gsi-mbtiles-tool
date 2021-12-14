import { Command } from 'commander';
import tilesets from '../etc/gsi_tilesets';
import processor from '../processor';
const program = new Command();

interface CliOptions {
  output: string
}

async function run(inputName: string, options: CliOptions) {
  const name = inputName in tilesets && inputName;
  if (!name) {
    throw new Error(`'tileset-id' must be one of: ${Object.keys(tilesets).join(', ')}`);
  }
  const tilesetMeta = tilesets[name];
  console.time(name);
  console.timeLog(name, `Starting up ${tilesetMeta.name}...`);

  await processor(name, options.output);

  console.timeEnd(name);
};

async function main() {
  program
    .argument('<tileset-id>', 'GSIのタイルセットID')
    .option('-o, --output <output>', '出力、または更新するファイル', './out.mbtiles')
    .showHelpAfterError()
    .action(run);
  await program.parseAsync(process.argv);
};

main().catch(e => {
  console.error('An error occurred:', e);
  process.exit(1);
});

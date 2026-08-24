// Pre-bundles workflow code at build time (Lambda cold starts shouldn't pay
// webpack bundling cost on every invocation). Run after `npm run build`,
// since it bundles the compiled output, not the TS source.
//
// Output consumed by worker-lambda.ts via workflowBundle: { codePath }.
const { bundleWorkflowCode } = require('@temporalio/worker');
const fs = require('node:fs');
const path = require('node:path');

async function main() {
  const { code } = await bundleWorkflowCode({
    workflowsPath: require.resolve('../../out/ts/src/workflows'),
  });
  const outPath = path.join(__dirname, '../../out/workflow-bundle.js');
  fs.writeFileSync(outPath, code);
  console.log(`wrote workflow bundle to ${outPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

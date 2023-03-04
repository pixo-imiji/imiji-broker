const { exec } = require("child_process");

const run = (cmd) => new Promise((resolve, reject) => exec(cmd, err => err ? reject(err) : resolve()));

const version = require("../package.json").version;
const m = "-SNAPSHOT.";
const [fixVersion, n] = version.split(m);
const incrementSnapshot = (BigInt(n ? n : 0) + BigInt(1)).toString();
const newSnapshotVersion = fixVersion + m + incrementSnapshot;

(async () => {
  try {
    await run(`npm run build`);
    await run(`npm version ${newSnapshotVersion}`);
    await run("git push");
    await run(`npm publish --access public`);
    console.log("done");
  } catch (err) {
    console.error(err);
  }
})();

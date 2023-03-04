const { exec } = require("child_process");
const bumpVersion = require("semver-increment");

const run = (cmd) => new Promise((resolve, reject) => exec(cmd, err => err ? reject(err) : resolve()));

const version = require("../package.json").version;
const m = "-SNAPSHOT.";
const [fixVersion, n] = version.split(m);
const newFixVersion = bumpVersion([0, 0, 1], fixVersion);
const newSnapshotVersion = newFixVersion + m + n;

(async () => {
  try {
    await run(`npm run build`);
    await run(`npm version ${bumpVersion([0, 0, 1], fixVersion)}`);
    await run("git push");
    await run(`npm publish --access public`);
    await run(`npm version ${newSnapshotVersion}`);
    await run("git push");
    console.log("done");
  } catch (err) {
    console.error(err);
  }
})();

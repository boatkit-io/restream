const cjsPackage = require('@boatkit-io/restream');

async function main() {
  const esmPackage = await import('@boatkit-io/restream');

  for (const [label, pkg] of [
    ['cjs', cjsPackage],
    ['esm', esmPackage],
  ]) {
    if (typeof pkg.BinaryReader !== 'function') {
      throw new Error(`${label} BinaryReader export is missing`);
    }
    if (typeof pkg.BinaryWriter !== 'function') {
      throw new Error(`${label} BinaryWriter export is missing`);
    }
    if (pkg.SerializationType.Uint8 !== esmPackage.SerializationType.Uint8) {
      throw new Error(`${label} SerializationType export is inconsistent`);
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

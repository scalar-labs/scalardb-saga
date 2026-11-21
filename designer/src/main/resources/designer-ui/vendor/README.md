# Vendored JavaScript

The designer's page loads these libraries from here, not from a CDN, so it works offline and every
upgrade is a reviewed diff. The files are copied unchanged from the npm packages, with each
package's license.

| Library | Version | Files | License |
| --- | --- | --- | --- |
| [Preact](https://preactjs.com) | 10.29.8 | `dist/preact.module.js`, `hooks/dist/hooks.module.js` and their source maps | MIT |
| [htm](https://github.com/developit/htm) | 3.1.1 | `dist/htm.module.js` | Apache-2.0 |

Dependabot cannot see these files, so they are updated by hand:

1. Download the new package tarball from `https://registry.npmjs.org/<package>/-/<package>-<version>.tgz`
   and check it against the `dist.integrity` value the registry lists for that version.
2. Copy the same files into a folder named `<package>-<version>`, with the package's `LICENSE`, and
   delete the old folder.
3. Update the paths in the import map in `../index.html`, and the table above.

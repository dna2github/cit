const i_fs = require('fs');
const i_path = require('path');

IGNORE_DIRS = ['.git', '.p4', '.cit'];

async function walk(path, oneFn) {
   const base = i_path.resolve(path);
   await expand(base, base, oneFn);

   async function expand(path, base, oneFn) {
      const stat = i_fs.lstatSync(path);
      if (stat.isDirectory()) {
         const basename = i_path.basename(path);
         if (IGNORE_DIRS.includes(basename)) return;
         const items = i_fs.readdirSync(path);
         if (items.length) items.sort((a, b) => (a>b?1:(a<b?-1:0)));
         for (let i = 0, n = items.length; i < n; i++) {
            const name = items[i];
            await expand(i_path.join(path, name), base, oneFn);
         }
      } else {
         oneFn(path, base);
      }
   }
}

module.exports = {
   walk,
}

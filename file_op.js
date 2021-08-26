const i_fs = require('fs');
const i_path = require('path');
const i_crypto = require('crypto');

IGNORE_DIRS = ['.git', '.p4', '.cit'];

async function Walk(path, oneFn) {
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
         await oneFn(path, base);
      }
   }
}

async function MkdirP(path) {
   const parent = i_path.dirname(path);
   if (parent === path) return; // @root
   try {
      const stat = i_fs.statSync(parent);
      if (!stat.isDirectory()) throw new Error('EINVALID');
   } catch (err) {
      if (err && err.code === 'ENOENT') {
         await mkdirP(parent);
      } else {
         throw err;
      }
   }
   try {
      const stat = i_fs.statSync(path);
      if (!stat.isDirectory()) throw new Error('EINVALID');
   } catch (err) {
      if (err && err.code === 'ENOENT') {
         i_fs.mkdirSync(path);
      } else {
         throw err;
      }
   }
}

async function Hash(path) {
   return new Promise((r, e) => {
      const input = i_fs.createReadStream(path);
      const hash = i_crypto.createHash('sha256');
      input.on('readable', () => {
         const data = input.read();
         if (data) {
            hash.update(data);
         } else {
            input.close();
            r(hash.digest('hex'));
         }
      });
      input.on('error', (err) => e(err))
   });
}

async function IsBinaryFile(path) {
   return new Promise((r, e) => {
      i_fs.open(path, (err, fd) => {
         if (err) return e(err);
         const stat = i_fs.statSync(path);
         let n = stat.size;
         if (n > 1024 * 1024) n = 1024 * 1024;
         const buf = Buffer.alloc(1024 * 1024);
         i_fs.read(fd, { bufffer: buf, length: n }, (err, n, raw) => {
            if (err) return e(err);
            const probe = raw.slice(0, n).toString();
            const isbinary = probe.toString().indexOf('\x00') >= 0;
            i_fs.close(fd, (err) => {
               r(isbinary);
            });
         });
      });
   });
}

module.exports = {
   Walk,
   MkdirP,
   Hash,
   IsBinaryFile,
}

const i_fs = require('fs');

const CACHE_N = 1024 * 1024;

class LineReader {
   constructor(filename, opt) {
      this.opt = opt || {};
      this.filename = filename;
      this.fd = -1;
      this.buf = Buffer.alloc(this.opt.cacheN || CACHE_N);
      this.eof = false;
      this.cache = [Buffer.alloc(0)];
   }

   Eof() {
      return this.eof;
   }

   async Open() {
      if (this.fd >= 0) return Promise.resolve();
      return new Promise((r, e) => {
         i_fs.open(this.filename, (err, fd) => {
            if (err) {
               if (err.code === 'ENOENT') {
                  this.eof = true;
                  r();
               }
               return e(err);
            }
            this.fd = fd;
            r();
         });
      });
   }

   async NextLine() {
      await this.Open();
      if (this.cache && this.cache.length > 1) {
         return Promise.resolve(this.cache.shift());
      }
      if (this.eof) return Promise.resolve(null);
      await this.fillCache();
      if (this.cache.length <= 1) {
         if (!this.cache[0] || this.cache[0].length === 0) {
            // empty file
            this.eof = true;
            return Promise.resolve(null);
         }
         // TODO: deal with too long line
         //       if change `if` to `while`,
         //       do N times to allow cache size * N length line
         return Promise.reject('too long line');
      }
      return Promise.resolve(this.cache.shift());
   }

   async fillCache() {
      return new Promise((r, e) => {
         const lastLine = this.cache.pop();
         i_fs.read(this.fd, { buffer: this.buf, length: this.buf.length }, (err, n, raw) => {
            if (err) return e(err);
            if (n < this.buf.length) {
               this.eof = true;
            }
            if (n === 0) return r();
            const buf = Buffer.concat([lastLine, raw.slice(0, n)])
            const lines = buf.toString().split('\n');
            const incompleteLine = lines.pop();
            lines.forEach((line) => this.cache.push(line));
            const postN = Buffer.from(incompleteLine).length;
            this.cache.push(buf.slice(buf.length - postN, buf.length));
            if (this.eof) {
               const remains = this.cache.pop();
               this.cache.push(remains.toString());
               this.cache.push(Buffer.alloc(0));
            }
            r();
         });
      });
   }

   async Close() {
      if (this.fd >= 0) {
         return new Promise((r, e) => {
            i_fs.close(this.fd, (err) => {
               if (err) return e(err); else r();
            });
         });
      }
      return Promise.resolve();
   }
}

class LineWriter {
   constructor(filename, opt) {
      this.filename = filename;
      this.fd = -1;
   }

   async Open() {
      if (this.fd >= 0) return Promise.resolve();
      return new Promise((r, e) => {
         i_fs.open(this.filename, 'w+', (err, fd) => {
            if (err) return e(err);
            this.fd = fd;
            r();
         });
      });
   }

   async Close() {
      if (this.fd >= 0) {
         return new Promise((r, e) => {
            i_fs.close(this.fd, (err) => {
               if (err) return e(err); else r();
            });
         });
      }
      return Promise.resolve();
   }

   async WriteLine(line) {
      if (this.fd <= 0) return false;
      return new Promise((r, e) => {
         i_fs.write(this.fd, Buffer.from(line + '\n'), (err, _n, _raw) => {
            if (err) return e(err);
            r(true);
         });
      });
   }
}

module.exports = {
   LineReader,
   LineWriter,
};

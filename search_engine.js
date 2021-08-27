const i_fs = require('fs');
const i_sqlite = require('sqlite3');

/* outDir:
   - files
     - (hid, hash)
     - (pid, path)
     - (hid, pid)
   - source_index
     - (tid, a, b, c)
     - (tid, hid, position)
   - path_index
     - (tid, pid, position)
   - symbol_index
     - (sid, symbol)
     - (tid, sid, position)
*/

class Database {
   constructor(filename) {
      this.filename = filename;
      this.db = new i_sqlite.Database(filename);
   }

   async CreateTable(name, fields) {
      return new Promise((r, e) => {
         const sql = `CREATE TABLE ${name} (${Object.keys(fields).map(
            (key) => `${key} ${fields[key]}`
         ).join(',')})`
         this.db.run(sql, (err) => {
            if (err) return e(err);
            r();
         });
      });
   }

   async InsertInto(name, keyval) {
      return new Promise((r, e) => {
         this.db.serialize(() => {
            const fields = Object.keys(keyval);
            const values = fields.map((x) => keyval[x]);
            const sql = `INSERT INTO ${name} (${fields.join(',')}) VALUES (${fields.map((x) => '?').join(',')})`;
            const stm = this.db.prepare(sql);
            stm.run(...values);
            stm.finalize();
            const q = `SELECT LAST_INSERT_ROWID()`;
            this.db.get(q, (err, row) => {
               if (err) return e(err);
               r(row['LAST_INSERT_ROWID()']);
            });
         });
      });
   }

   async DeleteFrom(name, keyval) {
      return new Promise((r, e) => {
         const fields = Object.keys(keyval);
         const values = fields.map((x) => keyval[x]);
         const sql = `DELETE FROM ${name} WHERE ${fields.map((x) => `${x} = ?`).join(' AND ')}`;
         const stm = this.db.prepare(sql);
         stm.run(...values);
         stm.finalize();
         r();
      });
   }

   async Exists(name, keyval) {
      return new Promise((r, e) => {
         const fields = Object.keys(keyval);
         const values = fields.map((x) => keyval[x]);
         const sql = `SELECT COUNT(*) FROM ${name} WHERE ${fields.map((x) => `${x} = ?`).join(' AND ')}`;
         const stm = this.db.prepare(sql);
         stm.get(...values, (err, row) => {
            stm.finalize();
            if (err) return e(err);
            r(row['COUNT(*)']);
         });
      });
   }

   async Get(name, keyval) {
      return new Promise((r, e) => {
         const fields = Object.keys(keyval);
         const values = fields.map((x) => keyval[x]);
         const sql = `SELECT * FROM ${name} WHERE ${fields.map((x) => `${x} = ?`).join(' AND ')}`;
         const stm = this.db.prepare(sql);
         stm.get(...values, (err, row) => {
            stm.finalize();
            if (err) return e(err);
            r(row);
         });
      });
   }
   async GetAll(name, keyval, opt) {
      opt = opt || {};
      opt.offset = opt.offset || 0;
      opt.limit = opt.limit || 10000;
      return new Promise((r, e) => {
         const fields = Object.keys(keyval);
         const values = fields.map((x) => keyval[x]);
         const sql = `SELECT * FROM ${name} WHERE ${fields.map((x) => `${x} = ?`).join(' AND ')} LIMIT ${opt.offset}, ${opt.limit}`;
         const stm = this.db.prepare(sql);
         stm.all(values, (err, rows) => {
            stm.finalize();
            if (err) return e(err);
            r(row);
         });
      });
   }

   async Run(sql, values) {
      const stm = this.db.prepare(sql);
      stm.run(...values);
      stm.finalize();
   }
   async RunGet(sql, values) {
      return new Promise((r , e) => {
         const stm = this.db.prepare(sql);
         stm.get(values, (err, row) => {
            if (err) return e(err);
            r(row);
            stm.finalize();
         });
      });
   }
   async RunGetAll(sql, values) {
      return new Promise((r , e) => {
         const stm = this.db.prepare(sql);
         stm.all(values, (err, rows) => {
            if (err) return e(err);
            r(rows);
            stm.finalize();
         });
      });
   }

   GetDB() { return this.db; }
}

async function Init(db) {
   await db.CreateTable('file_hash', {
      hid: 'INTEGER PRIMARY KEY',
      hash: 'VARCHAR(128)'
   });
   await db.CreateTable('file_path', {
      pid: 'INTEGER PRIMARY KEY',
      path: 'VARCHAR(512)'
   });
   await db.CreateTable('file_hash_path', {
      hid: 'INTEGER',
      pid: 'INTEGER'
   });
   await db.CreateTable('trigram', {
      tid: 'INTEGER PRIMARY KEY',
      a: 'VARCHAR(1)',
      b: 'VARCHAR(1)',
      c: 'VARCHAR(1)'
   });
   await db.CreateTable('index_src', {
      tid: 'INTEGER',
      hid: 'INTEGER',
      p: 'INTEGER'
   });
   await db.CreateTable('index_path', {
      tid: 'INTEGER',
      pid: 'INTEGER',
      p: 'INTEGER'
   });
   await db.CreateTable('symbol', {
      sid: 'INTEGER PRIMARY KEY',
      hid: 'INTEGER',
      symbol: 'VARCHAR(100)'
   });
   await db.CreateTable('index_sym', {
      tid: 'INTEGER',
      sid: 'INTEGER',
      p: 'INTEGER'
   });
}

async function IndexAdd(db, path, baseDir, newHash) {
   // e.g. path=/path/to/base/file1
   //      baseDir=/path/to/base
   //      rpath=/file1
   const rpath = path.substring(baseDir.length);
   const pobj = (await db.Get('file_path', { path: rpath })) || {};
   if (!pobj.pid) {
      pobj.pid = await db.InsertInto('file_path', { path: rpath });
      pobj.path = rpath;
   }
   const hobj = (await db.Get('file_hash', { hash: newHash })) || {};
   if (hobj.hid) {
      const phobj = await db.Get('file_hash_path', { hid: hobj.hid, pid: pobj.pid });
      if (!phobj) {
         await db.InsertInto('file_hash_path', { hid: hobj_hid, pid: pobj.pid });
      }
      return true;
   }
   hobj.hid = await db.InsertInto('file_hash', { hash: newHash });
   hobj.hash = newHash;
   const tidcache = {};
   const text = i_fs.readFileSync(path).toString();
   await indexText(db, 'index_src', text, hobj.hid, tidcache);
   await indexText(db, 'index_path', rpath, pobj.pid, tidcache);
   // TODO: get symbols and insert into symbol/index_sym
}
const tbxidmap = {
   'index_src': 'hid',
   'index_sym': 'sid',
   'index_path': 'pid',
}
async function indexText(db, tb, text, id, tidcache) {
   const n = text.length;
   const idname = tbxidmap[tb];
   if (n < 3) return false;
   for (let i = 0; i <= n-3; i++) {
      const t = text.substring(i, i+3);
      let tid = tidcache[t];
      if (!tid) {
         tobj = await db.Get('trigram', { a: t.charAt(0), b: t.charAt(1), c: t.charAt(2) });
         if (tobj) {
            tid = tobj.tid;
         } else {
            tid = await db.InsertInto('trigram', { a: t.charAt(0), b: t.charAt(1), c: t.charAt(2) });
         }
         tidcache[t] = tid;
      }
      const values = { tid, p: i };
      values[idname] = id;
      await db.InsertInto(tb, values);
   }
   return true;
}

async function IndexDel(db, path, baseDir, oldHash) {
   const rpath = path.substring(baseDir.length);
   const pobj = await db.Get('file_path', { path: rpath });
   if (!pobj) return false;
   await db.DeleteFrom('file_path', { pid: pobj.pid });
   await db.DeleteFrom('index_path', { pid: pobj.pid });
   const hobj = await db.Get('file_hash', { hash: oldHash });
   if (!hobj) return false;
   const n = await db.Exists('file_hash_path', { hid: hobj.hid });
   if (n > 1) {
      await db.DeleteFrom('file_hash_path', { hid: hobj.hid, pid: pobj.pid });
      return true;
   } else if (n === 1) {
      await db.DeleteFrom('file_hash_path', { hid: hobj.hid });
      await db.DeleteFrom('file_hash', { hid: hobj.hid });
      await db.DeleteFrom('index_src', { hid: hobj.hid });
      await db.Run('DELETE FROM index_sym WHERE sid IN (SELECT sid FROM symbol WHERE hid = ?)', [hobj.hid]);
      await db.DeleteFrom('symbol', { hid: hobj.hid });
      return true;
   } else {
      await db.DeleteFrom('file_hash', { hid: hobj.hid });
      return false;
   }
}

async function SearchText(db, tb, query, n) {
   const qn = query.length;
   if (qn < 3) {
      // TODO: use trigram to random pick axy, xay, xya, abx and xab
      return [];
   }
   const tida = await db.Get('trigram', { a: query.charAt(0), b: query.charAt(1), c: query.charAt(2) });
   if (!tida) return [];
   if (qn === 3) {
      return ((await db.RunGetAll(
         `SELECT DISTINCT hid FROM ${tb} WHERE tid = ? LIMIT ?`, [tida.tid, n]
      )) || []).map((x) => x.hid);
   }
   const tidb = await db.Get('trigram', { a: query.charAt(qn-3), b: query.charAt(qn-2), c: query.charAt(qn-1) });
   if (!tidb) return [];
   const env = { as: [], bs: [], anext: true, bnext: true, ao: 0, bo: 0, dp0: qn - 3, r: [] };
   while (env.r.length < n && (env.anext || env.bnext)) {
      await iterateMatches(env);
   }
   // TODO: here output contains merely possible
   //       search: `test` ---> match: `tinxpest`, `tingwest`, ...
   return env.r;

   async function iterateMatches(env) {
      pageN = 5000;
      if (env.anext && !env.as.length) {
         const tmpa = await db.RunGetAll(`SELECT hid, p FROM ${tb} WHERE tid = ? ORDER BY hid, p LIMIT ?, ?`, [tida.tid, env.ao, pageN]);
         if (tmpa.length < pageN) {
            env.anext = false;
         } else {
            env.ao += pageN;
         }
         env.as = env.as.concat(tmpa);
      }
      if (env.bnext && !env.bs.length) {
         const tmpb = await db.RunGetAll(`SELECT hid, p FROM ${tb} WHERE tid = ? ORDER BY hid, p LIMIT ?, ?`, [tidb.tid, env.bo, pageN]);
         if (tmpb.length < pageN) {
            env.bnext = false;
         } else {
            env.bo += pageN;
         }
         env.bs = env.bs.concat(tmpb);
      }
      let last_hid = -1;
      while (env.as.length && env.bs.length) {
         let a = env.as[0], b = env.bs[0];
         if (a.hid === last_hid) {
            env.as.shift();
            continue;
         }
         if (b.hid === last_hid) {
            env.bs.shift();
            continue;
         }
         if (a.hid === b.hid) {
            const d = (b.p - a.p) - env.dp0;
            if (d === 0) {
               last_hid = a.hid;
               env.r.push(a.hid);
               env.as.shift();
               env.bs.shift();
            } else if (d > 0) {
               env.as.shift();
               continue;
            } else /* d < 0 */ {
               env.bs.shift();
               continue;
            }
         } else if (a.hid > b.hid) {
            env.bs.shift();
            continue;
         } else /* if (a.hid < b.hid) */ {
            env.as.shift();
            continue;
         }
      }
   }
}

module.exports = {
   Database,
   Init,
   IndexAdd,
   IndexDel,
   SearchText,
};

/*async function main() {
   const i_path = require('path');
   const xxx = new Database(process.argv[2]);
   // await Init(xxx);
   // const input = i_path.resolve(process.argv[3]);
   // await IndexAdd(xxx, input, i_path.dirname(input), 'test');
   console.log(await SearchText(xxx, 'index_src', process.argv[3], 50));
   console.log('done.');
}
main();*/
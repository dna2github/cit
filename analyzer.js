const i_fs = require('fs');
const i_path = require('path');
const i_lf = require('./large_file');
const i_fo = require('./file_op');
const i_se = require('./search_engine');

function AnalyzeProject(srcRoot, outDir, opt) {
   srcRoot = i_path.resolve(srcRoot);
   outDir = i_path.resolve(outDir);
   return new Promise(async (r, e) => {
      await i_fo.MkdirP(outDir);
      await scanLatest();
      await detectChanges();
      await analyzeFiles();
      // TODO: handle the array `error`
      r();
   });

   async function scanLatest() {
      // scan current files
      // out: { p: path, m: mtime }
      const outNew = new i_lf.LineWriter(i_path.join(outDir, '_new'));
      await outNew.Open();
      await i_fo.Walk(srcRoot, async (path, base) => {
         const rp = path.substring(base.length);
         const stat = i_fs.statSync(path);
         await outNew.WriteLine(JSON.stringify({ p: rp, m: stat.mtimeMs }));
      });
      await outNew.Close();
   }

   async function detectChanges() {
      // diff(/outdir/cur, /outdir/new)
      // out: { p: path, a: action(a=added,d=deleted,u=updated) }
      //   --> /outdir/changelist
      const oldP = i_path.join(outDir, '_cur');
      const newP = i_path.join(outDir, '_new');
      const outNewWithHashP = i_path.join(outDir, '_newh');
      const outP = i_path.join(outDir, '_changelist');
      const outF = new i_lf.LineWriter(outP);
      const outNewWithHashF = new i_lf.LineWriter(outNewWithHashP);
      const oldF = new i_lf.LineReader(oldP);
      const newF = new i_lf.LineReader(newP);
      // TODO: use try...catch... to wrap io operations
      await outF.Open();
      await outNewWithHashF.Open();
      await oldF.Open();
      await newF.Open();
      let oldL = await oldF.NextLine();
      let newL = await newF.NextLine();
      let nextOld = false, nextNew = false;
      // merge 2 files and get change list
      while (oldL !== null || newL !== null ) {
         nextOld = false;
         nextNew = false;
         if (oldL === null) {
            // all remains added
            if (newL.length) {
               const objNew = JSON.parse(newL);
               const hash = await getFileHash(objNew.p);
               await outNewWithHashF.WriteLine(
                  JSON.stringify(Object.assign({ h: hash }, objNew))
               );
               await outF.WriteLine(
                  JSON.stringify(Object.assign({ a: 'a', h_: hash }, objNew))
               );
            }
            nextNew = true;
         } else if (newL === null) {
            // all remains deleted
            if (oldL.length) {
               const objOld = JSON.parse(oldL);
               await outF.WriteLine(
                  JSON.stringify(Object.assign({ a: 'd' }, objOld))
               );
            }
            nextOld = true;
         } else if (!oldL.length || !newL.length) {
            nextOld = !oldL.length;
            nextNew = !newL.length;
         } else {
            const objOld = JSON.parse(oldL);
            const objNew = JSON.parse(newL);
            if (objOld.p > objNew.p) {
               const hash = await getFileHash(objNew.p);
               await outNewWithHashF.WriteLine(
                  JSON.stringify(Object.assign({ h: hash }, objNew))
               );
               await outF.WriteLine(
                  JSON.stringify(Object.assign({ a: 'a', h_: hash }, objNew))
               );
               nextNew = true;
            } else if (objOld.p < objNew.p) {
               await outF.WriteLine(
                  JSON.stringify(Object.assign({ a: 'd' }, objOld))
               );
               nextOld = true;
            } else {
               const hash = await getFileHash(objNew.p);
               await outNewWithHashF.WriteLine(
                  JSON.stringify(Object.assign({ h: hash }, objNew))
               );
               if (objOld.m !== objNew.m) {
                  await outF.WriteLine(
                     JSON.stringify(Object.assign({ a: 'u', h_: hash }, objOld))
                  );
               }
               nextOld = true;
               nextNew = true;
            }
         }
         if (nextOld) oldL = await oldF.NextLine();
         if (nextNew) newL = await newF.NextLine();
      }
      await outF.Close();
      await outNewWithHashF.Close();
      await oldF.Close();
      await newF.Close();
      await i_fs.renameSync(outNewWithHashP, oldP);
      await i_fs.unlinkSync(newP);
   }

   async function getFileHash(item) {
      return await i_fo.Hash(i_path.join(srcRoot, item));
   }

   async function analyzeFiles() {
      // TODO: read(/outdir/changelist)
      //       for each file
      //       - index add/del/update
      const changeP = i_path.join(outDir, '_changelist');
      const indexP = i_path.join(outDir, '_index.db');
      const needInit = !i_fs.existsSync(indexP);
      const indexDB = new i_se.Database(indexP);
      if (needInit) await i_se.Init(indexDB);
      const changeF = new i_lf.LineReader(changeP);
      await changeF.Open();
      let line;
      while ((line = await changeF.NextLine()) !== null) {
         if (!line) continue;
         const obj = JSON.parse(line);
         const path = i_path.join(srcRoot, obj.p);
         const is_binary = obj.a==='d'?true:await i_fo.IsBinaryFile(path);
         switch (obj.a) {
            case 'u':
               console.log('update', obj, is_binary);
               if (!is_binary) {
                  await i_se.IndexDel(indexDB, path, srcRoot, obj.h);
                  await i_se.IndexAdd(indexDB, path, srcRoot, obj.h_);
               }
               break;
            case 'd':
               console.log('delete', obj);
               await i_se.IndexDel(indexDB, path, srcRoot, obj.h);
               break;
            case 'a':
               console.log('append', obj, is_binary);
               if (!is_binary) {
                  await i_se.IndexAdd(indexDB, path, srcRoot, obj.h_);
               }
               break;
            default:
               throw `unknown action type: ${obj.a}`;
         }
      }
      await changeF.Close();
      await i_fs.unlinkSync(changeP);
   }
}

module.exports = {
   AnalyzeProject,
};

if (require.main === module) {
   const srcDir = i_path.resolve(process.argv[2]);
   const outDir = i_path.resolve(process.argv[3]);
   console.log(srcDir, outDir);
   AnalyzeProject(srcDir, outDir);
}
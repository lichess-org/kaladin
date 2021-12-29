import { Dbs, withDbs } from './dbs';
import fetch from 'node-fetch';
import { Stream } from 'stream';
const transform = require('parallel-transform');
import config from './config';
import * as util from './util';

async function fetchInsights(dbs: Dbs) {
  // if ((await dbs.dest.insight.count({})) > 0) dbs.dest.insight.drop();
  await dbs.dest.insight.createIndex({ u: 1 });
  const selector = { hasMoves: true, hasInsights: { $ne: true }, marks: { $ne: 'engine' } };
  const projection = { _id: 1, marks: 1, modlog: 1 };
  const total = await dbs.dest.user.count(selector);
  let nb = 0;
  const userSource = new Stream.Readable({
    objectMode: true,
    read() {},
  });
  util.drain(
    'insight',
    dbs.dest.user.aggregate([{ $match: selector }, { $project: projection }, { $sample: { size: 100000 } }]),
    async u => {
      // console.log('PUSH ' + u._id);
      userSource.push(u);
    }
  );
  const callRefresh = transform(2, (u: any, callback: any) => {
    // console.log('GET ' + u._id);
    fetch(config.insight.refreshEndpoint(u._id), {
      method: 'POST',
      headers: { Authorization: `Bearer ${config.insight.token}` },
    }).then(res => {
      console.log('GOT ' + u._id, +' ' + res.status);
      if (res.status != 200) console.error(`${nb}/${total} ${res.status} ${u._id} ${(u.marks || []).join(' ')}`);
      callback(null, u);
    });
  });
  const insightSink = new Stream.Writable({
    objectMode: true,
    write: (u, _, next) => {
      setTimeout(async () => {
        try {
          await util.drainBatch(
            u._id,
            dbs.source.insight.find({ u: u._id, d: { $gt: getGamesSince(u) } }),
            100,
            async insights => {
              dbs.dest.insight.insertMany(insights, { ordered: false });
            }
          );
        } catch (err) {
          console.log(err.codeName);
        }
        await dbs.dest.user.updateOne({ _id: u._id }, { $set: { hasInsights: true } });
        nb++;
        console.log(`${nb}/${total} ${u._id} ${(u.marks || []).join(' ')}`);
      }, 5000);
      next();
    },
  });
  const stream = userSource.pipe(callRefresh).pipe(insightSink);
  await new Promise(res => stream.on('close', res));
}

function getGamesSince(u: any) {
  const cheatLog = u.modlog.find((l: any) => l.action == 'engine');
  return util.minusMonths(6)(cheatLog?.date || new Date());
}

withDbs(fetchInsights);

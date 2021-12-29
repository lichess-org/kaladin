import { Dbs, withDbs } from './dbs';
import { Stream } from 'stream';
const transform = require('parallel-transform');
import * as util from './util';

async function fetchInsights(dbs: Dbs) {
  // if ((await dbs.dest.insight.count({})) > 0) dbs.dest.insight.drop();
  await dbs.dest.insight.createIndex({ u: 1 });
  const selector = { hasInsights: true };
  const projection = { _id: 1 };
  const total = await dbs.dest.user.count(selector);
  let nb = 0;
  const userSource = new Stream.Readable({
    objectMode: true,
    read() {},
  });
  util.drain('insight', dbs.dest.user.aggregate([{ $match: selector }, { $project: projection }]), async u => {
    // console.log('PUSH ' + u._id);
    userSource.push(u);
  });
  const fetchTransform = transform(4, async (u: any, callback: any) => {
    try {
      await util.drainBatch(u._id, dbs.source.insight.find({ u: u._id }), 100, async insights => {
        dbs.dest.insight.insertMany(insights, { ordered: false });
      });
    } catch (err) {
      console.log(err.codeName);
    }
    callback(null, u);
  });
  const insightSink = new Stream.Writable({
    objectMode: true,
    write: (u, _, next) => {
      nb++;
      console.log(`${nb}/${total} ${u._id}`);
      next();
    },
  });
  const stream = userSource.pipe(fetchTransform).pipe(insightSink);
  await new Promise(res => stream.on('close', res));
}

withDbs(fetchInsights);

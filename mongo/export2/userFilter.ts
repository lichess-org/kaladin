import { Stream } from 'stream';
const transform = require('parallel-transform');
import { Dbs, withDbs } from './dbs';
import * as util from './util';

async function filterUsers(dbs: Dbs) {
  const selector = { hasMoves: { $exists: false } };
  const total = await dbs.dest.user.count(selector);
  let nb = 0;
  const userSource = new Stream.Readable({
    objectMode: true,
    read() {},
  });
  const userSink = new Stream.Writable({
    objectMode: true,
    write: ([userId, hasMoves], _, next) => {
      dbs.dest.user.updateOne({ _id: userId }, { $set: { hasMoves } });
      nb++;
      console.log(`${hasMoves ? '+' : ' '} ${nb}/${total} ${userId}`);
      next();
    },
  });
  const hasMovesTransform = transform(32, (user: any, callback: any) => {
    predicate(dbs, user).then(hasMoves => {
      callback(null, [user._id, hasMoves]);
    });
  });
  util.drain('user filter', dbs.dest.user.find(selector), async u => {
    userSource.push(u);
  });
  const stream = userSource.pipe(hasMovesTransform).pipe(userSink);
  await new Promise(res => stream.on('close', res));
  // userSource.destroy();
}

const predicate = async (dbs: Dbs, u: any) => {
  let moves = 0,
    game: any;
  const playSince = util.minusMonths(6)(util.userCheatDateOrCreationDate(u));
  // console.log('fetch ' + u._id);
  const cursor = dbs.source.game.aggregate([
    {
      $match: {
        ra: true, // rated
        us: u._id, // users
        an: true, // analysed
        c: { $exists: true }, // clock
        v: { $exists: false }, // variant
        ca: { $gt: playSince }, // date
      },
    },
    { $project: { c: true } },
    {
      $lookup: {
        from: 'analysis2',
        as: 'analysis',
        localField: '_id',
        foreignField: '_id',
      },
    },
    { $unwind: '$analysis' },
    { $project: { c: true, 'analysis.data': true } },
  ]);
  while ((game = await cursor.next())) {
    const time = estimateTime(game);
    if (time && time >= 180 && time < 1500) {
      moves += (game.analysis.data.match(/;/g) || []).length / 2;
    }
    if (moves >= 500) break;
  }
  return moves >= 500;
};

const estimateTime = (g: any) => {
  const buf = g.c?.buffer;
  if (buf && buf[0] <= 180) return buf[0] * 60 + buf[1] * 40;
  return undefined;
};

withDbs(filterUsers);

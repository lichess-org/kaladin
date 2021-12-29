import { Dbs, withDbs } from './dbs';
import * as util from './util';

async function exportUsers(dbs: Dbs) {
  const twoMonthsAgo = util.minusMonths(2)(new Date());
  const cursor = dbs.source.user.aggregate([
    {
      $match: {
        createdAt: {
          $gt: new Date('2019-07-01'),
          $lt: new Date('2021-07-01'),
        },
        marks: { $ne: 'boost' },
        title: { $ne: 'BOT' },
        $or: [{ 'perfs.blitz.gl.d': { $lt: 110 } }, { 'perfs.rapid.gl.d': { $lt: 110 } }],
      },
    },
    {
      $match: {
        $or: [
          { marks: 'engine' },
          {
            enabled: true,
            marks: { $nin: ['alt', 'boost'] },
            $or: [{ 'perfs.blitz.la': { $gt: twoMonthsAgo } }, { 'perfs.rapid.la': { $gt: twoMonthsAgo } }],
          },
        ],
      },
    },
    {
      $project: {
        perfs: 1,
        counts: 1,
        createdAt: 1,
        marks: 1,
      },
    },
    {
      $lookup: {
        from: 'modlog',
        as: 'modlog',
        let: { id: '$_id' },
        pipeline: [
          { $match: { $expr: { $and: [{ $eq: ['$user', '$$id'] }, { $eq: ['$action', 'engine'] }] } } },
          { $project: { _id: 0, mod: 1, date: 1, action: 1 } },
        ],
      },
    },
    {
      $lookup: {
        from: 'irwin_report',
        as: 'irwin',
        let: { id: '$_id' },
        pipeline: [{ $match: { $expr: { $eq: ['$_id', '$$id'] } } }, { $project: { _id: 0, activation: 1 } }],
      },
    },
    {
      $lookup: {
        from: 'report2',
        as: 'reports',
        let: { id: '$_id' },
        pipeline: [
          { $match: { $expr: { $and: [{ $eq: ['$user', '$$id'] }, { $eq: ['$reason', 'cheat'] }] } } },
          { $project: { _id: 0, atoms: 1 } },
          { $unwind: '$atoms' },
          { $replaceRoot: { newRoot: '$atoms' } },
        ],
      },
    },
    {
      $lookup: {
        from: 'appeal',
        as: 'appeal',
        let: { id: '$_id' },
        pipeline: [{ $match: { $expr: { $eq: ['$_id', '$$id'] } } }, { $project: { _id: 0, createdAt: 1 } }],
      },
    },
    {
      $addFields: {
        irwin: { $first: '$irwin' },
        appeal: { $first: '$appeal' },
      },
    },
  ]);
  if ((await dbs.dest.user.count({})) > 0) dbs.dest.user.drop();
  await util.drainBatch(
    'users',
    cursor,
    1000,
    async users => await dbs.dest.user.insertMany(users.filter(markedPlayerHasGamesInTwoMonthsBeforeMark))
  );
}

const markedPlayerHasGamesInTwoMonthsBeforeMark = (u: any) => {
  const playSince = util.minusMonths(2)(util.userCheatDateOrCreationDate(u));
  return u.perfs.blitz?.la > playSince || u.perfs.rapid?.la > playSince;
};

withDbs(exportUsers);

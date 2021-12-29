const lastTwoYears = new Date(Date.now() - 1000 * 3600 * 24 * 365 * 1);
const lastMonth = new Date(Date.now() - 1000 * 3600 * 24 * 30);
const userSelector = () => {
  const sumOfGte = (perfs, gte) => ({ $expr: { $gte: [{ $sum: perfs.map(p => '$perfs.' + p + '.nb') }, gte] } });
  const oneOfGte = (perfs, gte) => ({ $expr: { $or: perfs.map(p => ({ $gte: ['$perfs.' + p + '.nb', gte] })) } });
  const perfFilter = [
    sumOfGte(['blitz', 'rapid', 'classical'], 100),
    sumOfGte(['rapid', 'classical'], 50),
    oneOfGte(['blitz', 'rapid', 'classical'], 40),
    oneOfGte(['bullet'], 80),
  ];
  return { createdAt: { $gt: sinceDate }, $or: perfFilter };
};

const legitUsersQuery = {
  ...userSelector,
  marks: { $ne: 'engine' },
  createdAt: { $gt: lastTwoYears, $lt: lastMonth },
};
const cheatUsersQuery = {
  ...userSelector,
  marks: 'engine',
  createdAt: { $gt: lastTwoYears },
};

const userProjection = {
  perfs: 1,
  counts: 1,
  createdAt: 1,
  marks: 1,
};

const colls = {
  legit: {
    name: 'irwinsights_user_legit',
    nb: 100 * 1000,
  },
  cheat: {
    name: 'irwinsights_user_cheat',
    nb: 50 * 1000,
  },
};

db[colls.legit.name].drop();
db[colls.cheat.name].drop();

db.user4.aggregate([
  { $match: legitUsersQuery },
  { $sample: { size: colls.legit.nb } },
  { $project: userProjection },
  { $out: colls.legit.name },
]);

db.user4.aggregate([
  { $match: cheatUsersQuery },
  { $sample: { size: colls.cheat.nb } },
  { $project: userProjection },
  {
    $lookup: {
      from: 'modlog',
      as: 'marks',
      let: { id: '$_id' },
      pipeline: [
        { $match: { $expr: { $and: [{ $eq: ['$user', '$$id'] }, { $eq: ['$action', 'engine'] }] } } },
        { $project: { _id: 0, mod: 1, date: 1 } },
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
  { $out: colls.cheat.name },
]);

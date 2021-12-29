import { Collection, MongoClient } from 'mongodb';
import config from './config';

export interface Dbs {
  source: {
    user: Collection;
    game: Collection;
    insight: Collection;
  };
  dest: {
    user: Collection;
    insight: Collection;
  };
}

export const withDbs = async (f: (dbs: Dbs) => Promise<any>) => {
  const clients = await Promise.all(
    [config.source.main, config.source.insight, config.dest].map(url => {
      console.log(url);
      return MongoClient.connect(url, {
        directConnection: url.includes('secondary'),
      });
    })
  );
  const [main, insight, dest] = clients.map(client => client.db());
  const dbs = {
    source: {
      user: main.collection('user4'),
      game: main.collection('game5'),
      insight: insight.collection('insight'),
    },
    dest: {
      user: dest.collection('user'),
      insight: dest.collection('insight'),
    },
  };
  await f(dbs);

  clients.forEach(c => c.close());
};

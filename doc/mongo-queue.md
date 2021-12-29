# Mongo queue recommendations and examples

1. Lichess pushes requests into the queue

```js
db.kaladin_queue.insert({ _id: 'thibault', priority: 100, queuedAt: new Date() });
db.kaladin_queue.insert({ _id: 'sesquipedalism', priority: 10, queuedAt: new Date() });
db.kaladin_queue.insert({ _id: 'somethingpretentious', priority: 100, queuedAt: new Date() });
```

2. Kaladin pops requests from the queue, until it has enough or the queue is empty

```js
db.kaladin_queue.findOneAndUpdate(
  {
    startedAt: { $not: { $gt: new Date(Date.now() - 1000 * 3600) } },
    'response.at': { $not: { $gt: new Date(Date.now() - 1000 * 3600 * 24 * 7) } },
  },
  { $set: { startedAt: new Date() } },
  { sort: { priority: -1 } }
);
```

Note how we find requests that have not started recently,
to allow retrying if for some reason a request was not fully processed.
We also exclude requests that we already answered less than 7 days ago.
We find and update requests in a single atomic query.

3. Kaladin computes responses then updates the queue with them

```js
db.kaladin_queue.update({ _id: 'thibault' }, { $set: { response: { at: new Date(), pred: 0.3, ... } } });
```

It then returns to step 2.

4. Lichess reads new responses from the queue.

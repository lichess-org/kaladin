export default {
  source: {
    main: 'mongodb://localhost:27119/lichess?readPreference=secondary',
    insight: 'mongodb://localhost:27118/insight',
    // insight: 'mongodb://localhost:27118/insight',
  },
  dest: 'mongodb://localhost:27017/kaladin',
  insight: {
    refreshEndpoint: (id: string) => `https://lichess.org/insights/refresh/${id}`,
    token: '7PWkUj1LYF8ItEGC',
  },
};


movetime_by_material_pipeline = [
   {
        "$match":{
            "u":"placeholder"
      }
   },
   {'$limit': 10000},
   {
      "$project":{   # what you will see at the end
         "m.t":True,  # move time
         "m.i":True   # material difference
      }
   },
   {
      "$unwind":"$m"  # necessary to access individual move data
   },
   {
        "$sample" : {
            "size" : 200000
        }
   },
   {
      "$group":{      
         "_id":{     # Define the thing we are grouping by
            "$cond":[
               {
                  "$eq":[
                     "$m.i",
                     0
                  ]
               },
               5,   # if material diff is 0, then return 5
               {
                  "$cond":[
                     {
                        "$lt":[
                           "$m.i",
                           -6
                        ]
                     },
                     1,  # else if material diff is < -6, return 1
                     {
                        "$cond":[
                           {
                              "$lt":[
                                 "$m.i",
                                 -3
                              ]
                           },
                           2,   # else if material diff is < -3, return 2
                           {
                              "$cond":[
                                 {
                                    "$lt":[
                                       "$m.i",
                                       -1
                                    ]
                                 },
                                 3,   # else if material diff is < -1, return 3
                                 {
                                    "$cond":[
                                       {
                                          "$lt":[
                                             "$m.i",
                                             0
                                          ]
                                       },
                                       4, # else if material diff is < 0, return 4
                                       {
                                          "$cond":[
                                             {
                                                "$lte":[
                                                   "$m.i",
                                                   1
                                                ]
                                             },
                                             6, # else if material diff is <= 1, return 6
                                             {
                                                "$cond":[
                                                   {
                                                      "$lte":[
                                                         "$m.i",
                                                         3
                                                      ]
                                                   },
                                                   7,  # else if material diff is <= 6, return 7
                                                   {
                                                      "$cond":[
                                                         {
                                                            "$lte":[
                                                               "$m.i",
                                                               6
                                                            ]
                                                         },
                                                         8,  # else if material diff is <= 6, return 8
                                                         9   # else return 9
                                                      ]
                                                   }
                                                ]
                                             }
                                          ]
                                       }
                                    ]
                                 }
                              ]
                           }
                        ]
                     }
                  ]
               }
            ]
         },
         "v":{     # v for value, just a name
            "$avg":{  
               "$divide":[
                  "$m.t",
                  10
               ]
            }      # take the avg of (move time divided by 10) = move time in seconds
         },
         "nb":{    # nb for number of moves, just a name
            "$sum":1   # 1 for each move
         }
      }
   }
]

timevariance_by_date_pipeline = [
    {
        "$match" : {
            "u" : "dev"
        }
    },
    {'$limit': 10000},
    {
        "$project" : {
            "m.v" : True,
            "d" : True
        }
    },
    {
        "$unwind" : "$m"
    },
    {
        "$match" : {
            "m.v" : {
                "$exists" : True
            }
        }
    },
    {
        "$sample" : {
            "size" : 200000
        }
    },
    {
        "$bucketAuto" : {
            "groupBy" : "$d",
            "buckets" : 12,
            "output" : {
                "v" : {
                    "$avg" : {
                        "$divide" : [
                            "$m.v",
                            100000
                        ]
                    }
                },
                "nb" : {
                    "$sum" : 1
                }
            }
        }
    }
]


acpl_by_date_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'d': True, 'm.c': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$bucketAuto': {'buckets': 12,
                    'groupBy': '$d',
                    'output': {'nb': {'$sum': 1}, 'v': {'$avg': '$m.c'}}}}
]

acplfiltered_by_date_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {
        'd': True, 
        'm.c': True,
        'm.i': True,
        'm.p': True,
        'm.t': True}},
    {'$unwind': '$m'},
    {'$match': {
        'm.p': {'$in': [2, 3]},           # phase filter  
        'm.i': {'$gte': -3, '$lte': 3},    # material filter
    }},
    {'$sample': {'size': 200000}},
    {'$bucketAuto': {'buckets': 12,
                    'groupBy': '$d',
                    'output': {'nb': {'$sum': 1}, 'v': {'$avg': '$m.c'}}}}
]

acpl_by_variant_pipeline = [
    {
        "$match" : {
            "u" : "dev",
            "a" : True
        }
    },
    {'$limit': 10000},
    {
        "$project" : {
            "m.c" : True,
            "p" : True
        }
    },
    {
        "$unwind" : "$m"
    },
    {
        "$sample" : {
            "size" : 200000
        }
    },
    {
        "$group" : {
            "_id" : "$p",
            "v" : {
                "$avg" : "$m.c"
            },
            "nb" : {
                "$sum" : 1
            }
        }
    }
]

acpl_by_material_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.c': True, 'm.i': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$eq': ['$m.i', 0]},
                                5,
                                {'$cond': [{'$lt': ['$m.i', -6]},
                                            1,
                                            {'$cond': [{'$lt': ['$m.i', -3]},
                                                        2,
                                                        {'$cond': [{'$lt': ['$m.i',
                                                                            -1]},
                                                                3,
                                                                {'$cond': [{'$lt': ['$m.i',
                                                                                    0]},
                                                                            4,
                                                                            {'$cond': [{'$lte': ['$m.i',
                                                                                                1]},
                                                                                        6,
                                                                                        {'$cond': [{'$lte': ['$m.i',
                                                                                                            3]},
                                                                                                    7,
                                                                                                    {'$cond': [{'$lte': ['$m.i',
                                                                                                                        6]},
                                                                                                            8,
                                                                                                            9]}]}]}]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$avg': '$m.c'}}}
]

acpl_by_blur_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'm.c': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.b',
                'nb': {'$sum': 1},
                'v': {'$avg': '$m.c'}}}
]

acplfiltered_by_blur_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True,
                'm.c': True,
                'm.i': True,
                'm.p': True,
                'm.t': True,
                'm.e': True}},
    {'$unwind': '$m'},
    {'$match': {
        'm.t': {'$gte': 25, '$lte': 300},  # movetime filter
        'm.p': {'$in': [2, 3]},           # phase filter  
        'm.i': {'$gte': -1, '$lte': 1},    # material filter
        'm.e': {'$gte': -500, '$lte': 500} # evaluation filter
    }},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.b',
                'nb': {'$sum': 1},
                'v': {'$avg': '$m.c'}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
 ]

acpl_by_timevariance_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.c': True, 'm.v': True}},
    {'$unwind': '$m'},
    {'$match': {'m.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lte': ['$m.v', 25000]},
                            25000,
                            {'$cond': [{'$lte': ['$m.v', 40000]},
                                        40000,
                                        {'$cond': [{'$lte': ['$m.v', 60000]},
                                                    60000,
                                                    {'$cond': [{'$lte': ['$m.v',
                                                                        75000]},
                                                            75000,
                                                            100000]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$avg': '$m.c'}}}
 ]

acpl_by_phase_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.c': True, 'm.p': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.p',
                'nb': {'$sum': 1},
                'v': {'$avg': '$m.c'}}}
 ]


acpl_by_movetime_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.c': True, 'm.t': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lt': ['$m.t', 10]},
                                1,
                                {'$cond': [{'$lt': ['$m.t', 30]},
                                            3,
                                            {'$cond': [{'$lt': ['$m.t', 50]},
                                                        5,
                                                        {'$cond': [{'$lt': ['$m.t',
                                                                            100]},
                                                                10,
                                                                {'$cond': [{'$lt': ['$m.t',
                                                                                    300]},
                                                                            30,
                                                                            60]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$avg': '$m.c'}}}
 ]

movetime_by_piecemoved_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.r': True, 'm.t': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.r',
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.t', 10]}}}}
 ]


timevariance_by_material_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.i': True, 'm.v': True}},
    {'$unwind': '$m'},
    {'$match': {'m.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$eq': ['$m.i', 0]},
                                5,
                                {'$cond': [{'$lt': ['$m.i', -6]},
                                            1,
                                            {'$cond': [{'$lt': ['$m.i', -3]},
                                                        2,
                                                        {'$cond': [{'$lt': ['$m.i',
                                                                            -1]},
                                                                3,
                                                                {'$cond': [{'$lt': ['$m.i',
                                                                                    0]},
                                                                            4,
                                                                            {'$cond': [{'$lte': ['$m.i',
                                                                                                1]},
                                                                                        6,
                                                                                        {'$cond': [{'$lte': ['$m.i',
                                                                                                            3]},
                                                                                                    7,
                                                                                                    {'$cond': [{'$lte': ['$m.i',
                                                                                                                        6]},
                                                                                                            8,
                                                                                                            9]}]}]}]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.v', 100000]}}}}
 ]

timevariance_by_result_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.v': True, 'r': True}},
    {'$unwind': '$m'},
    {'$match': {'m.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$r',
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.v', 100000]}}}}
 ]

blur_by_result_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'r': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$r',
                'nb': {'$sum': 1},
                'v': {'$push': {'$cond': ['$m.b', 1, 0]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]},
                    'v': {'$multiply': [100, {'$avg': '$v'}]}}}
]

blurfiltered_by_result_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {
        'm.b': True, 
        'r': True,
        'm.i': True,
        'm.p': True,
        'm.t': True}},
    {'$unwind': '$m'},
    {'$match': {
        'm.t': {'$gte': 25, '$lte': 300},  # movetime filter
        'm.p': {'$in': [2, 3]},           # phase filter  
        'm.i': {'$gte': -1, '$lte': 1},    # material filter
    }},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$r',
                'nb': {'$sum': 1},
                'v': {'$push': {'$cond': ['$m.b', 1, 0]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]},
                    'v': {'$multiply': [100, {'$avg': '$v'}]}}}
]

blur_by_movetime_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'm.t': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lt': ['$m.t', 10]},
                                1,
                                {'$cond': [{'$lt': ['$m.t', 30]},
                                            3,
                                            {'$cond': [{'$lt': ['$m.t', 50]},
                                                        5,
                                                        {'$cond': [{'$lt': ['$m.t',
                                                                            100]},
                                                                10,
                                                                {'$cond': [{'$lt': ['$m.t',
                                                                                    300]},
                                                                            30,
                                                                            60]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$push': {'$cond': ['$m.b', 1, 0]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]},
        'v': {'$multiply': [100, {'$avg': '$v'}]}}}
]

blur_by_date_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'d': True, 'm.b': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$bucketAuto': {'buckets': 12,
                    'groupBy': '$d',
                    'output': {'ids': {'$addToSet': '$_id'},
                                'nb': {'$sum': 1},
                                'v': {'$push': {'$cond': ['$m.b', 1, 0]}}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]},
                    'v': {'$multiply': [100, {'$avg': '$v'}]}}}
]

movetime_by_date_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'d': True, 'm.t': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$bucketAuto': {'buckets': 12,
                    'groupBy': '$d',
                    'output': {'ids': {'$addToSet': '$_id'},
                                'nb': {'$sum': 1},
                                'v': {'$avg': {'$divide': ['$m.t', 10]}}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

opponentrating_by_date_pipeline = [
    {'$match': {'pr': {'$ne': True}, 'u': 'dev'}},
    {'$limit': 10000},
    {'$bucketAuto': {'buckets': 12,
                    'groupBy': '$d',
                    'output': {'ids': {'$addToSet': '$_id'},
                                'nb': {'$sum': 1},
                                'v': {'$avg': '$or'}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

ratinggain_by_date_pipeline = [
    {'$match': {'pr': {'$ne': True}, 'u': 'dev'}},
    {'$limit': 10000},
    {'$bucketAuto': {'buckets': 12,
                    'groupBy': '$d',
                    'output': {'ids': {'$addToSet': '$_id'},
                                'nb': {'$sum': 1},
                                'v': {'$avg': '$rd'}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

timevariance_by_movetime_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.t': True, 'm.v': True}},
    {'$unwind': '$m'},
    {'$match': {'m.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lt': ['$m.t', 10]},
                                1,
                                {'$cond': [{'$lt': ['$m.t', 30]},
                                            3,
                                            {'$cond': [{'$lt': ['$m.t', 50]},
                                                        5,
                                                        {'$cond': [{'$lt': ['$m.t',
                                                                            100]},
                                                                    10,
                                                                    {'$cond': [{'$lt': ['$m.t',
                                                                                        300]},
                                                                            30,
                                                                            60]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.v', 100000]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

blur_by_material_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'm.i': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$eq': ['$m.i', 0]},
                                5,
                                {'$cond': [{'$lt': ['$m.i', -6]},
                                            1,
                                            {'$cond': [{'$lt': ['$m.i', -3]},
                                                        2,
                                                        {'$cond': [{'$lt': ['$m.i',
                                                                            -1]},
                                                                    3,
                                                                    {'$cond': [{'$lt': ['$m.i',
                                                                                        0]},
                                                                            4,
                                                                            {'$cond': [{'$lte': ['$m.i',
                                                                                                    1]},
                                                                                        6,
                                                                                        {'$cond': [{'$lte': ['$m.i',
                                                                                                            3]},
                                                                                                    7,
                                                                                                    {'$cond': [{'$lte': ['$m.i',
                                                                                                                        6]},
                                                                                                                8,
                                                                                                                9]}]}]}]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$push': {'$cond': ['$m.b', 1, 0]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]},
                    'v': {'$multiply': [100, {'$avg': '$v'}]}}}
]

timevariance_by_phase_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.p': True, 'm.v': True}},
    {'$unwind': '$m'},
    {'$match': {'m.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.p',
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.v', 100000]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

blur_by_phase_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'm.p': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.p',
                'nb': {'$sum': 1},
                'v': {'$push': {'$cond': ['$m.b', 1, 0]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]},
                    'v': {'$multiply': [100, {'$avg': '$v'}]}}}
]

movetime_by_phase_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.p': True, 'm.t': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.p',
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.t', 10]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

timevariance_by_blur_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'm.v': True}},
    {'$unwind': '$m'},
    {'$match': {'m.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.b',
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.v', 100000]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]
 
movetime_by_blur_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'm.t': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.b',
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.t', 10]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

blur_by_timevariance_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'm.v': True}},
    {'$unwind': '$m'},
    {'$match': {'m.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lte': ['$m.v', 25000]},
                                25000,
                                {'$cond': [{'$lte': ['$m.v', 40000]},
                                            40000,
                                            {'$cond': [{'$lte': ['$m.v', 60000]},
                                                        60000,
                                                        {'$cond': [{'$lte': ['$m.v',
                                                                            75000]},
                                                                    75000,
                                                                    100000]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$push': {'$cond': ['$m.b', 1, 0]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]},
                    'v': {'$multiply': [100, {'$avg': '$v'}]}}}
]

movetime_by_timevariance_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.t': True, 'm.v': True}},
    {'$unwind': '$m'},
    {'$match': {'m.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lte': ['$m.v', 25000]},
                                25000,
                                {'$cond': [{'$lte': ['$m.v', 40000]},
                                            40000,
                                            {'$cond': [{'$lte': ['$m.v', 60000]},
                                                        60000,
                                                        {'$cond': [{'$lte': ['$m.v',
                                                                            75000]},
                                                                    75000,
                                                                    100000]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.t', 10]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

acpl_by_result_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.c': True, 'r': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$r',
                'nb': {'$sum': 1},
                'v': {'$avg': '$m.c'}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

movetime_by_result_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.t': True, 'r': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$r',
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.t', 10]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

opponentrating_by_result_pipeline = [
    {'$match': {'pr': {'$ne': True}, 'u': 'dev'}},
    {'$limit': 10000},
    {'$group': {'_id': '$r',
                'nb': {'$sum': 1},
                'v': {'$avg': '$or'}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

acpl_by_opponentstrength_pipeline = [
    {'$match': {'a': True, 'pr': {'$ne': True}, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.c': True, 'os': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$os',
                'nb': {'$sum': 1},
                'v': {'$avg': '$m.c'}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

timevariance_by_opponentstrength_pipeline = [
    {'$match': {'pr': {'$ne': True}, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.v': True, 'os': True}},
    {'$unwind': '$m'},
    {'$match': {'m.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$os',
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.v', 100000]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

blur_by_opponentstrength_pipeline = [
    {'$match': {'pr': {'$ne': True}, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'os': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$os',
                'nb': {'$sum': 1},
                'v': {'$push': {'$cond': ['$m.b', 1, 0]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]},
                'v': {'$multiply': [100, {'$avg': '$v'}]}}}
]

movetime_by_opponentstrength_pipeline = [
    {'$match': {'pr': {'$ne': True}, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.t': True, 'os': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$os',
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.t', 10]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

ratinggain_by_opponentstrength_pipeline = [
    {'$match': {'pr': {'$ne': True}, 'u': 'dev'}},
    {'$limit': 10000},
    {'$group': {'_id': '$os',
                'nb': {'$sum': 1},
                'v': {'$avg': '$rd'}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

timevariance_by_centipawnloss_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.c': True, 'm.v': True}},
    {'$unwind': '$m'},
    {'$match': {'m.c': {'$exists': True}, 'm.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lte': ['$m.c', 0]},
                                0,
                                {'$cond': [{'$lte': ['$m.c', 10]},
                                            10,
                                            {'$cond': [{'$lte': ['$m.c', 25]},
                                                        25,
                                                        {'$cond': [{'$lte': ['$m.c',
                                                                            50]},
                                                                    50,
                                                                    {'$cond': [{'$lte': ['$m.c',
                                                                                        100]},
                                                                            100,
                                                                            {'$cond': [{'$lte': ['$m.c',
                                                                                                    200]},
                                                                                        200,
                                                                                        {'$cond': [{'$lte': ['$m.c',
                                                                                                            500]},
                                                                                                    500,
                                                                                                    {'$cond': [{'$lte': ['$m.c',
                                                                                                                        99999]},
                                                                                                                99999,
                                                                                                                99999]}]}]}]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.v', 100000]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

blur_by_centipawnloss_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'm.c': True}},
    {'$unwind': '$m'},
    {'$match': {'m.c': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lte': ['$m.c', 0]},
                                0,
                                {'$cond': [{'$lte': ['$m.c', 10]},
                                            10,
                                            {'$cond': [{'$lte': ['$m.c', 25]},
                                                        25,
                                                        {'$cond': [{'$lte': ['$m.c',
                                                                            50]},
                                                                    50,
                                                                    {'$cond': [{'$lte': ['$m.c',
                                                                                        100]},
                                                                            100,
                                                                            {'$cond': [{'$lte': ['$m.c',
                                                                                                    200]},
                                                                                        200,
                                                                                        {'$cond': [{'$lte': ['$m.c',
                                                                                                            500]},
                                                                                                    500,
                                                                                                    {'$cond': [{'$lte': ['$m.c',
                                                                                                                        99999]},
                                                                                                                99999,
                                                                                                                99999]}]}]}]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$push': {'$cond': ['$m.b', 1, 0]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]},
                    'v': {'$multiply': [100, {'$avg': '$v'}]}}}
]

movetime_by_centipawnloss_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.c': True, 'm.t': True}},
    {'$unwind': '$m'},
    {'$match': {'m.c': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lte': ['$m.c', 0]},
                                0,
                                {'$cond': [{'$lte': ['$m.c', 10]},
                                            10,
                                            {'$cond': [{'$lte': ['$m.c', 25]},
                                                        25,
                                                        {'$cond': [{'$lte': ['$m.c',
                                                                            50]},
                                                                    50,
                                                                    {'$cond': [{'$lte': ['$m.c',
                                                                                        100]},
                                                                            100,
                                                                            {'$cond': [{'$lte': ['$m.c',
                                                                                                    200]},
                                                                                        200,
                                                                                        {'$cond': [{'$lte': ['$m.c',
                                                                                                            500]},
                                                                                                    500,
                                                                                                    {'$cond': [{'$lte': ['$m.c',
                                                                                                                        99999]},
                                                                                                                99999,
                                                                                                                99999]}]}]}]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.t', 10]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

acpl_by_piecemoved_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.c': True, 'm.r': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.r',
                'nb': {'$sum': 1},
                'v': {'$avg': '$m.c'}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

timevariance_by_piecemoved_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.r': True, 'm.v': True}},
    {'$unwind': '$m'},
    {'$match': {'m.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.r',
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.v', 100000]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

blur_by_piecemoved_pipeline = [
    {'$match': {'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'm.r': True}},
    {'$unwind': '$m'},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': '$m.r',
                'nb': {'$sum': 1},
                'v': {'$push': {'$cond': ['$m.b', 1, 0]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]},
                    'v': {'$multiply': [100, {'$avg': '$v'}]}}}
]

acpl_by_evaluation_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.c': True, 'm.e': True}},
    {'$unwind': '$m'},
    {'$match': {'m.e': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lt': ['$m.e', -600]},
                                1,
                                {'$cond': [{'$lt': ['$m.e', -350]},
                                            2,
                                            {'$cond': [{'$lt': ['$m.e', -175]},
                                                        3,
                                                        {'$cond': [{'$lt': ['$m.e',
                                                                            -80]},
                                                                    4,
                                                                    {'$cond': [{'$lt': ['$m.e',
                                                                                        -25]},
                                                                            5,
                                                                            {'$cond': [{'$lt': ['$m.e',
                                                                                                25]},
                                                                                        6,
                                                                                        {'$cond': [{'$lt': ['$m.e',
                                                                                                            80]},
                                                                                                    7,
                                                                                                    {'$cond': [{'$lt': ['$m.e',
                                                                                                                        175]},
                                                                                                                8,
                                                                                                                {'$cond': [{'$lt': ['$m.e',
                                                                                                                                    350]},
                                                                                                                        9,
                                                                                                                        {'$cond': [{'$lt': ['$m.e',
                                                                                                                                            600]},
                                                                                                                                    10,
                                                                                                                                    11]}]}]}]}]}]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$avg': '$m.c'}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]

timevariance_by_evaluation_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.e': True, 'm.v': True}},
    {'$unwind': '$m'},
    {'$match': {'m.e': {'$exists': True}, 'm.v': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lt': ['$m.e', -600]},
                                1,
                                {'$cond': [{'$lt': ['$m.e', -350]},
                                            2,
                                            {'$cond': [{'$lt': ['$m.e', -175]},
                                                        3,
                                                        {'$cond': [{'$lt': ['$m.e',
                                                                            -80]},
                                                                    4,
                                                                    {'$cond': [{'$lt': ['$m.e',
                                                                                        -25]},
                                                                            5,
                                                                            {'$cond': [{'$lt': ['$m.e',
                                                                                                25]},
                                                                                        6,
                                                                                        {'$cond': [{'$lt': ['$m.e',
                                                                                                            80]},
                                                                                                    7,
                                                                                                    {'$cond': [{'$lt': ['$m.e',
                                                                                                                        175]},
                                                                                                                8,
                                                                                                                {'$cond': [{'$lt': ['$m.e',
                                                                                                                                    350]},
                                                                                                                        9,
                                                                                                                        {'$cond': [{'$lt': ['$m.e',
                                                                                                                                            600]},
                                                                                                                                    10,
                                                                                                                                    11]}]}]}]}]}]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.v', 100000]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]


blur_by_evaluation_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.b': True, 'm.e': True}},
    {'$unwind': '$m'},
    {'$match': {'m.e': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lt': ['$m.e', -600]},
                                1,
                                {'$cond': [{'$lt': ['$m.e', -350]},
                                            2,
                                            {'$cond': [{'$lt': ['$m.e', -175]},
                                                        3,
                                                        {'$cond': [{'$lt': ['$m.e',
                                                                            -80]},
                                                                    4,
                                                                    {'$cond': [{'$lt': ['$m.e',
                                                                                        -25]},
                                                                            5,
                                                                            {'$cond': [{'$lt': ['$m.e',
                                                                                                25]},
                                                                                        6,
                                                                                        {'$cond': [{'$lt': ['$m.e',
                                                                                                            80]},
                                                                                                    7,
                                                                                                    {'$cond': [{'$lt': ['$m.e',
                                                                                                                        175]},
                                                                                                                8,
                                                                                                                {'$cond': [{'$lt': ['$m.e',
                                                                                                                                    350]},
                                                                                                                        9,
                                                                                                                        {'$cond': [{'$lt': ['$m.e',
                                                                                                                                            600]},
                                                                                                                                    10,
                                                                                                                                    11]}]}]}]}]}]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$push': {'$cond': ['$m.b', 1, 0]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]},
                    'v': {'$multiply': [100, {'$avg': '$v'}]}}}
]

movetime_by_evaluation_pipeline = [
    {'$match': {'a': True, 'u': 'dev'}},
    {'$limit': 10000},
    {'$project': {'m.e': True, 'm.t': True}},
    {'$unwind': '$m'},
    {'$match': {'m.e': {'$exists': True}}},
    {'$sample': {'size': 200000}},
    {'$group': {'_id': {'$cond': [{'$lt': ['$m.e', -600]},
                                1,
                                {'$cond': [{'$lt': ['$m.e', -350]},
                                            2,
                                            {'$cond': [{'$lt': ['$m.e', -175]},
                                                        3,
                                                        {'$cond': [{'$lt': ['$m.e',
                                                                            -80]},
                                                                4,
                                                                {'$cond': [{'$lt': ['$m.e',
                                                                                    -25]},
                                                                            5,
                                                                            {'$cond': [{'$lt': ['$m.e',
                                                                                                25]},
                                                                                        6,
                                                                                        {'$cond': [{'$lt': ['$m.e',
                                                                                                            80]},
                                                                                                    7,
                                                                                                    {'$cond': [{'$lt': ['$m.e',
                                                                                                                        175]},
                                                                                                            8,
                                                                                                            {'$cond': [{'$lt': ['$m.e',
                                                                                                                                350]},
                                                                                                                        9,
                                                                                                                        {'$cond': [{'$lt': ['$m.e',
                                                                                                                                            600]},
                                                                                                                                    10,
                                                                                                                                    11]}]}]}]}]}]}]}]}]}]},
                'nb': {'$sum': 1},
                'v': {'$avg': {'$divide': ['$m.t', 10]}}}},
    {'$addFields': {'ids': {'$slice': ['$ids', 4]}}}
]
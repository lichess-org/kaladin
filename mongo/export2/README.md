## PLAYER FILTERS

- Players must have at least 500 moves with eval in blitz or rapid in the past 6 months
- Bot accounts should not be included in the dataset
- Legit players that played no rated games in the last 2 months should not be included.
- Cheat players that played no rated games in the last 2 months before the mark date should not be included.
- Account must have been created between 2019-07-01 and 2021-05-01
- One of blitz or rapid rating must be established (non-provisional)

- ~Legit players who have closed accounts, alt marks, or boost marks should not be included.~

##REDUCE DATA SIZE

- Games by legit players that are older than 6 months ago should be excluded from the insights dataset
- Games by cheat players that are older than 6 months before their mark date should be excluded from the insights dataset

## EXTRA DATA

- Would like an extra 1000 legit players rated under 1300 in blitz or rapid who meet the above criteria
- Would like an extra 500 cheated players rated under 1300 in blitz or rapid who meet the above criteria
- Would like an extra 1000 legit players rated above 2300 in rapid or blitz
- Would like an extra 500 cheated players rated above 2300 in rapid or blitz

## Usage

```
ssh -L 27119:laura.vrack.lichess.ovh:27017 root@laura.lichess.ovh
ssh -L 27118:bowie.vrack.lichess.ovh:27017 root@bowie.lichess.ovh

yarn run userPreFilter
yarn run userFilter
yarn run insight
```

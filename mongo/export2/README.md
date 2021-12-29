## Dataset filters

- Players must have at least 1000 moves in blitz or rapid in the past 6 months
- Bot accounts should not be included in the dataset
- Legit players that played no rated games in the last 2 months should not be included.
- Cheat players that played no rated games in the last 2 months before the mark date should not be included.
- Account must have been created between 2019-07-01 and 2021-05-01
- One of blitz or rapid rating must be established (non-provisional)
- Legit players who have closed accounts, alt marks, or boost marks should not be included.

## Reduce data size

- Games by legit players that are older than 6 months ago should be excluded from the insights dataset
- Games by cheat players that are older than 6 months before their mark date should be excluded from the insights dataset
- Other time controls are excluded from the dataset

## Usage

```
ssh -L 27119:laura.vrack.lichess.ovh:27017 root@laura.lichess.ovh
ssh -L 27118:bowie.vrack.lichess.ovh:27017 root@bowie.lichess.ovh

yarn run userPreFilter
yarn run userFilter
yarn run insight
```

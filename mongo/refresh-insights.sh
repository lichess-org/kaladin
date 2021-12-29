#!/bin/sh

# asks lichess to generate insights for a list of user IDs
i=1
for id in $(cat all-user-ids.txt); do
  echo "$i $id"
  ((i=i+1))
  curl -XPOST -H "Authorization: Bearer $INSIGHTS_OAUTH" https://lichess.org/insights/refresh/$id
done

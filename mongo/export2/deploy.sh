#!/bin/sh

mongodump --db kaladin --collection user --query '{"hasInsights":true}'
mongodump --db kaladin --collection insight
filename=dump-$(date +%d-%m-%Y-%H-%M).tar.bz2
tar -c dump | pbzip2 -c > $filename
rm -rfv dump
scp $filename root@bowie.lichess.ovh:/home/lichess-db/vJADKRgfiF1etTFT/$filename

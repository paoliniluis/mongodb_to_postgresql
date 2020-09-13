# mongodb_to_postgresql

A simple script that will grab an entire collection and will insert it into a PostgreSQL table with streams

1 - enter the info in the config json
2 - configure the projections to get from mongo
3 - do npm i && node mongo_to_postgres.js

Everything is done with streams, so it won't blow up your RAM :)
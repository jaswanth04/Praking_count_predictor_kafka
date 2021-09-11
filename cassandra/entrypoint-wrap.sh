#!/bin/bash

if [[ ! -z "$CASSANDRA_KEYSPACE" && $1 = 'cassandra' ]]; then
  # Create default keyspace for single node cluster
  CQL="CREATE KEYSPACE $CASSANDRA_KEYSPACE WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}; USE $CASSANDRA_KEYSPACE; CREATE TABLE $TABLE (timestamp text PRIMARY KEY,violation_code int, violation_location int, violation_county text, violation_type text, month int, date int, hour int, violation_count int);"
  until echo $CQL | cqlsh; do
    echo "cqlsh: Cassandra is unavailable - retry later"
    sleep 2
  done &
fi

exec /docker-entrypoint.sh "$@"
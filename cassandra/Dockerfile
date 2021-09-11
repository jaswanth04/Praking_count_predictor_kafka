FROM cassandra:3.11

ENV CASSANDRA_KEYSPACE parking_keyspace
ENV TABLE violation_details

COPY entrypoint-wrap.sh /entrypoint-wrap.sh
ENTRYPOINT ["/entrypoint-wrap.sh"]
CMD ["cassandra", "-f"]

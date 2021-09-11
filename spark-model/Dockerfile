FROM datamechanics/spark:3.1.1-hadoop-3.2.0-java-8-scala-2.12-python-3.8-latest

RUN pip3 install numpy
RUN pip3 install kafka_python

WORKDIR /opt/app
COPY src /opt/app

# Uncomment this to build the model first
CMD ["/opt/spark/bin/spark-submit", "/opt/app/build_model.py"]
#CMD ["/opt/spark/bin/spark-submit", "--packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.1.0", "--conf", "spark.cassandra.connection.host=cass", "/opt/app/predict.py"]


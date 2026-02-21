FROM apache/spark:3.5.0
USER root

RUN apt-get update && apt-get install -y curl

RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar \
 https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar

RUN curl -L -o /opt/spark/jars/delta-storage-3.1.0.jar \
 https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar

RUN curl -L -o /opt/spark/jars/spark-cassandra-connector_2.12-3.5.0.jar \
 https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.5.0/spark-cassandra-connector_2.12-3.5.0.jar

RUN curl -L -o /opt/spark/jars/spark-cassandra-connector-assembly.jar \
https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.3.0/spark-cassandra-connector-assembly_2.12-3.3.0.jar


# ✅ Backup jars so volume mount doesn't hide them
RUN cp -r /opt/spark/jars /opt/spark/jars-backup

RUN chmod -R 777 /opt/spark/jars
RUN mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars
RUN chmod -R 777 /home/spark/.ivy2

USER spark
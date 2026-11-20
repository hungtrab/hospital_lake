FROM apache/spark:3.5.5-python3
USER root
RUN pip3 install --no-cache-dir pyarrow==15.0.2 PyYAML==6.0.2 kafka-python==2.2.15
# Resolve once when building; runtime driver/executors use the same pinned jars.
RUN printf 'print("Connectors resolved")\n' > /tmp/resolve.py && \
    SPARK_LOCAL_IP=127.0.0.1 /opt/spark/bin/spark-submit \
      --conf spark.jars.ivy=/opt/ivy \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,org.apache.iceberg:iceberg-aws-bundle:1.9.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
      --exclude-packages org.apache.hadoop:hadoop-client-runtime,org.apache.hadoop:hadoop-client-api,org.lz4:lz4-java,org.xerial.snappy:snappy-java,org.slf4j:slf4j-api,commons-logging:commons-logging,com.google.code.findbugs:jsr305 \
      /tmp/resolve.py && chmod -R a+rwX /opt/ivy
WORKDIR /opt/hospital-lake
COPY spark ./spark
COPY generators ./generators
COPY sql ./sql
COPY scripts ./scripts
ENV PYTHONPATH=/opt/hospital-lake
USER 185

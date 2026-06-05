#!/bin/bash
if [ "$SPARK_MODE" = "master" ]; then
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --host spark-master \
        --port 7077 \
        --webui-port 8080
elif [ "$SPARK_MODE" = "worker" ]; then
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port 8081 \
        $SPARK_MASTER_URL
else
    exec "$@"
fi

ARG SPARK_IMAGE=
FROM ${SPARK_IMAGE}
COPY entrypoint.sh /usr/bin
RUN mkdir -p /tmp/hs
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar $SPARK_HOME/jars

ENTRYPOINT ["/usr/bin/entrypoint.sh"]



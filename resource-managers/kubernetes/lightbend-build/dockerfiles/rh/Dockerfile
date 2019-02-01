#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM registry.access.redhat.com/redhat-openjdk-18/openjdk18-openshift:1.5-14.1539812388

ARG spark_jars=jars
ARG img_path=kubernetes/dockerfiles

# Before building the docker image, first build and make a Spark distribution following
# the instructions in http://spark.apache.org/docs/latest/building-spark.html.
# If this docker file is being used in the context of building your images from a Spark
# distribution, the docker build command should be invoked from the top level directory
# of the Spark distribution. E.g.:
# docker build -t spark:latest -f kubernetes/dockerfiles/spark/Dockerfile .

USER root

COPY ${spark_jars} /opt/spark/jars
COPY bin /opt/spark/bin
COPY sbin /opt/spark/sbin
COPY ${img_path}/spark/entrypoint.sh /opt/
COPY examples /opt/spark/examples
#COPY ${k8s_tests} /opt/spark/tests
COPY data /opt/spark/data

RUN mkdir -p /opt/spark/work-dir && \
    touch /opt/spark/RELEASE && \
    chgrp -R 0 /opt/spark  && \
    chmod -R g=u /opt/spark && \
    chmod -R u+x /opt/spark/bin && \
    chmod -R u+x /opt/spark/sbin

FROM registry.access.redhat.com/redhat-openjdk-18/openjdk18-openshift:1.5-14.1539812388

USER root
COPY --from=0 /opt /opt

RUN set -ex && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd  && \
    mkdir -p /etc/metrics/conf && \
    mkdir -p /prometheus && \
    curl https://s3-us-west-2.amazonaws.com/fdp-kubernetes-builds/spark/extra/metrics.properties -o /etc/metrics/conf/metrics.properties && \
    curl  http://central.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.3.1/jmx_prometheus_javaagent-0.3.1.jar -o /prometheus/jmx_prometheus_javaagent-0.3.1.jar && \
    curl https://s3-us-west-2.amazonaws.com/fdp-kubernetes-builds/spark/extra/spark-prom-exporter.yml -o /etc/metrics/conf/prometheus.yaml && \
    chgrp -R 0 /prometheus  && \
    chmod -R g=u /prometheus && \
    chgrp -R 0 /etc/metrics/conf  && \
    chmod -R g=u /etc/metrics/conf && \
    #chgrp -R 0 /opt/spark  && \
    #chmod -R g=u /opt/spark && \
    #chmod -R u+x /opt/spark/bin && \
    #chmod -R u+x /opt/spark/sbin && \
    curl -L https://github.com/krallin/tini/releases/download/v0.18.0/tini -o /sbin/tini && \
    chmod +x /sbin/tini

USER 185

ENV SPARK_HOME /opt/spark

WORKDIR /opt/spark/work-dir

ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Instructions

This project adds small programs to test Spark specific features.
To build the jar run: `sbt assembly`
This will create the following assembly:
lightbend-spark-kubernetes-tests-assembly-latest.jar

Then after you have created your Spark on K8s docker images you need
to modify them as follows.

- Create a derived image with the following Dockefile:
```
ARG IMG
FROM $IMG

COPY lightbend-spark-kubernetes-tests-assembly-latest.jar /lightbend-spark-kubernetes-tests-assembly-latest.jar
```
Assuming your initial image is: `skonto/spark:itests`

```
docker build  --build-arg IMG=skonto/spark:itests -t skonto/spark:itests2 .
docker push skonto/spark:itests2
```

Then on your host machine make sure you have connected to a remote DC/OS cluster which has (DC/OS KDFS, HDFS and Kuebernetes installed).

Then run:
```
export HADOOP_CONFIG_URL=http://api.hdfs.marathon.l4lb.thisdcos.directory/v1/endpoints
export EXTRA_JARS=/lightbend-spark-kubernetes-tests-assembly-latest.jar
export TGZ_PATH=<path to spark tzg to use for exampke: /path/to/spark-2.4.0-SNAPSHOT-bin-lightbend.tgz>
export KAFKA_BROKERS=broker.kafka.l4lb.thisdcos.directory:9092

./dev/dev-run-integration-tests.sh --extra-jars $EXTRA_JARS --deploy-mode cloud --spark-master k8s://http://localhost:9000 --service-account spark-sa --namespace spark --image-tag itests2 --spark-tgz $TGZ_PATH --image-repo skonto --exclude-tags noDcos
```

The above will run the tests against the remote DC/OS cluster and also will exclude any tests tagged with `noDcos` tag.
THe extra jar variable refers to the jar that resides in the docker image, and is passed to spark-submit in cluster mode directly with the flag `--jars`.

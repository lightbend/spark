/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.k8s.integrationtest

import java.io.File
import java.nio.file.{Path, Paths}
import java.util.UUID
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.util.Random

import com.google.common.io.PatternFilenameFilter
import io.fabric8.kubernetes.api.model.Pod
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.Tag
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Minutes, Seconds, Span}

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.integrationtest.TestConfig._
import org.apache.spark.deploy.k8s.integrationtest.backend.{IntegrationTestBackend, IntegrationTestBackendFactory}
import org.apache.spark.internal.Logging

object NoKerberos extends Tag("NoKerberos")
object DCOS extends Tag("DCOS")

private[spark] class LightbendKubernetesSuite extends SparkFunSuite
  with BeforeAndAfterAll with BeforeAndAfter {

  import LightbendKubernetesSuite._

  private var testBackend: IntegrationTestBackend = _
  private var sparkHomeDir: Path = _
  private var kubernetesTestComponents: KubernetesTestComponents = _
  private var sparkAppConf: SparkAppConf = _
  private var image: String = _
  private var containerLocalSparkDistroExamplesJar: String = _
  private var appLocator: String = _
  private var driverPodName: String = _

  override def beforeAll(): Unit = {
    // The scalatest-maven-plugin gives system properties that are referenced but not set null
    // values. We need to remove the null-value properties before initializing the test backend.
    val nullValueProperties = System.getProperties.asScala
      .filter(entry => entry._2.equals("null"))
      .map(entry => entry._1.toString)
    nullValueProperties.foreach { key =>
      System.clearProperty(key)
    }

    val sparkDirProp = System.getProperty("spark.kubernetes.test.unpackSparkDir")
    require(sparkDirProp != null, "Spark home directory must be provided in system properties.")
    sparkHomeDir = Paths.get(sparkDirProp)
    require(sparkHomeDir.toFile.isDirectory,
      s"No directory found for spark home specified at $sparkHomeDir.")
    val imageTag = getTestImageTag
    val imageRepo = getTestImageRepo
    image = s"$imageRepo/spark:$imageTag"

    val sparkDistroExamplesJarFile: File = sparkHomeDir.resolve(Paths.get("examples", "jars"))
      .toFile
      .listFiles(new PatternFilenameFilter(Pattern.compile("^spark-examples_.*\\.jar$")))(0)
    containerLocalSparkDistroExamplesJar = s"local:///opt/spark/examples/jars/" +
      s"${sparkDistroExamplesJarFile.getName}"
    testBackend = IntegrationTestBackendFactory.getTestBackend
    testBackend.initialize()
    kubernetesTestComponents = new KubernetesTestComponents(testBackend.getKubernetesClient)
  }

  override def afterAll(): Unit = {
    testBackend.cleanUp()
  }

  before {
    resetPodProperties()
  }

  private def resetPodProperties(): Unit = {
    appLocator = UUID.randomUUID().toString.replaceAll("-", "")
    driverPodName = "spark-test-app-" + UUID.randomUUID().toString.replaceAll("-", "")
    sparkAppConf = kubernetesTestComponents.newSparkAppConf()
      .set("spark.kubernetes.container.image", image)
      .set("spark.kubernetes.driver.pod.name", driverPodName)
      .set("spark.kubernetes.driver.label.spark-app-locator", appLocator)
      .set("spark.kubernetes.executor.label.spark-app-locator", appLocator)
    if (!kubernetesTestComponents.hasUserSpecifiedNamespace) {
      kubernetesTestComponents.createNamespace()
    }
  }

  private def cleanup() = {
    if (!kubernetesTestComponents.hasUserSpecifiedNamespace) {
      kubernetesTestComponents.deleteNamespace()
    }
    deleteDriverPod()
  }

  after {
    cleanup()
  }

  test("Run basic read/write HDFS job (DFSReadWriteTest)", Array(NoKerberos, DCOS): _ *) {
    runDFSReadWriteTestAndVerifyCompletion()
  }

  test("Run basic read/write Kafka job (KafkaToHdfsWithCheckpointing)",
    Array(NoKerberos, DCOS): _ *) {
    val topic = s"test-topic-${Random.alphanumeric.take(5).mkString}"
    // Write data to Kafka by submitting a main
    runProducerTestAndVerifyCompletion(topic = topic)
    cleanup()
    resetPodProperties()
    // Read from kafka with structured streaming
    runKafkaWithStructuredStreamingTestAndVerifyCompletion(topic = topic)
  }

  private def runDFSReadWriteTestAndVerifyCompletion(
      appResource: String = containerLocalSparkDistroExamplesJar,
      driverPodChecker: Pod => Unit = doBasicDriverPodCheck,
      executorPodChecker: Pod => Unit = doBasicExecutorPodCheck,
      appArgs: Array[String] = Array.empty[String],
      appLocator: String = appLocator): Unit = {
    sparkAppConf.set("spark.kubernetes.container.image.pullPolicy", "Always")

    // For DC/OS that is: http://api.hdfs.marathon.l4lb.thisdcos.directory/v1/endpoints
    val hadoopConf = sys.env.get("HADOOP_CONFIG_URL")
    require(hadoopConf.nonEmpty)
    hadoopConf
      .foreach(sparkAppConf.set("spark.kubernetes.driverEnv.HADOOP_CONFIG_URL", _))
    val args =
      appArgs ++ Array("/etc/resolv.conf", s"hdfs:///test-${Random.alphanumeric.take(5).mkString}")
    runSparkApplicationAndVerifyCompletion(
      appResource,
      DFS_READ_WRITE_CLASS,
      Seq("Success! Local Word Count"),
      args,
      driverPodChecker,
      executorPodChecker,
      appLocator)
  }

  private def runKafkaWithStructuredStreamingTestAndVerifyCompletion(
      topic: String,
      appResource: String = containerLocalSparkDistroExamplesJar,
      driverPodChecker: Pod => Unit = doBasicDriverPodCheck,
      executorPodChecker: Pod => Unit = doBasicExecutorPodCheck,
      appArgs: Array[String] = Array.empty[String],
      appLocator: String = appLocator): Unit = {
    sparkAppConf.set("spark.kubernetes.container.image.pullPolicy", "Always")

    // For DC/OS that is: broker.kafka.l4lb.thisdcos.directory:9092
    val kafkaBrokers = sys.env.get("KAFKA_BROKERS")
    require(kafkaBrokers.nonEmpty)
    val jars = System.getProperty("spark.kubernetes.test.extraJars")
    require(jars != null)
    val args = Array(
      "--topic", topic,
      "--bootstrapServers", kafkaBrokers.get,
      "--checkpointDirectory", s"hdfs:///checkpoint-${Random.alphanumeric.take(5).mkString}"
    )
    // expected string:
    // +--------+
    // |count(1)|
    // +--------+
    // |     100|
    // +--------+

    runSparkApplicationAndVerifyCompletion(
      appResource,
      KAFKA_STRUCTURED_STREAMING_CONSOLE,
      Seq("|count(1)|", "|     100|"),
      args,
      driverPodChecker,
      executorPodChecker,
      appLocator,
      Some(jars))
  }

  private def runProducerTestAndVerifyCompletion(
      topic: String,
      appResource: String = containerLocalSparkDistroExamplesJar,
      driverPodChecker: Pod => Unit = doBasicDriverPodCheck,
      executorPodChecker: Pod => Unit = doBasicExecutorPodCheck,
      appArgs: Array[String] = Array.empty[String],
      appLocator: String = appLocator): Unit = {
    sparkAppConf.set("spark.kubernetes.container.image.pullPolicy", "Always")

    // For DC/OS that is: broker.kafka.l4lb.thisdcos.directory:9092
    val kafkaBrokers = sys.env.get("KAFKA_BROKERS")
    require(kafkaBrokers.nonEmpty)
    val jars = System.getProperty("spark.kubernetes.test.extraJars")
    require(jars != null)

    val args = Array(
      "--topic", topic,
      "--bootstrapServers", kafkaBrokers.get,
      "--step", "1",
      "--maxRecords", "100"
    )
    runSparkApplicationAndVerifyCompletion(
      appResource,
      KAFKA_SIMPLE_PRODUCER_CLASS,
      Seq("Success!"),
      args,
      driverPodChecker,
      executorPodChecker,
      appLocator,
      Some(jars))
  }

  private def runSparkApplicationAndVerifyCompletion(
      appResource: String,
      mainClass: String,
      expectedLogOnCompletion: Seq[String],
      appArgs: Array[String],
      driverPodChecker: Pod => Unit,
      executorPodChecker: Pod => Unit,
      appLocator: String,
      extraJars: Option[String] = None): Unit = {
    val appArguments = SparkAppArguments(
      mainAppResource = appResource,
      mainClass = mainClass,
      appArgs = appArgs)

    CustomSparkAppLauncher.
      launch(appArguments, sparkAppConf, TIMEOUT.value.toSeconds.toInt, sparkHomeDir, extraJars)
    val driverPod = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "driver")
      .list()
      .getItems
      .get(0)
    driverPodChecker(driverPod)

    val executorPods = kubernetesTestComponents.kubernetesClient
      .pods()
      .withLabel("spark-app-locator", appLocator)
      .withLabel("spark-role", "executor")
      .list()
      .getItems
    executorPods.asScala.foreach { pod =>
      executorPodChecker(pod)
    }

    Eventually.eventually(TIMEOUT, INTERVAL) {
      expectedLogOnCompletion.foreach { e =>
        assert(kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(driverPod.getMetadata.getName)
          .getLog
          .contains(e), "The application did not complete.")
      }
    }
  }

  private def doBasicDriverPodCheck(driverPod: Pod): Unit = {
    assert(driverPod.getMetadata.getName === driverPodName)
    assert(driverPod.getSpec.getContainers.get(0).getImage === image)
    assert(driverPod.getSpec.getContainers.get(0).getName === "spark-kubernetes-driver")
  }

  private def doBasicExecutorPodCheck(executorPod: Pod): Unit = {
    assert(executorPod.getSpec.getContainers.get(0).getImage === image)
    assert(executorPod.getSpec.getContainers.get(0).getName === "executor")
  }

  private def deleteDriverPod(): Unit = {
    kubernetesTestComponents.kubernetesClient.pods().withName(driverPodName).delete()
    Eventually.eventually(TIMEOUT, INTERVAL) {
      assert(kubernetesTestComponents.kubernetesClient
        .pods()
        .withName(driverPodName)
        .get() == null)
    }
  }
}

private[spark] object LightbendKubernetesSuite {
  val TIMEOUT = PatienceConfiguration.Timeout(Span(2, Minutes))
  val INTERVAL = PatienceConfiguration.Interval(Span(2, Seconds))
  val DFS_READ_WRITE_CLASS: String = "org.apache.spark.examples.DFSReadWriteTest"
  val KAFKA_SIMPLE_PRODUCER_CLASS: String =
    "com.lightbend.fdp.spark.k8s.test.KafkaSimpleProducer"
  val KAFKA_STRUCTURED_STREAMING_CONSOLE =
    "com.lightbend.fdp.spark.k8s.test.KafkaToHdfsWithCheckpointing"
}

private[spark] object CustomSparkAppLauncher extends Logging {

  def launch(
      appArguments: SparkAppArguments,
      appConf: SparkAppConf,
      timeoutSecs: Int,
      sparkHomeDir: Path,
      extraJars: Option[String] = None): Unit = {
    val sparkSubmitExecutable = sparkHomeDir.resolve(Paths.get("bin", "spark-submit"))
    logInfo(s"Launching a spark app with arguments $appArguments and conf $appConf")

    val jars = {
      extraJars match {
      case Some(value) => Array("--jars", value)
      case None => Array[String]()
      }
    }
    val commandLine = (Array(sparkSubmitExecutable.toFile.getAbsolutePath,
      "--deploy-mode", "cluster",
      "--class", appArguments.mainClass,
      "--master", appConf.get("spark.master")
    ) ++ jars ++ appConf.toStringArray :+
      appArguments.mainAppResource) ++
      appArguments.appArgs
    val output = ProcessUtils.executeProcess(commandLine, timeoutSecs)
    // scalastyle:off println
    // output.foreach(println(_))
    // scalastyle:on println
  }
}

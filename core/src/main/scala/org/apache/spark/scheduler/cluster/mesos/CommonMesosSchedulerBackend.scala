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

package org.apache.spark.scheduler.cluster.mesos

import java.io.File
import java.util.{List => JList}
import java.util.Collections
import java.util.concurrent.locks.ReentrantLock

import com.google.common.collect.HashBiMap

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap => MutableHashMap, HashSet => MutableHashSet}

import org.apache.mesos.{Scheduler => MScheduler}
import org.apache.mesos._
import org.apache.mesos.Protos.{TaskInfo => MesosTaskInfo, TaskState => MesosTaskState, _}

import org.apache.spark.{Logging, SparkContext, SparkEnv, SparkException}
import org.apache.spark.scheduler.{SchedulerBackend, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{Utils, AkkaUtils}

/**
 * Shared code between {@link FineGrainedMesosSchedulerBackend} and
 * {@link CoarseGrainedMesosSchedulerBackend}.
 */
trait CommonMesosSchedulerBackend extends SchedulerBackend {

  self: MScheduler with Logging =>

  // TODO Move these declarations somewhere else?
  def resourceOffers(d: SchedulerDriver, offers: JList[Offer]): Unit
  def slaveLost(d: SchedulerDriver, slaveId: SlaveID): Unit
  def statusUpdate(d: SchedulerDriver, status: TaskStatus): Unit
  def requestExecutors(numAdditionalExecutors: Int): Boolean
  def requestTotalExecutors(numAdditionalExecutors: Int): Boolean
  def doKillExecutors(executorIds: Seq[String]): Boolean

  val scheduler: TaskSchedulerImpl
  val sparkContext: SparkContext
  val master: String

  /** Driver for talking to Mesos */
  var driver: SchedulerDriver = null

  // the total number of executors we aim to have
  private[mesos] var executorLimit = Int.MaxValue

  /**
   *  Return the current executor limit, which may be [[Int.MaxValue]].
   */
  def getExecutorLimit: Int = executorLimit

  protected val executorBackend: Class[_]

  private[mesos] val taskIdToSlaveId = HashBiMap.create[Long, String]

  private[mesos] val slaveIdsWithExecutors = MutableHashSet.empty[String]

  def slaveHasExecutor(slaveId: String): Boolean = {
    slaveIdsWithExecutors.contains(slaveId)
  }

  private def executorBackendName: String = executorBackend.getName
  private def executorSimpleBackendName: String = executorBackend.getSimpleName

  @volatile var appId: String = _

  // Lock used to wait for scheduler to be registered
  private var isRegistered = false
  private val registeredLock = new Object()

  // Protected lock object protecting other mutable state. Using the intrinsic lock
  // may lead to deadlocks since the superclass might also try to lock
  protected val stateLock = new ReentrantLock

  // ==== Declarations for doStart():

  protected def preStart(): Unit
  protected def postStart(): Unit

  /**
   * We would like to override <tt>start</tt> here and we almost can, except
   * unfortunately, we have to call <tt>super.start</tt> in
   * <tt>CoarseGrainedMesosSchedulerBackend.start</tt>, to invoke
   * <tt>CoarseGrainedSchedulerBackend.start</tt>), which is concrete.
   * However, for <tt>FineGrainedMesosSchedulerBackend</tt>, we _can't_ call
   * <tt>super.start</tt>, because <tt>SchedulerBackend.start</tt> is abstract.
   * So, all the common logic is implemented in this helper method and each
   * concrete class overrides <tt>start</tt> itself.
   */
  protected def doStart(): Unit = {
    preStart()

    stateLock.synchronized {
      new Thread(s"$executorSimpleBackendName driver") {
        setDaemon(true)
        override def run() {
          // i.e., val scheduler = CoarseGrainedMesosSchedulerBackend.this
          val scheduler = self
          val fwInfo = FrameworkInfo.newBuilder().
            setUser(sparkContext.sparkUser).setName(sparkContext.appName).build()
          driver = new MesosSchedulerDriver(scheduler, fwInfo, master)
          try {
            val ret = driver.run()
            logInfo("driver.run() returned with code " + ret)
          } catch {
            case e: Exception => logError("driver.run() failed", e)
          }
        }
      }.start()

      waitForRegister()
      postStart()
    }
  }

  /** Like <tt>start</tt>, we must override <tt>stop</tt> the same way. */
  protected def doStop(): Unit = {
    if (driver != null) {
      driver.stop()
    }
  }

  def createCommandInfo(extraCommandArguments: String): CommandInfo = {
    val executorSparkHome = sparkContext.conf.getOption("spark.mesos.executor.home")
      .orElse(sparkContext.getSparkHome())
      .getOrElse {
        throw new SparkException("Executor Spark home `spark.mesos.executor.home` is not set!")
      }
    val environment = Environment.newBuilder()
    sparkContext.conf.getOption("spark.executor.extraClassPath").foreach { cp =>
      environment.addVariables(
        Environment.Variable.newBuilder().setName("SPARK_CLASSPATH").setValue(cp).build())
    }
    val extraJavaOpts = sparkContext.conf.get("spark.executor.extraJavaOptions", "")

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    val prefixEnv = sparkContext.conf.getOption("spark.executor.extraLibraryPath").map { p =>
      Utils.libraryPathEnvPrefix(Seq(p))
    }.getOrElse("")

    environment.addVariables(
      Environment.Variable.newBuilder()
        .setName("SPARK_EXECUTOR_OPTS")
        .setValue(extraJavaOpts)
        .build())

    sparkContext.executorEnvs.foreach { case (key, value) =>
      environment.addVariables(Environment.Variable.newBuilder()
        .setName(key)
        .setValue(value)
        .build())
    }
    val command = CommandInfo.newBuilder()
      .setEnvironment(environment)

    val uri = sparkContext.conf.get("spark.executor.uri", null)
    if (uri == null) {
      val executorPath= new File(executorSparkHome, "./bin/spark-class").getCanonicalPath
      command.setValue("%s \"%s\" %s %s".format(
        prefixEnv, executorPath, executorBackendName, extraCommandArguments))
    } else {
      // Grab everything to the first '.'. We'll use that and '*' to
      // glob the directory "correctly".
      val basename = uri.split('/').last.split('.').head
      command.setValue("cd %s*; %s \"%s\" %s %s".format(
        basename, prefixEnv, "./bin/spark-class", executorBackendName, extraCommandArguments))
      command.addUris(CommandInfo.URI.newBuilder().setValue(uri))
    }
    command.build()
  }

  /** Handle rescinding of an offer from Mesos. */
  override def offerRescinded(d: SchedulerDriver, o: OfferID): Unit = {}

  /**
   * Implements <tt>registered</tt> for coarse grained, but the fine grained
   * implementation wraps it in the separate class loader.
   */
  protected def doRegistered(
      d: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit = {
    appId = frameworkId.getValue
    logInfo("Registered as framework ID " + appId)
    registeredLock.synchronized {
      isRegistered = true
      registeredLock.notifyAll()
    }
  }

  /** Busy-wait for registration to complete. */
  def waitForRegister(): Unit = {
    registeredLock.synchronized {
      while (!isRegistered) {
        registeredLock.wait()
      }
    }
  }

  override def disconnected(d: SchedulerDriver): Unit = {}

  override def reregistered(d: SchedulerDriver, masterInfo: MasterInfo): Unit = {}


  /** Helper function to pull out a resource from a Mesos Resources protobuf */
  protected def getResource(res: JList[Resource], name: String): Double = {
    for (r <- res if r.getName == name) {
      return r.getScalar.getValue
    }
    0
  }

  /** Check whether a Mesos task state represents a finished task */
  protected def isFinished(state: MesosTaskState): Boolean = {
    state == MesosTaskState.TASK_FINISHED ||
    state == MesosTaskState.TASK_FAILED   ||
    state == MesosTaskState.TASK_KILLED   ||
    state == MesosTaskState.TASK_LOST
  }

  /**
   * Implements <tt>error</tt> for coarse grained, but the fine grained
   * implementation wraps it in the separate class loader.
   */
  protected def doError(d: SchedulerDriver, message: String): Unit = {
    logError("Mesos error: " + message)
    scheduler.error(message)
  }

  override def frameworkMessage(
    d: SchedulerDriver, e: ExecutorID, s: SlaveID, b: Array[Byte]): Unit = {}
}

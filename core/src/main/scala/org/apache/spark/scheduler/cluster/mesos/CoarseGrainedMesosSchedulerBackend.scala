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
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.{Utils, AkkaUtils}

/**
 * A SchedulerBackend that runs tasks on Mesos, but uses "coarse-grained" tasks, where it holds
 * onto each Mesos node for the duration of the Spark job instead of relinquishing cores whenever
 * a task is done. It launches Spark tasks within the coarse-grained Mesos tasks using the
 * CoarseGrainedSchedulerBackend mechanism. This class is useful for lower and more predictable
 * latency.
 *
 * Unfortunately this has a bit of duplication from FineGrainedMesosSchedulerBackend, but it seems hard to
 * remove this.
 */
private[spark] class CoarseGrainedMesosSchedulerBackend(
    val scheduler: TaskSchedulerImpl,
    val sc: SparkContext,
    val master: String)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem)
  with CommonMesosSchedulerBackend
  with MScheduler
  with Logging {

  val MAX_SLAVE_FAILURES = 2     // Blacklist a slave after this many failures

  // Maximum number of cores to acquire (TODO: we'll need more flexible controls here.)
  private[mesos] val maxCores = conf.get("spark.cores.max",  Int.MaxValue.toString).toInt

  /** Cores we have acquired with each Mesos task ID */
  private[mesos] val coresByTaskId = MutableHashMap.empty[Int, Int]

  private[mesos] var totalCoresAcquired = 0

  // How many times tasks on each slave failed?
  private[mesos] val failuresBySlaveId = MutableHashMap.empty[String, Int]

  private[mesos] val pendingRemovedSlaveIds = MutableHashSet.empty[String]

  val extraCoresPerSlave = conf.getInt("spark.mesos.extra.cores", 0)

  /** Return a new task id for coarse-grained mode. */
  def newMesosTaskId(): Int = {
    val id = nextMesosTaskId
    nextMesosTaskId += 1
    id
  }

  // ==== Definitions for start():

  // Nothing to do
  protected def preStart(): Unit = {}

  // Nothing to do
  protected def postStart(): Unit = {}

  /** @see CommonMesosSchedulerBackend.doStart() */
  override def start() = {
    super.start()
    doStart()
  }

  /** @see CommonMesosSchedulerBackend.doStop() */
  override def stop() {
    super.stop()
    doStop()
  }

  def createCommand(offer: Offer, numCores: Int): CommandInfo = {
    val extraCommandArguments =
        s" --driver-url $driverUrl" +
        s" --executor-id ${offer.getSlaveId.getValue}" +
        s" --hostname ${offer.getHostname}" +
        s" --cores $numCores" +
        s" --app-id $appId"
    createCommandInfo(extraCommandArguments)
  }

  protected def driverUrl: String = {
    AkkaUtils.address(
      AkkaUtils.protocol(sc.env.actorSystem),
      SparkEnv.driverActorSystemName,
      conf.get("spark.driver.host"),
      conf.get("spark.driver.port"),
      CoarseGrainedSchedulerBackend.ACTOR_NAME)
  }

  override def registered(d: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) = {
    doRegistered(d: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo)
  }

  /**
   * Method called by Mesos to offer resources on slaves. We respond by launching an executor,
   * unless we've already launched more than we wanted to.
   */
  override def resourceOffers(d: SchedulerDriver, offers: JList[Offer]) {
    stateLock.synchronized {
      val filters = Filters.newBuilder().setRefuseSeconds(-1).build()

      for (offer <- offers) {
        val slaveId = offer.getSlaveId.getValue
        val mem = getResource(offer.getResourcesList, "mem")
        val cpus = getResource(offer.getResourcesList, "cpus").toInt
        if (taskIdToSlaveId.size < executorLimit &&
            totalCoresAcquired < maxCores &&
            mem >= MemoryUtils.calculateTotalMemory(sc) &&
            cpus >= 1 &&
            failuresBySlaveId.getOrElse(slaveId, 0) < MAX_SLAVE_FAILURES &&
            !slaveIdsWithExecutors.contains(slaveId)) {
          // Launch an executor on the slave
          val cpusToUse = math.min(cpus, maxCores - totalCoresAcquired)
          totalCoresAcquired += cpusToUse
          val taskId = newMesosTaskId()
          taskIdToSlaveId(taskId) = slaveId
          slaveIdsWithExecutors += slaveId
          coresByTaskId(taskId) = cpusToUse
          val task = MesosTaskInfo.newBuilder()
            .setTaskId(TaskID.newBuilder().setValue(taskId.toString).build())
            .setSlaveId(offer.getSlaveId)
            .setCommand(createCommand(offer, cpusToUse + extraCoresPerSlave))
            .setName("Task " + taskId)
            .addResources(createResource("cpus", cpusToUse))
            .addResources(createResource("mem",
              MemoryUtils.calculateTotalMemory(sc)))
            .build()
          d.launchTasks(
            Collections.singleton(offer.getId),  Collections.singletonList(task), filters)
        } else {
          // Filter it out
          driver.declineOffer(offer.getId)
        }
      }
    }
  }

  /** Build a Mesos resource protobuf object */
  private def createResource(resourceName: String, quantity: Double): Protos.Resource = {
    Resource.newBuilder()
      .setName(resourceName)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(quantity).build())
      .build()
  }


  override def statusUpdate(d: SchedulerDriver, status: TaskStatus): Unit = {
    val taskId = status.getTaskId.getValue.toInt
    val state = status.getState
    logInfo("Mesos task " + taskId + " is now " + state)
    stateLock.synchronized {
      if (isFinished(state)) {
        val slaveId = taskIdToSlaveId(taskId)
        slaveIdsWithExecutors -= slaveId
        taskIdToSlaveId -= taskId
        // Remove the cores we have remembered for this task, if it's in the hashmap
        for (cores <- coresByTaskId.get(taskId)) {
          totalCoresAcquired -= cores
          coresByTaskId -= taskId
        }
        // If it was a failure, mark the slave as failed for blacklisting purposes
        if (state == MesosTaskState.TASK_FAILED || state == MesosTaskState.TASK_LOST) {
          failuresBySlaveId(slaveId) = failuresBySlaveId.getOrElse(slaveId, 0) + 1
          if (failuresBySlaveId(slaveId) >= MAX_SLAVE_FAILURES) {
            logInfo("Blacklisting Mesos slave " + slaveId + " due to too many failures; " +
                "is Spark installed on it?")
          }
        }
        executorTerminated(d, slaveId, s"Executor finished with state $state")
        driver.reviveOffers() // In case we'd rejected everything before but have now lost a node
      }
    }
  }

  override def error(d: SchedulerDriver, message: String) = doError(d, message)

  /** Called when a slave is lost or a Mesos task finished. Update local view on
   *  what tasks are running and remove the terminated slave from the list of pending
   *  slave IDs that we might have asked to be killed. It also notifies the driver
   *  that an executor was removed.
   */
  private def executorTerminated(d: SchedulerDriver, slaveId: String, reason: String) {
    stateLock.synchronized {
      if (slaveIdsWithExecutors.contains(slaveId)) {
        val slaveIdToTaskId = taskIdToSlaveId.inverse()
        if (slaveIdToTaskId.contains(slaveId)) {
          val taskId: Long = slaveIdToTaskId.get(slaveId)
          taskIdToSlaveId.remove(taskId)
          removeExecutor(sparkExecutorId(slaveId, taskId.toString), reason)
        }
        pendingRemovedSlaveIds -= slaveId
        slaveIdsWithExecutors -= slaveId
      }
    }
  }

  private def sparkExecutorId(slaveId: String, taskId: String) = "%s/%s".format(slaveId, taskId)

  override def slaveLost(d: SchedulerDriver, slaveId: SlaveID) {
    val sid = slaveId.getValue
    logInfo("Mesos slave lost: " + sid)
    executorTerminated(d, sid, "Mesos slave lost: " + sid)
  }

  override def executorLost(d: SchedulerDriver, e: ExecutorID, s: SlaveID, status: Int) {
    logInfo("Executor lost: %s, marking slave %s as lost".format(e.getValue, s.getValue))
    slaveLost(d, s)
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Boolean = {
    // We don't truly know if we can fulfill the full amount of executors
    // since at coarse grain it depends on the amount of slaves available.
    logInfo("Capping the total amount of executors to " + requestedTotal)
    executorLimit = requestedTotal
    true
  }

  override def doKillExecutors(executorIds: Seq[String]): Boolean = {
    if (driver == null) {
      logWarning("Asked to kill executors before the executor was started.")
      return false
    }

    val slaveIdToTaskId = taskIdToSlaveId.inverse()
    for (executorId <- executorIds) {
      val slaveId = executorId.split("/")(0)
      if (slaveIdToTaskId.contains(slaveId)) {
        driver.killTask(
          TaskID.newBuilder().setValue(slaveIdToTaskId.get(slaveId).toString).build)
        pendingRemovedSlaveIds += slaveId
      } else {
        logWarning("Unable to find executor Id '" + executorId + "' in Mesos scheduler")
      }
    }

    assert(pendingRemovedSlaveIds.size <= taskIdToSlaveId.size)

    // We cannot simply decrement from the existing executor limit as we may not able to
    // launch as much executors as the limit. But we assume if we are notified to kill
    // executors, that means the scheduler wants to set the limit that is less than
    // the amount of the executors that has been launched. Therefore, we take the existing
    // amount of executors launched and deduct the executors killed as the new limit.
    executorLimit = taskIdToSlaveId.size - pendingRemovedSlaveIds.size
    true
  }
}

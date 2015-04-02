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

import java.util
import java.util.Collections

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.apache.mesos.Protos.Value.Scalar
import org.apache.mesos.Protos._
import org.apache.mesos.SchedulerDriver
import org.apache.spark.scheduler.{SchedulerBackend, TaskSchedulerImpl}
import org.apache.spark.{ LocalSparkContext, SparkConf, SparkEnv, SparkContext }
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{ ArgumentCaptor, Matchers }
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable

trait MesosSchedulerBackendSuiteHelper {
  self: FunSuite with LocalSparkContext with MockitoSugar =>

  private def makeOffer(offerId: String, slaveId: String, mem: Int, cpu: Int) = {
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(mem))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(cpu))
    builder.setId(OfferID.newBuilder().setValue(offerId).build()).setFrameworkId(FrameworkID.newBuilder().setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId)).setHostname(s"host$slaveId").build()
  }

  private def makeOffersList(offers: Offer*): java.util.ArrayList[Offer] = {
    val mesosOffers = new java.util.ArrayList[Offer]
    for (o <- offers) mesosOffers.add(o)
    mesosOffers
  }

  private def mockSparkContext: SparkContext = {
    val sparkConf = new SparkConf
    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")
    val se = mock[SparkEnv]
    val sc = mock[SparkContext]
    when(sc.executorMemory).thenReturn(100)
    when(sc.getSparkHome()).thenReturn(Option("/path"))

    val emptyHashMap = mutable.HashMap.empty[String, String]
    when(sc.executorEnvs).thenReturn(emptyHashMap)
    when(sc.conf).thenReturn(sparkConf)
    when(sc.env).thenReturn(se)
    sc
  }

  private def mockEnvironment: (SparkContext, TaskSchedulerImpl, SchedulerDriver) = {
    val sc = mockSparkContext
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)
    (sc, taskScheduler, driver)
  }

  private def makeBackendAndDriver: (CommonMesosSchedulerBackend, SchedulerDriver) = {
    val (sc, taskScheduler, driver) = mockEnvironment
    val backend = makeTestMesosSchedulerBackend(taskScheduler)
    backend.driver = driver
    (backend, driver)
  }

  private def makeTaskID( id: String): TaskID  = TaskID.newBuilder().setValue(id).build()
  private def makeSlaveID(id: String): SlaveID = SlaveID.newBuilder().setValue(id).build()
  private def makeOfferID(id: String): OfferID = OfferID.newBuilder().setValue(id).build()

  // Simulate task killed message, signaling that an executor is no longer running.
  private def makeKilledTaskStatus(taskId: String, slaveId: String) =
    TaskStatus.newBuilder()
      .setTaskId(makeTaskID(taskId))
      .setSlaveId(makeSlaveID(slaveId))
      .setState(TaskState.TASK_KILLED)
      .build

  private def minMemMinCPU(sc: SparkContext, extraMemory: Int = 0, numCores: Int = 4): (Int,Int) =
    (MemoryUtils.calculateTotalMemory(sc).toInt + extraMemory, numCores)

  protected def makeTestMesosSchedulerBackend(
      taskScheduler: TaskSchedulerImpl): CommonMesosSchedulerBackend

  val (taskIDVal1, slaveIDVal1) = ("0", "s1")
  val (taskIDVal2, slaveIDVal2) = ("1", "s2")

  def makeMesosExecutorsTest(): (CommonMesosSchedulerBackend, SchedulerDriver) = {
    val (backend, driver) = makeBackendAndDriver

    val (minMem, minCPU) = minMemMinCPU(backend.sparkContext)

    val mesosOffers1 = makeOffersList(makeOffer(taskIDVal1, slaveIDVal1, minMem, minCPU))

    backend.resourceOffers(driver, mesosOffers1)
    verify(driver, times(1)).launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers1.get(0).getId)),
        any[util.Collection[TaskInfo]],
        any[Filters])

    // Verify we have one executor and the executor limit is 1.
    assert(backend.slaveIdsWithExecutors.size === 1)
    assert(backend.getExecutorLimit >= 1)

    (backend, driver)  // Return so this test can be embedded in others.
  }

  def killMesosExecutorDeprecateByOneTest(): (CommonMesosSchedulerBackend, SchedulerDriver) = {
    val (backend, driver) = makeMesosExecutorsTest()

    // Calling doKillExecutors should invoke driver.killTask.
    val taskID1 = makeTaskID(taskIDVal1)
    assert(backend.doKillExecutors(Seq(s"$slaveIDVal1/$taskIDVal1")))
    verify(driver, times(1)).killTask(taskID1)
    // Must invoke the status update explicitly here.
    // TODO: can we mock other parts of the API so this can be called automatically?
    backend.statusUpdate(driver, makeKilledTaskStatus(taskIDVal1, slaveIDVal1))

    // Verify we don't have any executors.
    assert(backend.slaveIdsWithExecutors.size === 0)
    // Verify that the executor limit is now 0.
    assert(backend.getExecutorLimit === 0)

    val (minMem, minCPU) = minMemMinCPU(backend.sparkContext)
    val mesosOffers2 = makeOffersList(makeOffer(taskIDVal2, slaveIDVal2, minMem, minCPU))
    backend.resourceOffers(driver, mesosOffers2)

    verify(driver, times(1))
      .declineOffer(makeOfferID(taskIDVal2))

    // Verify we didn't launch any new executor
    assert(backend.slaveIdsWithExecutors.size === 0)
    assert(backend.getExecutorLimit === 0)

    (backend, driver)  // Return so this test can be embedded in others.
  }

  def increaseAllowedMesosExecutorsTest(): (CommonMesosSchedulerBackend, SchedulerDriver) = {
    val (backend, driver) = killMesosExecutorDeprecateByOneTest()

    val (minMem, minCPU) = minMemMinCPU(backend.sparkContext)
    val mesosOffers2 = makeOffersList(makeOffer(taskIDVal2, slaveIDVal2, minMem, minCPU))

    // Now allow one more executor:
    backend.requestExecutors(1)
    backend.resourceOffers(driver, mesosOffers2)
    verify(driver, times(1)).launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers2.get(0).getId)),
        any[util.Collection[TaskInfo]],
        any[Filters])

    assert(backend.slaveIdsWithExecutors.size === 1)
    assert(backend.getExecutorLimit >= 1)

    (backend, driver)  // Return so this test can be embedded in others.
  }

  def slaveLostDoesntChangeMaxAllowedMesosExecutorsTest(): Unit = {
    val (backend, driver) = increaseAllowedMesosExecutorsTest()

    backend.slaveLost(driver, makeSlaveID(slaveIDVal2))
    assert(backend.slaveIdsWithExecutors.size === 0)
    assert(backend.getExecutorLimit >= 1)
  }

  def killAndRelaunchTasksTest(): Unit = {
    val (backend, driver) = makeBackendAndDriver
    val (minMem, minCPU) = minMemMinCPU(backend.sparkContext, 1024)
    val offer1 = makeOffer(taskIDVal1, slaveIDVal1, minMem, minCPU)
    val mesosOffers = makeOffersList(offer1)

    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(offer1.getId)),
      anyObject(),
      anyObject[Filters])
    assert(backend.slaveIdsWithExecutors.contains(slaveIDVal1))

    backend.statusUpdate(driver, makeKilledTaskStatus(taskIDVal1, slaveIDVal1))
    assert(!backend.slaveIdsWithExecutors.contains(slaveIDVal1))
    assert(backend.slaveIdsWithExecutors.size === 0)
    assert(backend.getExecutorLimit >= 1)

    val offer2 = makeOffer(taskIDVal2, slaveIDVal2, minMem, 1)
    mesosOffers.clear()
    mesosOffers.add(offer2)
    backend.resourceOffers(driver, mesosOffers)
    assert(!backend.slaveIdsWithExecutors.contains(slaveIDVal1))
    assert( backend.slaveIdsWithExecutors.contains(slaveIDVal2))
    assert(backend.slaveIdsWithExecutors.size === 1)
    assert(backend.getExecutorLimit >= 1)

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(offer2.getId)),
      anyObject(),
      anyObject[Filters])

    verify(driver, times(1)).reviveOffers()
  }
}

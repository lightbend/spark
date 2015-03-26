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

package org.apache.spark.scheduler.mesos

import java.util
import java.util.Collections

import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.apache.mesos.Protos.Value.Scalar
import org.apache.mesos.Protos._
import org.apache.mesos.SchedulerDriver
import org.apache.spark.scheduler.{SchedulerBackend, TaskSchedulerImpl}
import org.apache.spark.scheduler.cluster.mesos.{ CommonMesosSchedulerBackend, MemoryUtils }
import org.apache.spark.{ LocalSparkContext, SparkConf, SparkEnv, SparkContext }
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{ ArgumentCaptor, Matchers }
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable

trait MesosSchedulerBackendSuiteHelper {
  self: FunSuite with LocalSparkContext with MockitoSugar =>

  private def createOffer(offerId: String, slaveId: String, mem: Int, cpu: Int) = {
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


  private def mockEnvironment(): (SparkContext, TaskSchedulerImpl, SchedulerDriver) = {
    val sparkConf = new SparkConf
    sparkConf.set("spark.driver.host", "driverHost")
    sparkConf.set("spark.driver.port", "1234")

    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    val se = mock[SparkEnv]
    val sc = mock[SparkContext]
    when(sc.executorMemory).thenReturn(100)
    when(sc.getSparkHome()).thenReturn(Option("/path"))

    val emptyHashMap = mutable.HashMap.empty[String, String]
    when(sc.executorEnvs).thenReturn(emptyHashMap)
    when(sc.conf).thenReturn(sparkConf)
    when(sc.env).thenReturn(se)
    when(taskScheduler.sc).thenReturn(sc)
    (sc, taskScheduler, driver)
  }

  // Simulate task killed, executor no longer running
  private def makeKilledTaskStatus = TaskStatus.newBuilder()
    .setTaskId(TaskID.newBuilder().setValue("0").build())
    .setSlaveId(SlaveID.newBuilder().setValue("s1").build())
    .setState(TaskState.TASK_KILLED)
    .build


  protected def makeTestMesosSchedulerBackend(
      taskScheduler: TaskSchedulerImpl,
      sc: SparkContext): CommonMesosSchedulerBackend

  def killAndLimitExecutors() {
    val (sc, taskScheduler, driver) = mockEnvironment()

    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt
    val minCpu = 4

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(createOffer("o1", "s1", minMem, minCpu))

    val taskID0 = TaskID.newBuilder().setValue("0").build()

    val backend = makeTestMesosSchedulerBackend(taskScheduler, sc)
    backend.driver = driver
    backend.resourceOffers(driver, mesosOffers)
    verify(driver, times(1)).launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
        any[util.Collection[TaskInfo]],
        any[Filters])

    // Calling doKillExecutors should invoke driver.killTask.
    // TODO: Shouldn't we call killExecutors??
    assert(backend.doKillExecutors(Seq("s1/0")))
    verify(driver, times(1)).killTask(taskID0)
    // Must invoke the status update explicitly here.
    backend.statusUpdate(driver, makeKilledTaskStatus)

    // Verify we don't have any executors.
    assert(backend.slaveIdsWithExecutors.size === 0)
    // Verify that the executor limit is now 0.
    assert(backend.getExecutorLimit === 0)

    val mesosOffers2 = new java.util.ArrayList[Offer]
    mesosOffers2.add(createOffer("o2", "s2", minMem, minCpu))
    backend.resourceOffers(driver, mesosOffers2)

    verify(driver, times(1))
      .declineOffer(OfferID.newBuilder().setValue("o2").build())

    // Verify we didn't launch any new executor
    assert(backend.slaveIdsWithExecutors.size === 0)
    assert(backend.getExecutorLimit === 0)

    // Now allow one executor:
    backend.requestExecutors(1)
    backend.resourceOffers(driver, mesosOffers2)
    verify(driver, times(1)).launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers2.get(0).getId)),
        any[util.Collection[TaskInfo]],
        any[Filters])

    assert(backend.slaveIdsWithExecutors.size === 1)
    backend.slaveLost(driver, SlaveID.newBuilder().setValue("s2").build())
    assert(backend.slaveIdsWithExecutors.size === 0)
  }

  def killAndRelaunchTasks() {
    val (sc, taskScheduler, driver) = mockEnvironment()

    val minMem = MemoryUtils.calculateTotalMemory(sc).toInt + 1024
    val minCpu = 4

    val offer1 = createOffer("o1", "s1", minMem, minCpu)
    val offer2 = createOffer("o2", "s2", minMem, 1)

    val mesosOffers = new java.util.ArrayList[Offer]
    mesosOffers.add(offer1)  // Just offer 1 now.

    val backend = makeTestMesosSchedulerBackend(taskScheduler, sc)
    backend.driver = driver
    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(offer1.getId)),
      anyObject(),
      anyObject[Filters])
    assert(backend.slaveIdsWithExecutors.contains("s1"))

    backend.statusUpdate(driver, makeKilledTaskStatus)
    assert(!backend.slaveIdsWithExecutors.contains("s1"))

    mesosOffers.clear()
    mesosOffers.add(offer2)
    backend.resourceOffers(driver, mesosOffers)
    assert(!backend.slaveIdsWithExecutors.contains("s1"))
    assert( backend.slaveIdsWithExecutors.contains("s2"))

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(offer2.getId)),
      anyObject(),
      anyObject[Filters])

    verify(driver, times(1)).reviveOffers()
  }
}

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
import java.util.{ ArrayList => JArrayList }
import akka.actor.ActorSystem
import com.typesafe.config.Config
import org.apache.mesos.Protos.Value.Scalar
import org.apache.mesos.Protos.{TaskState => MesosTaskState, _}
import org.apache.mesos.SchedulerDriver
import org.apache.spark.scheduler.{ LiveListenerBus, SchedulerBackend, TaskSchedulerImpl }
import org.apache.spark.{ LocalSparkContext, SparkConf, SparkEnv, SparkContext }
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{ ArgumentCaptor, Matchers }
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable

trait MesosSchedulerBackendSuiteHelper[MSB <: CommonMesosSchedulerBackend] {
  self: FunSuite with LocalSparkContext with MockitoSugar =>

  protected def makeTestMesosSchedulerBackend(
      taskScheduler: TaskSchedulerImpl): MSB

  protected def makeOffer(offerId: String, slaveId: String, mem: Int, cpu: Int) = {
    val builder = Offer.newBuilder()
    builder.addResourcesBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(mem))
    builder.addResourcesBuilder()
      .setName("cpus")
      .setType(Value.Type.SCALAR)
      .setScalar(Scalar.newBuilder().setValue(cpu))
    builder.setId(OfferID.newBuilder().setValue(offerId).build())
      .setFrameworkId(FrameworkID.newBuilder().setValue("f1"))
      .setSlaveId(SlaveID.newBuilder().setValue(slaveId))
      .setHostname(s"host_$slaveId").build()
  }

  protected def makeOffersList(offers: Offer*): JArrayList[Offer] = {
    val mesosOffers = new JArrayList[Offer]
    for (o <- offers) mesosOffers.add(o)
    mesosOffers
  }

  protected def makeMockSparkContext(): SparkContext = {
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

    val listenerBus = mock[LiveListenerBus]
    when(sc.listenerBus).thenReturn(listenerBus)

    sc
  }

  protected def resetTaskScheduler(taskScheduler: TaskSchedulerImpl): TaskSchedulerImpl = {
    val sc = taskScheduler.sc
    reset(taskScheduler)
    when(taskScheduler.sc).thenReturn(sc)
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)
    taskScheduler
  }

  protected def makeMockEnvironment(): (SparkContext, TaskSchedulerImpl, SchedulerDriver) = {
    val sc = makeMockSparkContext()
    val driver = mock[SchedulerDriver]
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)
    (sc, taskScheduler, driver)
  }

  protected def makeBackendAndDriver(): (MSB, SchedulerDriver) = {
    val (sc, taskScheduler, driver) = makeMockEnvironment()
    val backend = makeTestMesosSchedulerBackend(taskScheduler)
    backend.driver = driver
    (backend, driver)
  }

  protected def makeTaskID( id: String): TaskID  = TaskID.newBuilder().setValue(id).build()
  protected def makeSlaveID(id: String): SlaveID = SlaveID.newBuilder().setValue(id).build()
  protected def makeOfferID(id: String): OfferID = OfferID.newBuilder().setValue(id).build()

  // Simulate task killed message, signaling that an executor is no longer running.
  protected def makeKilledTaskStatus(taskId: String, slaveId: String, state: MesosTaskState = MesosTaskState.TASK_KILLED) =
    TaskStatus.newBuilder()
      .setTaskId(makeTaskID(taskId))
      .setSlaveId(makeSlaveID(slaveId))
      .setState(state)
      .build

  protected def minMemMinCPU(sc: SparkContext, extraMemory: Int = 0, numCores: Int = 4): (Int,Int) =
    (MemoryUtils.calculateTotalMemory(sc).toInt + extraMemory, numCores)

}

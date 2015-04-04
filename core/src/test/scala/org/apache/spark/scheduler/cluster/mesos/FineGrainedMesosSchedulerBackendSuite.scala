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

import java.nio.ByteBuffer
import java.util
import java.util.Collections

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.mesos.SchedulerDriver
import org.apache.mesos.Protos._
import org.apache.mesos.Protos.Value.Scalar
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.mockito.{ArgumentCaptor, Matchers}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext}
import org.apache.spark.executor.MesosExecutorBackend
import org.apache.spark.scheduler.{LiveListenerBus, SparkListenerExecutorAdded,
  TaskDescription, TaskSchedulerImpl, WorkerOffer}
import org.apache.spark.scheduler.cluster.ExecutorInfo

class FineGrainedMesosSchedulerBackendSuite
  extends FunSuite
  with MesosSchedulerBackendSuiteHelper
  with LocalSparkContext
  with MockitoSugar {

  protected def makeTestMesosSchedulerBackend(
      taskScheduler: TaskSchedulerImpl): FineGrainedMesosSchedulerBackend = {
    new FineGrainedMesosSchedulerBackend(taskScheduler, taskScheduler.sc, "master")
  }

  test("The spark-class location is correctly computed") {
    val sc = makeMockSparkContext()
    sc.conf.set("spark.mesos.executor.home" , "/mesos-home")

    sc.listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1", new ExecutorInfo("host1", 2, Map.empty)))

    when(sc.getSparkHome()).thenReturn(Option("/spark-home"))

    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val mesosSchedulerBackend = new FineGrainedMesosSchedulerBackend(taskScheduler, sc, "master")

    // uri is null.
    val executorInfo = mesosSchedulerBackend.createExecutorInfo("test-id")
    assert(executorInfo.getCommand.getValue === s""" "/mesos-home/bin/spark-class" ${classOf[MesosExecutorBackend].getName} """)

    // uri exists.
    sc.conf.set("spark.executor.uri", "hdfs:///test-app-1.0.0.tgz")
    val executorInfo1 = mesosSchedulerBackend.createExecutorInfo("test-id")
    assert(executorInfo1.getCommand.getValue === s"""cd test-app-1*;  "./bin/spark-class" ${classOf[MesosExecutorBackend].getName} """)
  }

  test("When Mesos resource offers are received, tasks are launched") {

    val (backend, driver) = makeBackendAndDriver()
    val taskScheduler = backend.scheduler

    val sc = taskScheduler.sc  // a mocked object
    when(sc.getSparkHome()).thenReturn(Option("/path"))
    sc.listenerBus.post(
      SparkListenerExecutorAdded(anyLong, "s1", new ExecutorInfo("host_s1", 2, Map.empty)))

    val (minMem, minCpu) = minMemMinCPU(sc)

    val mesosOffers = makeOffersList(
      makeOffer("o1", "s1", minMem,     minCpu),
      makeOffer("o2", "s2", minMem - 1, minCpu),
      makeOffer("o3", "s3", minMem,     minCpu))

    val expectedWorkerOffers = new ArrayBuffer[WorkerOffer](2)
    expectedWorkerOffers.append(new WorkerOffer(
      mesosOffers.get(0).getSlaveId.getValue,
      mesosOffers.get(0).getHostname,
      2
    ))
    expectedWorkerOffers.append(new WorkerOffer(
      mesosOffers.get(2).getSlaveId.getValue,
      mesosOffers.get(2).getHostname,
      2
    ))
    val taskDesc = new TaskDescription(1L, 0, "s1", "n1", 0, ByteBuffer.wrap(new Array[Byte](0)))
    when(taskScheduler.resourceOffers(expectedWorkerOffers)).thenReturn(Seq(Seq(taskDesc)))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)

    val capture = ArgumentCaptor.forClass(classOf[util.Collection[TaskInfo]])
    when(
      driver.launchTasks(
        Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
        capture.capture(),
        any(classOf[Filters])
      )
    ).thenReturn(Status.valueOf(1))
    when(driver.declineOffer(mesosOffers.get(1).getId)).thenReturn(Status.valueOf(1))
    when(driver.declineOffer(mesosOffers.get(2).getId)).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers)

    verify(driver, times(1)).launchTasks(
      Matchers.eq(Collections.singleton(mesosOffers.get(0).getId)),
      capture.capture(),
      any(classOf[Filters])
    )
    verify(driver, times(1)).declineOffer(mesosOffers.get(1).getId)
    verify(driver, times(1)).declineOffer(mesosOffers.get(2).getId)
    assert(capture.getValue.size() === 1)
    val taskInfo = capture.getValue.iterator().next()
    assert(taskInfo.getName === "n1")
    val cpus = taskInfo.getResourcesList.get(0)
    assert(cpus.getName === "cpus")
    val actual = cpus.getScalar.getValue - 2.0
    val delta = 0.00001
    assert(actual >= - delta && actual <= delta)
    assert(taskInfo.getSlaveId.getValue === "s1")

    // Unwanted resources offered on an existing node. Make sure they are declined
    val mesosOffers2 = makeOffersList(makeOffer("o1", "s1", minMem, minCpu))
    reset(taskScheduler)
    reset(driver)
    when(taskScheduler.resourceOffers(any(classOf[Seq[WorkerOffer]]))).thenReturn(Seq(Seq()))
    when(taskScheduler.CPUS_PER_TASK).thenReturn(2)
    when(driver.declineOffer(mesosOffers2.get(0).getId)).thenReturn(Status.valueOf(1))

    backend.resourceOffers(driver, mesosOffers2)
    verify(driver, times(1)).declineOffer(mesosOffers2.get(0).getId)
  }
}

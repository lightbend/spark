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
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.mesos.{ CoarseGrainedMesosSchedulerBackend, MemoryUtils }
import org.apache.spark.{ LocalSparkContext, SparkConf, SparkEnv, SparkContext }
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.{ ArgumentCaptor, Matchers }
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import scala.collection.mutable

class CoarseGrainedMesosSchedulerBackendSuite
  extends FunSuite
  with MesosSchedulerBackendSuiteHelper
  with LocalSparkContext
  with MockitoSugar {

  protected def makeTestMesosSchedulerBackend(
      taskScheduler: TaskSchedulerImpl): CoarseGrainedMesosSchedulerBackend = {
    new CoarseGrainedMesosSchedulerBackend(taskScheduler, taskScheduler.sc, "master") {
      override val driverUrl = "<stub>"
    }
  }

  test("When Mesos offers resources, a Mesos executor is created.") {
    makeMesosExecutorsTest
  }

  test("When a Mesos executor is killed, the maximum number of allowed Mesos executors is deprecated by one") {
    killMesosExecutorDeprecateByOneTest()
  }

  test("The maximum number of allowed Mesos executors can be increased by explicitly requesting new Mesos executors") {
    increaseAllowedMesosExecutorsTest()
  }

  test("Losing a slave and its Mesos executor doesn't change the maximum allowed number of Mesos executors") {
    slaveLostDoesntChangeMaxAllowedMesosExecutorsTest()
  }

  test("mesos supports killing and relaunching tasks with executors") {
    killAndRelaunchTasksTest()
  }
}

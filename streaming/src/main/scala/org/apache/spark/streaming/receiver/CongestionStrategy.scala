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

package org.apache.spark.streaming.receiver

import scala.collection.mutable.ArrayBuffer

/**
 * This trait provides a strategy to deal with a large amount of data seen
 * at a Receiver, possibly ensuing an exhaustion of resources.
 * See SPARK-7398
 * Any long blocking operation in this class will hurt the throughput.
 */
trait CongestionStrategy {

  /**
  * Called on every batch interval with the estimated maximum number of
  * elements per block that can been processed in a batch interval,
  * based on the processing speed observed over the last batch.
  */
  def onBlockBoundUpdate(bound: Int): Unit

  /**
  * Given data buffers intended for a block, and for the following block
  * mutates those buffers to an amount appropriate with respect to the
  * back-pressure information provided through `onBlockBoundUpdate`.
  */
  def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any], nextBuffer: ArrayBuffer[Any]): Unit

}

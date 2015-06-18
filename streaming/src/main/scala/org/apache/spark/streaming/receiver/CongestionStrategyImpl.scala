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
import java.util.concurrent.atomic.AtomicInteger
import java.util.Random
import org.apache.spark.Logging
import org.apache.spark.util.random.{RandomSampler, BernoulliSampler}

/**
 * This class provides a congestion strategy that ignores
 * any back-pressure information.
 * @see CongestionStrategy
 */
class IgnoreCongestionStrategy extends CongestionStrategy {

  override def onBlockBoundUpdate(bound: Int): Unit = {}

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {}
}

class PushBackCongestionStrategy(blockGenerator: BlockGenerator)
  extends ThrottlingCongestionStrategy(blockGenerator) {

  private val latestBound = new AtomicInteger(-1)

  override def onBlockBoundUpdate(bound: Int): Unit = {
    if (bound > 0) rateLimiter.updateRate(bound * 1000 / blockGenerator.blockIntervalMs.toInt)
    latestBound.set(bound)
  }

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {
    val bound = latestBound.get()
    val difference = currentBuffer.size - bound
    if (bound > 0 && difference > 0) {
      nextBuffer ++=: currentBuffer.takeRight(difference)
      currentBuffer.reduceToSize(bound)
    }
  }

}

class DropCongestionStrategy extends CongestionStrategy with Logging {

  private val latestBound = new AtomicInteger(-1)

  override def onBlockBoundUpdate(bound: Int): Unit = latestBound.set(bound)

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {
    val bound = latestBound.get()
    val difference = currentBuffer.size - bound
    if (bound > 0 && difference > 0) {
      currentBuffer.reduceToSize(bound)

      val f = bound.toDouble / currentBuffer.size
      logDebug(f"Prepared block by dropping with ratio of $f%2.2f.")
    }
  }

}

class SamplingCongestionStrategy extends CongestionStrategy with Logging {

  private val rng = RandomSampler.newDefaultRNG

  private val latestBound = new AtomicInteger(-1)

  override def onBlockBoundUpdate(bound: Int): Unit = latestBound.set(bound)

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {
    val bound = latestBound.get()
    val f = bound.toDouble / currentBuffer.size
    if (f > 0 && f < 1){
      val sampled = new BernoulliSampler(f, rng).sample(currentBuffer.toIterator).toArray

      currentBuffer.clear()
      currentBuffer ++= sampled

      logDebug(f"Prepared sampled block with ratio of $f%2.2f.")
    }
  }

}

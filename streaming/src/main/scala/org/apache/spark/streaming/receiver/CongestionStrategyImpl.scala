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
import org.apache.spark.util.random.BernoulliSampler

/**
 * This class provides a congestion strategy that ignores
 * any back-pressure information.
 * @see CongestionStrategy
 */
class IgnoreCongestionStrategy extends CongestionStrategy {

  override def onBlockBoundUpdate(bound: Int) {}

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {}
}

class PushBackCongestionStrategy(blockInterval: Long) extends CongestionStrategy {

  private val latestBound = new AtomicInteger(-1)

  override def onBlockBoundUpdate(bound: Int): Unit = latestBound.set(bound)

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {
    val bound = latestBound.get()
    val difference = currentBuffer.size - bound
    if (bound > 0 && difference > 0) {
      nextBuffer ++=: currentBuffer.takeRight(difference)
      currentBuffer.reduceToSize(bound)
    }
    // We've had our fill for the amount of time it would take us to process the difference.
    // Use the fact this is synchronized with data ingestion to prevent more data coming in.
    // We need to wait for quite less than the block interval for the next 'clock tick'
    // (and block generation) to occur normally : this only leaves 5% of the block interval
    // during which data can 'sneak in'
    val delay = math.round( blockInterval * math.min((difference.toFloat / bound), 0.9) )
    Thread.sleep(delay)
  }

}

class DropCongestionStrategy extends CongestionStrategy {

  private val latestBound = new AtomicInteger(-1)

  override def onBlockBoundUpdate(bound: Int): Unit = latestBound.set(bound)

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {
    val bound = latestBound.get()
    val difference = currentBuffer.size - bound
    if (bound > 0 && difference > 0) {
      currentBuffer.reduceToSize(bound)
    }
  }

}

class SamplingCongestionStrategy extends CongestionStrategy {

  private val latestBound = new AtomicInteger(-1)

  override def onBlockBoundUpdate(bound: Int): Unit = latestBound.set(bound)

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {
    val bound = latestBound.get()
    val f = bound.toDouble / currentBuffer.size
    val samplees = currentBuffer.to

    val sampled = new BernoulliSampler(f).sample(samplees.toIterator)

    currentBuffer.clear()
    currentBuffer ++= sampled

  }

}

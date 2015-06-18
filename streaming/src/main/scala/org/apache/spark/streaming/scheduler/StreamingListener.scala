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

package org.apache.spark.streaming.scheduler

import scala.collection.mutable.Queue

import org.apache.spark.util.Distribution
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Time

/**
 * :: DeveloperApi ::
 * Base trait for events related to StreamingListener
 */
@DeveloperApi
sealed trait StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchSubmitted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchCompleted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerBatchStarted(batchInfo: BatchInfo) extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverStarted(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverError(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

@DeveloperApi
case class StreamingListenerReceiverStopped(receiverInfo: ReceiverInfo)
  extends StreamingListenerEvent

/**
 * :: DeveloperApi ::
 * A listener interface for receiving information about an ongoing streaming
 * computation.
 */
@DeveloperApi
trait StreamingListener {

  /** Called when a receiver has been started */
  def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) { }

  /** Called when a receiver has reported an error */
  def onReceiverError(receiverError: StreamingListenerReceiverError) { }

  /** Called when a receiver has been stopped */
  def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) { }

  /** Called when a batch of jobs has been submitted for processing. */
  def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) { }

  /** Called when processing of a batch of jobs has started.  */
  def onBatchStarted(batchStarted: StreamingListenerBatchStarted) { }

  /** Called when processing of a batch of jobs has completed. */
  def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) { }
}

/**
 * :: DeveloperApi ::
 * A StreamingListener that estimates the number of elements that the previous
 * batch would have processed, for each stream, if the duration of computation
 * was one batchDuration.
 * @param batchDuration The nominal (yardstick) duration for computation.
 */
@DeveloperApi
class LatestSpeedListener(batchDuration: Duration) extends StreamingListener {
  var latestTime : Option[Time] = None
  var streamIdToElemsPerBatch: Option[Map[Int, Long]] = None
  val proportionalK: Double = 1.0
  val integralK: Double = -0.2

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted){
    this.synchronized{
      val newTime = batchCompleted.batchInfo.batchTime
      val workDelay = batchCompleted.batchInfo.processingDelay
      val waitDelay = batchCompleted.batchInfo.schedulingDelay

      if (latestTime.isEmpty || newTime > latestTime.get && workDelay.isDefined){
        latestTime = Some(newTime)
        val elements = batchCompleted.batchInfo.streamIdToNumRecords
        // Percentage of a batch interval this job computed for
        val workRatio = workDelay.get.toDouble / batchDuration.milliseconds
        // Ratio of wait to work time
        val waitRatio = waitDelay.getOrElse(0L).toDouble / workDelay.get.toDouble

        streamIdToElemsPerBatch = Some(elements.mapValues{ elements =>
        // Based on this batch's size, how many elements this setup
        // can process given one batch interval of computation
        val elementsThisBatch = elements / workRatio

        // This computes how many elements could have been processed
        // at that speed, if the wait time had been compute time
        // (i.e. given an amount of time equal to the scheduling delay),
        // Because the scheduling delay increases synchronously for
        // every job, this represents the aggregation of the error
        // signal between elements flowing in and out - the integral
        // component of a PID.
        val waitElementsThisBatch = elements * waitRatio

        val batchElements = proportionalK * elementsThisBatch + integralK * waitElementsThisBatch
        math.round(batchElements)
        })
      }
    }
  }

  def getSpeedForStreamId(streamId: Int): Option[Long] = {
    streamIdToElemsPerBatch.flatMap(_.get(streamId))
  }

}

/**
 * :: DeveloperApi ::
 * A simple StreamingListener that logs summary statistics across Spark Streaming batches
 * @param numBatchInfos Number of last batches to consider for generating statistics (default: 10)
 */
@DeveloperApi
class StatsReportListener(numBatchInfos: Int = 10) extends StreamingListener {
  // Queue containing latest completed batches
  val batchInfos = new Queue[BatchInfo]()

  override def onBatchCompleted(batchStarted: StreamingListenerBatchCompleted) {
    batchInfos.enqueue(batchStarted.batchInfo)
    if (batchInfos.size > numBatchInfos) batchInfos.dequeue()
    printStats()
  }

  def printStats() {
    showMillisDistribution("Total delay: ", _.totalDelay)
    showMillisDistribution("Processing time: ", _.processingDelay)
  }

  def showMillisDistribution(heading: String, getMetric: BatchInfo => Option[Long]) {
    org.apache.spark.scheduler.StatsReportListener.showMillisDistribution(
      heading, extractDistribution(getMetric))
  }

  def extractDistribution(getMetric: BatchInfo => Option[Long]): Option[Distribution] = {
    Distribution(batchInfos.flatMap(getMetric(_)).map(_.toDouble))
  }
}

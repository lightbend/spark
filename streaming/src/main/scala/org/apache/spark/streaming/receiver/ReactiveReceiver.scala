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

import org.reactivestreams._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.annotation.DeveloperApi
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success, Failure}
import java.lang.IllegalStateException

/**
 * An abstract class thet helps in implementing a Receiver compliant with the
 * Reactive Streams API (http://www.reactive-streams.org/)
 *
 *
 * Classes deriving from this API must be used with the 'reactive'
 * congestion strategy setting. They can then communicate back-pressure through
 * the reactiveCongestionStrategy below, and receive new elements through onNext.
 *
 * Implementation checklist:
 *
 * - Implementations should acquire a subscription in onStart by calling
 *   Publisher#subscribe(this) on a reactive Publisher.
 * - Implementations should override whenNext to configure custom behavior
 *   upon receiving a new data element. They should call store in that method.
 * - Implementations should override whenError to configure custom behavior
 *   upon failure. They should call on Stop in that method.
 * - Implementations should override whenComplete to configure custom behavior
 *   upon completion. They should call onStop in that method.
 *
 * @see [[receiver.congestionStrategy]]
 * @see [[org.reactivestreams.Subscriber]]
 */
@DeveloperApi
abstract class ReactiveReceiver[T](storageLevel: StorageLevel)
  extends Receiver[T](storageLevel) with Subscriber[T] { outer =>
    private var subscription: Subscription = null
    private var done: Boolean = false

    private sealed trait SpecificationViolation {
      val message: String
      def printStackTrace(t: Option[Throwable] = None): Unit = t match {
        case Some(e) => new IllegalStateException(message, e).printStackTrace(System.err)
        case None => new IllegalStateException(message).printStackTrace(System.err)
      }
    }

    private case class CancelException(s: Subscription) extends SpecificationViolation {
      val message = s"""$s violated the Reactive Streams rule 3.15
                       | by throwing an exception from cancel.""".stripMargin.replaceAll("\n", "")
    }

    private case class RequestException(s: Subscription) extends SpecificationViolation {
      val message = s"""$s violated the Reactive Streams rule 3.16
                       |by throwing an exception from request.""".stripMargin.replaceAll("\n", "")
    }

    private case class BeforeSubscribe() extends SpecificationViolation {
      val message = """Publisher violated the Reactive Streams rule 1.09 by signalling onNext,
                       | onComplete or onError prior to onSubscribe.
                       |""".stripMargin.replaceAll("\n", "")
    }

    private case class ErrorException(c: outer.type) extends SpecificationViolation {
      val message = s"""$c violates the Reactive Streams rule 2.13 by throwing an
                       | exception from onError.""".stripMargin.replaceAll("\n", "")
    }

    private def finish(): Unit = {
      done = true
      if (subscription != null) Try(subscription.cancel()) recover {
        case t: Throwable => CancelException(subscription).printStackTrace(Some(t))
      }
    }

    /**
     * Invoked by the Publisher after calling Publisher#subscribe(this).
     * No data will start flowing until subscription.request(Int) is invoked
     *p
     * @param s the subscription that allows requesting data via subscription.request(Int)
     */
    final override def onSubscribe(s: Subscription): Unit = {
      for { sub <- Option(s) }
      {
        if (subscription != null) {
          Try(subscription.cancel()) recover {
            case t: Throwable => CancelException(subscription).printStackTrace(Some(t))
          }
        } else {
          subscription = s
        }
      }
    }

    /**
     * Data notification sent by the Publisher, in reponse to requests to
     * subscription.request(Int) made by the reactiveCongestionStrategy.
     *
     * Performs spec compliance checks and defers to whenNext for override.
     *
     * @see whenNext
     * @param element The element signaled
     */
    final override def onNext(element: T): Unit = {
      if (!done){
        if (subscription == null) {
          BeforeSubscribe().printStackTrace()
        } else {
          Try(whenNext(element)) match {
            case Success(continue) => if (!continue) finish()
            case Failure(f) => Try(onError(f)) recover {
              case t: Throwable => ErrorException(this).printStackTrace(Some(t))
            }
          }
        }
      }
    }

    /**
     * Failed terminal state. No further events will be sent even if
     * subscription.request(Int) is called again
     *
     * Performs spec compliance checks and defers to whenError for override.
     *
     * @see whenError
     * @param error The throwable signaled
     */
    final override def onError(error: Throwable): Unit = {
      if (subscription == null) {
        BeforeSubscribe().printStackTrace()
      } else {
        done = true
        whenError(error)
      }
    }

    /**
     * Successful terminal state. No further events will be sent even if
     * subscription.request(Int) is called again
     *
     * Performs spec compliance checks and defers to whenComplete for override.
     *
     * @see whenComplete
     */
    final override def onComplete(): Unit = {
      if (subscription == null) {
        BeforeSubscribe().printStackTrace()
      } else {
        done = true
        whenComplete()
      }
    }

    /**
     * Data notification sent by the Publisher, in response to requests to
     * subscription.request() made by the reactiveCongestionStrategy.
     *
     * This is not intended to be called directly, onNext will be called instead.
     *
     * @see onNext
     * @param element The element signaled
     */
    protected def whenNext(element: T): Boolean = {
      store(element)
      true
    }

    /**
     * Successful terminal state. No further events will be sent even if
     * subscription.request() is called again
     *
     * This is not intended to be called directly, onComplete will be called instead.
     *
     * @see onComplete
     */
    protected def whenComplete(): Unit = {
      onStop()
    }

    /**
     * Failed terminal state. No further events will be sent even if
     * subscription.request() is called again
     *
     * This is not intended to be called directly, onError will be called instead.
     *
     * @see onError
     * @param error The throwable signaled
     */
    protected def whenError(error: Throwable): Unit = {
      onStop()
    }


    /**
     * A congestion Strategy which transmits Spark's back-pressure signal to the
     * Publisher, through the Reactive Streams API.
     *
     * @see [[receiver.CongestionStrategy]]
     */
    val reactiveCongestionStrategy = new CongestionStrategy {

      override def onBlockBoundUpdate(bound: Int) = if (subscription != null) {
        Try(subscription.request(bound)) recover {
          case t: Throwable => RequestException(subscription).printStackTrace(Some(t))
        }
      }

      override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                         nextBuffer: ArrayBuffer[Any]): Unit = {}
    }


}

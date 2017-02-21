/*
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.cripac.isee.vpe.util.kafka

import java.{lang => jl, util => ju}
import javax.annotation.{Nonnull, Nullable}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.annotation.Experimental
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.cripac.isee.vpe.ctrl.TaskData
import org.cripac.isee.vpe.util.SerializationHelper
import org.cripac.isee.vpe.util.logging.{ConsoleLogger, Logger}

import scala.language.postfixOps

/**
  * The class KafkaHelper provides static basic methods for manipulating Kafka affairs.
  * It is written in Scala because we cannot create KafkaCluster in some versions of Java and Scala.
  * This class is also an example of how to insert Scala codes in the Java project.
  *
  * @author Ken Yu
  */
object KafkaHelper {
  /**
    * Send a message to Kafka with provided producer. Debug info is output to given logger.
    *
    * @param topic     The Kafka topic to send to.
    * @param key       Key of the message.
    * @param value     Value of the message.
    * @param producer  The Kafka producer to use to send the message.
    * @param extLogger The logger to output debug info.
    * @tparam K Type of the key.
    * @tparam V Type of the value.
    */
  def sendWithLog[K, V](
                         @Nonnull topic: String,
                         @Nonnull key: K,
                         @Nonnull value: V,
                         @Nonnull producer: KafkaProducer[K, V],
                         @Nullable extLogger: Logger
                       ): Unit = {
    // Check if logger is provided. If not, create a console logger.
    val logger = if (extLogger == null) new ConsoleLogger() else extLogger
    // Send the message.
    logger debug ("Sending to Kafka <" + topic + ">\t" + key)
    val future = producer send new ProducerRecord[K, V](topic, key, value)
    // Retrieve sending report.
    try {
      val recMeta = future get;
      logger debug ("Sent to Kafka" + " <"
        + recMeta.topic + "-"
        + recMeta.partition + "-"
        + recMeta.offset + ">\t" + key)
    } catch {
      case e: InterruptedException =>
        logger error("Interrupted when retrieving Kafka sending result.", e)
    }
  }

  def sendWithLog[K](
                      @Nonnull key: K,
                      @Nonnull taskData: TaskData,
                      @Nonnull producer: KafkaProducer[K, Array[Byte]],
                      @Nullable extLogger: Logger
                    ): Unit = {
    sendWithLog(taskData.outputType.name(),
      key,
      SerializationHelper.serialize(taskData),
      producer,
      extLogger)
  }

  /**
    * :: Experimental ::
    * Scala constructor for a DStream where
    * each given Kafka topic/partition corresponds to an RDD partition.
    * The spark configuration spark.streaming.kafka.maxRatePerPartition gives the maximum number
    * of messages
    * per second that each '''partition''' will accept.
    *
    * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
    *                         see [[LocationStrategies]] for more details.
    * @param consumerStrategy In most cases, pass in ConsumerStrategies.subscribe,
    *                         see [[ConsumerStrategies]] for more details
    * @tparam K type of Kafka message key
    * @tparam V type of Kafka message value
    */
  @Experimental
  def createDirectStream[K, V](
                                ssc: StreamingContext,
                                locationStrategy: LocationStrategy,
                                consumerStrategy: ConsumerStrategy[K, V]
                              ): InputDStream[ConsumerRecord[K, V]] = {
    new DirectKafkaInputDStreamForVPE[K, V](ssc, locationStrategy, consumerStrategy)
  }

  /**
    * :: Experimental ::
    * Java constructor for a DStream where
    * each given Kafka topic/partition corresponds to an RDD partition.
    *
    * @param locationStrategy In most cases, pass in LocationStrategies.preferConsistent,
    *                         see [[LocationStrategies]] for more details.
    * @param consumerStrategy In most cases, pass in ConsumerStrategies.subscribe,
    *                         see [[ConsumerStrategies]] for more details
    * @tparam K type of Kafka message key
    * @tparam V type of Kafka message value
    */
  @Experimental
  def createDirectStream[K, V](
                                jssc: JavaStreamingContext,
                                locationStrategy: LocationStrategy,
                                consumerStrategy: ConsumerStrategy[K, V]
                              ): JavaInputDStream[ConsumerRecord[K, V]] = {
    new JavaInputDStream(
      createDirectStream[K, V](
        jssc.ssc, locationStrategy, consumerStrategy))
  }
}
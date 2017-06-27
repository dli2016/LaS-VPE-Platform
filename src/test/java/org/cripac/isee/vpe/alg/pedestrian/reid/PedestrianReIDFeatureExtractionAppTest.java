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

package org.cripac.isee.vpe.alg.pedestrian.reid;

import kafka.utils.ZkUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.alg.pedestrian.reid.Feature;
import org.cripac.isee.util.ResourceManager;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.debug.FakePedestrianTracker;
import org.cripac.isee.vpe.alg.pedestrian.tracking.TrackletOrURL;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.math.*;

import static org.cripac.isee.util.SerializationHelper.deserialize;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

public class PedestrianReIDFeatureExtractionAppTest {

    private static final Stream.Port TEST_PED_REID_FEATURE_RECV_PORT =
            new Stream.Port("test-pedestrian-reid-feature-recv", DataType.REID_FEATURE);

    private KafkaProducer<String, byte[]> producer;
    private KafkaConsumer<String, byte[]> consumer;
    private ConsoleLogger logger;
    private PedestrianReIDFeatureExtractionApp.AppPropertyCenter propCenter;

    public static void main(String[] args) {
        PedestrianReIDFeatureExtractionAppTest test = new PedestrianReIDFeatureExtractionAppTest();
        try {
            test.init(args);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        try {
            test.testReIDFeatureExtractionApp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void checkTopic(String topic) {
        Logger logger = new ConsoleLogger(Level.DEBUG);
        logger.info("Connecting to zookeeper: " + propCenter.zkConn);
        final ZkUtils zkUtils = KafkaHelper.createZkUtils(propCenter.zkConn,
                propCenter.zkSessionTimeoutMs,
                propCenter.zkConnectionTimeoutMS);
        logger.info("Checking topic: " + topic);
        KafkaHelper.createTopic(zkUtils,
                topic,
                propCenter.kafkaNumPartitions,
                propCenter.kafkaReplFactor);
    }

    private void init(String[] args)
            throws ParserConfigurationException, IOException, SAXException, URISyntaxException {
        PropertyConfigurator.configure(
            ResourceManager.getResource("/conf/log4j_local.properties").getPath());
        logger = new ConsoleLogger(Level.DEBUG);
        propCenter = new PedestrianReIDFeatureExtractionApp.AppPropertyCenter(args);
    }

    private void testReIDFeatureExtractionApp() throws Exception {
        logger.info("Testing reid feature extraction app.");

        checkTopic(TEST_PED_REID_FEATURE_RECV_PORT.inputType.name());

        try {
            Properties producerProp = propCenter.getKafkaProducerProp(false);
            producer = new KafkaProducer<>(producerProp);

            Properties consumerProp = 
                propCenter.getKafkaConsumerProp(UUID.randomUUID().toString(), false);
            consumer = new KafkaConsumer<>(consumerProp);
            consumer.subscribe(Collections.singletonList(
                TEST_PED_REID_FEATURE_RECV_PORT.inputType.name()));
        } catch (Exception e) {
            logger.error("When checking topics", e);
            logger.info("App test is disabled.");
        }

        TaskData.ExecutionPlan plan = new TaskData.ExecutionPlan();
        TaskData.ExecutionPlan.Node reidFeatureExtractNode = plan.addNode(
            PedestrianReIDFeatureExtractionApp.ReIDFeatureExtractionStream.OUTPUT_TYPE);
        TaskData.ExecutionPlan.Node reidFeatureSavingNode = plan.addNode(DataType.NONE);
        reidFeatureExtractNode.outputTo(
            reidFeatureSavingNode.createInputPort(TEST_PED_REID_FEATURE_RECV_PORT));

        // Send request (fake tracklet).
        //noinspection ConstantConditions
        Tracklet[] tracklets = new FakePedestrianTracker().track(null);
        for (int i = 0; i < tracklets.length; ++i) {
            TaskData trackletData = new TaskData(
                    reidFeatureExtractNode.createInputPort(
                    PedestrianReIDFeatureExtractionApp.ReIDFeatureExtractionStream.TRACKLET_PORT), 
                    plan,
                    new TrackletOrURL(tracklets[i]));
            assert trackletData.predecessorRes != null && trackletData.predecessorRes instanceof Tracklet;
            sendWithLog(UUID.randomUUID().toString(),
                    trackletData,
                    producer,
                    logger);
        }
        logger.info("Waiting for response...");
        // Receive result (attributes).
        ConsumerRecords<String, byte[]> records;
        // Feature list.
        List<Feature> feaList = new ArrayList<Feature>();
        //noinspection InfiniteLoopStatement
        while (true) {
            records = consumer.poll(0);
            if (records.isEmpty()) {
                continue;
            }

            logger.info("Response received!");
            records.forEach(rec -> {
                TaskData taskData;
                try {
                    taskData = deserialize(rec.value());
                } catch (Exception e) {
                    logger.error("During TaskData deserialization", e);
                    return;
                }
                if (taskData.destPorts.containsKey(TEST_PED_REID_FEATURE_RECV_PORT)) {
                    logger.info("<" + rec.topic() + ">\t" + rec.key());
                    Feature fea = (Feature)taskData.predecessorRes;
                    feaList.add(fea);
                }
            });

            consumer.commitSync();
            if (feaList.size() == tracklets.length) {
                break;
            }
        }

        // Dissimilarity
        float[] featureAFloat = feaList.get(0).getVector();
        float[] featureBFloat = feaList.get(1).getVector();
        float accumulate = 0.0f;
        for (int i = 0; i < featureAFloat.length; ++i) {
            float diff = featureAFloat[i] - featureBFloat[i];
            float sqrDiff = diff * diff;
            accumulate += sqrDiff;
        }
        logger.info("Dissimilarity: " + Math.sqrt(accumulate));
    }
}

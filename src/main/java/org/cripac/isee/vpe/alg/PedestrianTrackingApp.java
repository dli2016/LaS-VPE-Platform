/***********************************************************************
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
 ************************************************************************/

package org.cripac.isee.vpe.alg;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.cripac.isee.pedestrian.tracking.BasicTracker;
import org.cripac.isee.pedestrian.tracking.Tracker;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.*;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.ctrl.TopicManager;
import org.cripac.isee.vpe.data.WebCameraConnector;
import org.cripac.isee.vpe.debug.FakeWebCameraConnector;
import org.cripac.isee.vpe.util.Singleton;
import org.cripac.isee.vpe.util.hdfs.HDFSFactory;
import org.cripac.isee.vpe.util.kafka.KafkaProducerFactory;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.cripac.isee.vpe.util.SerializationHelper.deserialize;
import static org.cripac.isee.vpe.util.SerializationHelper.serialize;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * The PedestrianTrackingApp class takes in video URLs from Kafka, then process
 * the videos with pedestrian tracking algorithms, and finally push the tracking
 * results back to Kafka.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class PedestrianTrackingApp extends SparkStreamingApp {
    /**
     * The NAME of this application.
     */
    public static final String APP_NAME = "pedestrian-tracking";
    private int batchDuration = 1000;

    private Stream fragmentTrackingStream;
    private Stream rtTrackingStream;

    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception Any exception that might occur during execution.
     */
    public PedestrianTrackingApp(SystemPropertyCenter propCenter) throws Exception {
        batchDuration = propCenter.batchDuration;
        fragmentTrackingStream = new HDFSVideoTrackingStream(propCenter);
        rtTrackingStream = new RTVideoStreamTrackingStream(propCenter);
    }

    /**
     * @param args No options supported currently.
     * @throws Exception Any exception that might occur during execution.
     */
    public static void main(String[] args) throws Exception {
        // Load system properties.
        SystemPropertyCenter propCenter = new SystemPropertyCenter(args);

        // Start the pedestrian tracking application.
        SparkStreamingApp app = new PedestrianTrackingApp(propCenter);
        TopicManager.checkTopics(propCenter);
        app.initialize(propCenter);
        app.start();
        app.awaitTermination();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * SparkStreamingApp#getStreamContext()
     */
    @Override
    protected JavaStreamingContext getStreamContext() {
        // Create contexts.
        JavaStreamingContext jsc =
                new JavaStreamingContext(new SparkConf(true), Durations.milliseconds(batchDuration));

        fragmentTrackingStream.addToContext(jsc);
        rtTrackingStream.addToContext(jsc);

        return jsc;
    }

    /*
     * (non-Javadoc)
     *
     * @see SparkStreamingApp#getStreamInfo()
     */
    @Override
    public String getAppName() {
        return APP_NAME;
    }

    /**
     * The class ConfigPool wraps a broadcast of a pool of bytes of
     * tracking configuration files.
     */
    private static class ConfigPool {

        private static volatile Broadcast<Map<String, byte[]>> inst = null;

        /**
         * Get an instance of the pool broadcast.
         *
         * @param jsc    The JavaSparkContext the driver is running on.
         * @param hdfs   HDFS to read files from.
         * @param logger Logger.
         * @return An instance of the pool broadcast.
         * @throws IOException On failure of accessing HDFS for uploaded files.
         */
        public static Broadcast<Map<String, byte[]>> getInst(JavaSparkContext jsc,
                                                             FileSystem hdfs,
                                                             Logger logger) throws IOException {
            if (inst == null) {
                logger.debug("Creating instance of ConfigPool...");
                Map<String, byte[]> pool = new HashedMap();
                FileSystem stagingDir = FileSystem.get(new Configuration());
                RemoteIterator<LocatedFileStatus> files =
                        stagingDir.listFiles(new Path(System.getenv("SPARK_YARN_STAGING_DIR")), false);
                while (files.hasNext()) {
                    Path path = files.next().getPath();
                    if (path.getName().contains(".conf")) {
                        try {
                            logger.debug("Reading " + path.getName() + "...");
                            pool.put(path.getName(), IOUtils.toByteArray(hdfs.open(path)));
                            logger.debug("Added " + path.getName() + " to tracking configuration pool.");
                        } catch (IOException e) {
                            logger.error("Error when reading file " + path.getName(), e);
                        }
                    }
                }
                inst = jsc.broadcast(pool);
            }
            return inst;
        }
    }

    /**
     * The RTVideoStreamTrackingStream receives web-camera IPs
     * and perform pedestrian tracking on real-time video bit
     * stream from these cameras.
     */
    public static class RTVideoStreamTrackingStream extends Stream {

        public static final Info INFO =
                new Info("rt-video-tracking", DataTypes.TRACKLET);

        /**
         * Topic for inputting from Kafka the IPs of cameras.
         */
        public static final Topic LOGIN_PARAM_TOPIC =
                new Topic("cam-address-for-pedestrian-tracking",
                        DataTypes.WEBCAM_LOGIN_PARAM, INFO);

        /**
         * Kafka parameters for creating input streams pulling messages
         * from Kafka brokers.
         */
        private final Map<String, String> kafkaParams;

        private final Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private final Singleton<FileSystem> hdfsSingleton;
        private final Map<ServerID, Singleton<WebCameraConnector>> connectorPool;

        public RTVideoStreamTrackingStream(SystemPropertyCenter propCenter) throws
                Exception {
            super(new Singleton<>(new SynthesizedLoggerFactory(APP_NAME, propCenter)));

            kafkaParams = propCenter.generateKafkaParams(INFO.NAME);
            Properties producerProp = propCenter.generateKafkaProducerProp(false);

            producerSingleton = new Singleton<>(new KafkaProducerFactory<>(producerProp));
            hdfsSingleton = new Singleton<>(new HDFSFactory());
            connectorPool = new Object2ObjectOpenHashMap();
        }

        @Override
        public void addToContext(JavaStreamingContext jssc) {
            buildBytesDirectStream(jssc, Arrays.asList(LOGIN_PARAM_TOPIC.NAME), kafkaParams)
                    .foreachRDD(rdd -> rdd.foreachAsync(kvPair -> {
                                // Recover data.
                                final String taskID = kvPair._1();
                                TaskData taskData;
                                try {
                                    taskData = (TaskData) deserialize(kvPair._2());
                                } catch (Exception e) {
                                    loggerSingleton.getInst().error("During TaskData deserialization", e);
                                    return;
                                }

                                // Get camera WEBCAM_LOGIN_PARAM.
                                if (taskData.predecessorRes == null) {
                                    loggerSingleton.getInst().error(
                                            "No camera WEBCAM_LOGIN_PARAM specified for real-time tracking stream!");
                                    return;
                                }
                                if (!(taskData.predecessorRes instanceof String)) {
                                    loggerSingleton.getInst().error(
                                            "Real-time tracking stream expects camera WEBCAM_LOGIN_PARAM but received "
                                                    + taskData.predecessorRes.getClass().getName() + "!");
                                    return;
                                }
                                LoginParam loginParam = (LoginParam) taskData.predecessorRes;

                                WebCameraConnector cameraConnector;
                                if (connectorPool.containsKey(loginParam.serverID)) {
                                    cameraConnector = connectorPool.get(loginParam.serverID).getInst();
                                } else {
                                    Singleton<WebCameraConnector> cameraConnectorSingleton =
                                            new Singleton(new FakeWebCameraConnector
                                                    .FakeWebCameraConnectorFactory(loginParam));
                                    connectorPool.put(loginParam.serverID, cameraConnectorSingleton);
                                    cameraConnector = cameraConnectorSingleton.getInst();
                                }

                                // Connect to camera.
                                InputStream rtVideoStream = cameraConnector.getStream();
                                // TODO(Ken Yu): Perform tracking on the real-time video stream.
                            })
                    );
        }
    }

    public static class HDFSVideoTrackingStream extends Stream {

        public static final Info INFO = new Info("hdfs-video-tracking", DataTypes.TRACKLET);

        /**
         * Topic to input video URLs from Kafka.
         */
        public static final Topic VIDEO_URL_TOPIC =
                new Topic("hdfs-video-url-for-pedestrian-tracking", DataTypes.URL, INFO);
        /**
         * Kafka parameters for creating input streams pulling messages
         * from Kafka brokers.
         */
        private final Map<String, String> kafkaParams;

        private final Singleton<KafkaProducer<String, byte[]>> producerSingleton;
        private final Singleton<FileSystem> hdfsSingleton;

        public HDFSVideoTrackingStream(SystemPropertyCenter propCenter) throws
                Exception {
            super(new Singleton<>(new SynthesizedLoggerFactory(APP_NAME, propCenter)));

            kafkaParams = propCenter.generateKafkaParams(INFO.NAME);

            Properties producerProp = propCenter.generateKafkaProducerProp(false);
            producerSingleton = new Singleton<>(new KafkaProducerFactory<>(producerProp));

            hdfsSingleton = new Singleton<>(new HDFSFactory());
        }

        public static class VideoFragment implements Serializable {
            public String videoID;
            public byte[] bytes;
        }

        @Override
        public void addToContext(JavaStreamingContext jssc) {
            buildBytesDirectStream(jssc, Arrays.asList(VIDEO_URL_TOPIC.NAME), kafkaParams).foreachRDD(rdd -> {
                final Broadcast<Map<String, byte[]>> confPool =
                        ConfigPool.getInst(new JavaSparkContext(rdd.context()),
                                hdfsSingleton.getInst(),
                                loggerSingleton.getInst());

                rdd.foreachAsync(kvPair -> {
                    final Logger logger = loggerSingleton.getInst();
                    try {
                        final String taskID = kvPair._1();
                        final TaskData taskData = (TaskData) deserialize(kvPair._2());

                        final String videoURL = (String) taskData.predecessorRes;
                        final InputStream videoStream = hdfsSingleton.getInst().open(new Path(videoURL));
                        logger.debug("Received taskID=" + taskID + ", URL=" + videoURL);

                        final TaskData.ExecutionPlan.Node curNode = taskData.curNode;
                        // Get tracking configuration for this execution.
                        final String confFile = (String) curNode.getExecData();
                        if (confFile == null) {
                            logger.error("Tracking configuration file is not specified for this node!");
                            return;
                        }

                        // Get the IDs of successor nodes.
                        final List<Topic> succTopics = curNode.getSuccessors();
                        // Mark the current node as executed in advance.
                        taskData.curNode.markExecuted();

                        // Load tracking configuration to create a tracker.
                        if (!confPool.getValue().containsKey(confFile)) {
                            throw new FileNotFoundException("Couldn't find tracking config file " + confFile);
                        }
                        final byte[] confBytes = confPool.getValue().get(confFile);
                        if (confBytes == null) {
                            logger.fatal("confPool contains key " + confFile + " but value is null!");
                            return;
                        }
                        final Tracker tracker = new BasicTracker(confBytes, logger);
                        //Tracker tracker = new FakePedestrianTracker();

                        // Conduct tracking on video read from HDFS.
                        logger.debug("Performing tracking on " + videoURL);
                        final Tracklet[] tracklets = tracker.track(videoStream);
                        logger.debug("Finished tracking on " + videoURL);

                        // Send tracklets.
                        final KafkaProducer producer = producerSingleton.getInst();
                        for (Tracklet tracklet : tracklets) {
                            // Complete identifier of each tracklet.
                            tracklet.id.videoID = videoURL;
                            // Stored the track in the task data, which can be cyclic utilized.
                            taskData.predecessorRes = tracklet;
                            // Send to all the successor nodes.
                            for (Topic topic : succTopics) {
                                try {
                                    taskData.changeCurNode(topic);
                                } catch (RecordNotFoundException e) {
                                    logger.warn("When changing node in TaskData", e);
                                }

                                final byte[] serialized = serialize(taskData);
                                logger.debug("To sendWithLog message with size: " + serialized.length);
                                sendWithLog(topic, taskID, serialized, producer, logger);
                            }
                        }
                    } catch (Exception e) {
                        logger.error("During tracking.", e);
                    }
                });
            });
        }
    }
}

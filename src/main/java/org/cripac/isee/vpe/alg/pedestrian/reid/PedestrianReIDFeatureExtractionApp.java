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

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.reid.Feature;
import org.cripac.isee.alg.pedestrian.reid.PedestrianInfo;
import org.cripac.isee.alg.pedestrian.reid.ReIDFeatureExtracter;
import org.cripac.isee.alg.pedestrian.reid.MSCANFeatureExtracter;
import org.cripac.isee.vpe.alg.pedestrian.tracking.TrackletOrURL;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.RobustExecutor;
import org.cripac.isee.vpe.common.SparkStreamingApp;
import org.cripac.isee.vpe.common.Stream;
import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
import org.cripac.isee.vpe.ctrl.TaskData;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.util.Singleton;
import scala.Tuple2;

import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * The PedestrianReIDFeatureExtractionApp class is a Spark Streaming application
 * which performs reid feature extraction.
 *
 * @author da.li, CRIPAC, 2017
 */
public class PedestrianReIDFeatureExtractionApp extends SparkStreamingApp {
    /**
     * The name of this application.
     */
    public static final String APP_NAME = "pedestrian-reid-feature-extraction";
    private static final long serialVersionUID = 7561012713161590005L;

    @Override
    public void addToContext() throws Exception {
        // Do nothing.
    }

    /**
     * Available algorithms of pedestrian reid feature extraction.
     */
    public enum ReIDAlgorithm {
        MSCAN,
    }

    /**
     * Constructor of the application, configuring properties read from a
     * property center.
     *
     * @param propCenter A class saving all the properties this application may need.
     * @throws Exception On failure in Spark.
     */
    public PedestrianReIDFeatureExtractionApp(AppPropertyCenter propCenter) throws Exception {
        super(propCenter, APP_NAME);

        registerStreams(Collections.singletonList(new ReIDFeatureExtractionStream(propCenter)));
    }

    public static class AppPropertyCenter extends SystemPropertyCenter {
        private static final long serialVersionUID = 7561912313161500905L;
        public ReIDAlgorithm algorithm = ReIDAlgorithm.MSCAN;

        public AppPropertyCenter(@Nonnull String[] args) 
            throws URISyntaxException, ParserConfigurationException, SAXException, UnknownHostException {
            super(args);
            // Digest the settings.
            for (Map.Entry<Object, Object> entry : sysProps.entrySet()) {
                switch ((String) entry.getKey()) {
                    case "vpe.ped.reid.alg":
                        algorithm = ReIDAlgorithm.valueOf((String) entry.getValue());
                        break;
                    default:
                        logger.warn("Unrecognized option: " + entry.getKey());
                        break;
                }
            }
        }
    } 

    /**
     * @param args No options supported currently.
     * @throws Exception On failure in Spark.
     */
    public static void main(String[] args) throws Exception {
        // Load system properties.
        AppPropertyCenter propCenter = new AppPropertyCenter(args);

        // Start the pedestrian tracking application.
        SparkStreamingApp app = new PedestrianReIDFeatureExtractionApp(propCenter);
        app.initialize();
        app.start();
        app.awaitTermination();
    }

    public static class ReIDFeatureExtractionStream extends Stream {

        public static final String NAME = "PedestrianReIDFeatureExtraction";
        public static final DataType OUTPUT_TYPE = DataType.REID_FEATURE;

        /**
         * Port to input pedestrian tracklets from Kafka.
         */
        public static final Port TRACKLET_PORT =
                new Port("pedestrian-tracklet-for-reid-feature-extraction",
                        DataType.TRACKLET);
        
        private static final long serialVersionUID = 3988152284961510251L;

        private Singleton<ReIDFeatureExtracter> reidSingleton;

        public ReIDFeatureExtractionStream(AppPropertyCenter propCenter) throws Exception {
            super(APP_NAME, propCenter);

            switch (propCenter.algorithm) {
                case MSCAN:
                    reidSingleton = new Singleton<> (
                        () -> new MSCANFeatureExtracter(propCenter.caffeGPU, loggerSingleton.getInst()),
                        MSCANFeatureExtracter.class
                    );
            }
        }

        @Override
        public void addToGlobalStream(Map<DataType, JavaPairDStream<UUID, TaskData>> globalStreamMap) {
            
            // Extract tracklets from the data.
            final JavaPairDStream<UUID, TaskData> trackletDStream = filter(globalStreamMap, TRACKLET_PORT);
            // Extract features from the tracklets.
            trackletDStream.foreachRDD(rdd -> rdd.glom().foreach(kvList -> {
                Logger logger = loggerSingleton.getInst();
                long startTime = System.currentTimeMillis();
                final long[] reidCostTime = {0};
                final int[] numSamples = {0};
                kvList.forEach(kv -> {
                    try {
                        final UUID taskID = kv._1();
                        final TaskData taskData = kv._2();
                        logger.debug("To extract reid feature for task " + taskID + "!");
                        // Etract features robustly;
                        final Feature feature = new RobustExecutor<>((Function<TrackletOrURL, Feature>) tou -> {
                            final PedestrianInfo pedestrian = new PedestrianInfo(tou);
                            long reidStartTime = System.currentTimeMillis();
                            final Feature fea = reidSingleton.getInst().extract(pedestrian);
                            long reidEndTime = System.currentTimeMillis();
                            reidCostTime[0] += reidEndTime - reidStartTime;
                            numSamples[0] += tou.getTracklet().getSamples().size();
                            return fea;
                        }).execute((TrackletOrURL) taskData.predecessorRes);
                        logger.debug("Feature retrieved for task " + taskID + "!");
                        // Find current node.
                        final TaskData.ExecutionPlan.Node curNode = taskData.getDestNode(TRACKLET_PORT);
                        // Get ports to output to.
                        assert curNode != null;
                        final List<TaskData.ExecutionPlan.Node.Port> outputPorts = curNode.getOutputPorts();
                        // Mark the current node as executed (Add userPlan by da.li).
                        curNode.markExecuted();
                        output(outputPorts, taskData.executionPlan, feature, taskData.userPlan, taskID);
                    } catch (Exception e) { 
                        logger.error("During reid feature extraction.", e);
                    }
                });
                if (kvList.size() > 0) {
                    long endTime = System.currentTimeMillis();
                    logger.info("Overall speed=" + ((endTime - startTime) / kvList.size())
                                + "ms per tracklet (totally " + kvList.size() + " tracklets)");
                }
                if (numSamples[0] > 0) {
                    logger.info("Reid speed=" + (reidCostTime[0] / numSamples[0])
                                + "ms per sample (totally " + numSamples[0] + " samples)");
                }
            }));
        }

        @Override
        public List<Port> getPorts() {
            return Collections.singletonList(TRACKLET_PORT);
        }
    }
}

/**
 * To test the function getTrackletInfo in HadoopHelper
 * 
 * @Author da.li on 2017-07-21
 */

package org.cripac.isee.vpe.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Level;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;

import java.util.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.URI;
import java.net.URISyntaxException;

import org.spark_project.guava.collect.ContiguousSet;
import org.spark_project.guava.collect.DiscreteDomain;
import org.spark_project.guava.collect.Range;

import org.cripac.isee.vpe.alg.pedestrian.tracking.TrackletOrURL;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.attr.Recognizer;
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffe2Native;
import org.cripac.isee.alg.pedestrian.reid.*;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.data.Neo4jConnector;
import org.cripac.isee.vpe.data.GraphDatabaseConnector;

//import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.getDefaultConf;
//import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.storeTracklet;
//import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.retrieveTracklet;
//import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.storeTrackletNew;
import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.retrieveTrackletNew;
import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.getTrackletInfo;
//
//import static org.bytedeco.javacpp.opencv_core.CV_8UC3;
//import static org.bytedeco.javacpp.opencv_imgcodecs.imdecode;
//import static org.bytedeco.javacpp.opencv_imgcodecs.imencode;


/**
 * Test the functions in class Neo4jConnector.
 *
 * @Author da.li on 2017/07/21
 */

public class Neo4jTest {

    private ConsoleLogger logger;
    private FileSystem hdfs;
    private GraphDatabaseConnector dbConnector;

    /* Initialize */
    private void init() throws Exception {
        // Logger
        logger = new ConsoleLogger(Level.DEBUG);
        // Hdfs
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",
             org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        hdfs = FileSystem.get(new URI("hdfs://rtask-nod8:8020"), conf);
        // Neo4j
        dbConnector = new Neo4jConnector();
    }

    // Insert.
    // Tracklet ...
    private void insertTracklet(String trackletPath, String dataType) throws Exception {
        // Get tracklet info.
        String trackletInfoPath = trackletPath + "/info.txt";
        final InputStreamReader infoReader = 
            new InputStreamReader(hdfs.open(new Path(trackletInfoPath)));
        BufferedReader bufferedReader = new BufferedReader(infoReader);
        String trackletInfo = bufferedReader.readLine();

        // Load Tracklet to get tracklet id.
        Tracklet tracklet = retrieveTrackletNew(trackletPath, hdfs);
        
        // Insert
        long startTime = System.currentTimeMillis();
        dbConnector.setPedestrianTracklet(
            tracklet.id.toString(),  /*nodeID*/
            dataType,                /*Lable for current algorithm/userplan.*/
            trackletPath,            /*Dir to save the file of tracklet.data.*/
            trackletInfo             /*Information of a tracklet.*/
        );
        long endTime = System.currentTimeMillis();
        logger.info("Elapsed time of insert a tracklet as a node in Neo4j: " +
                    (endTime - startTime) + "ms");
    }
    // Attributes ...
    private Attributes insertAttributes(String trackletPath, String dataType) throws Exception {
        // Get tracklet info.
        String trackletInfoPath = trackletPath + "/info.txt";
        final InputStreamReader infoReader = 
            new InputStreamReader(hdfs.open(new Path(trackletInfoPath)));
        BufferedReader bufferedReader = new BufferedReader(infoReader);
        String trackletInfo = bufferedReader.readLine();

        // Load Tracklet.
        Tracklet tracklet = retrieveTrackletNew(trackletPath, hdfs);
        // Attributes recognition.
        String gpu = "-1";
        File netModel = new File("models/DeepMARCaffe2/init_net.pb");
        File modelParams = new File("models/DeepMARCaffe2/predict_net.pb");
        Recognizer recognizer = new DeepMARCaffe2Native (
            gpu,
            netModel,
            modelParams,
            logger
        );

        Attributes attr = recognizer.recognize(tracklet);
        long startTime = System.currentTimeMillis();
        dbConnector.setPedestrianAttributes(tracklet.id.toString(), 
                                            dataType, attr);
        long endTime = System.currentTimeMillis();
        logger.info("Elapsed time of insert attributes to a node in Neo4j: " +
                    (endTime - startTime) + "ms");
        return attr;
    }

    // Reid Feature.
    private Feature insertReidFeature(String trackletPath, String dataType) throws Exception {
        // Get tracklet info.
        String trackletInfoPath = trackletPath + "/info.txt";
        final InputStreamReader infoReader =
            new InputStreamReader(hdfs.open(new Path(trackletInfoPath)));
        BufferedReader bufferedReader = new BufferedReader(infoReader);
        String trackletInfo = bufferedReader.readLine();

        // Load Tracklet.
        Tracklet tracklet = retrieveTrackletNew(trackletPath, hdfs);

        // Reid feature Extraction.
        String gpu = "-1";
        File protoFile = new File("models/MSCANCaffe/deploy.prototxt");
        File caffeModel= new File("models/MSCANCaffe/mscan.caffemodel");
        MSCANFeatureExtracter extracter = new MSCANFeatureExtracter(
            gpu, protoFile, caffeModel, logger
        );
        TrackletOrURL trackletOrURL = new TrackletOrURL(tracklet);
        PedestrianInfo pedestrian = new PedestrianInfo(trackletOrURL);
        Feature fea = extracter.extract(pedestrian);

        // Insert
        long startTime = System.currentTimeMillis();
        dbConnector.setPedestrianReIDFeature(tracklet.id.toString(),
                                             dataType, fea);
        long endTime = System.currentTimeMillis();
        logger.info("Elapsed time of insert a reidfeature to neo4j: " +
                    (endTime - startTime) + "ms");
        return fea;
    }

    // Query.
    // Path of tracklet path.
    private String getTrackletSavingPath(String trackletID, 
                                         String dataType) throws Exception {
        long startTime = System.currentTimeMillis();
        String path = 
            dbConnector.getTrackletSavingDir(trackletID, dataType);
        long endTime = System.currentTimeMillis();
        logger.info("Elapsed time of search a tracklet in neo4j: " +
                    (endTime - startTime) + "ms");
        return path;
    }
    // Attributes.
    private Attributes getAttributes(String trackletID,
                                     String dataType) throws Exception {
        long startTime = System.currentTimeMillis();
        Attributes attr =
           dbConnector.getPedestrianAttributes(trackletID, dataType);
        long endTime = System.currentTimeMillis();
        logger.info("Elapsed time of search the attributes in neo4j: " +
                    (endTime - startTime) + "ms");
        return attr;
    }
    // reid feature
    private Feature getReidFeature(String trackletID,
                                   String dataType) throws Exception {
        long startTime = System.currentTimeMillis();
        Feature fea = 
            dbConnector.getPedestrianReIDFeature(trackletID, dataType);
        long endTime = System.currentTimeMillis();
        logger.info("Elapsed time of search the reidfeature in neo4j: " +
                    (endTime - startTime) + "ms");
        return fea;
    }
    
    // Is the same feature.
    private boolean sameReidFeature(Feature feaLeft, Feature feaRight) {
        float[] feaLeftFloat = feaLeft.getVector();
        float[] feaRightFloat= feaRight.getVector();

        int feaLeftLength = feaLeftFloat.length;
        int feaRightLength= feaRightFloat.length;
        if (feaLeftLength != 128 || feaRightLength != 128) {
            logger.fatal("Bad feature length, have a check about the feature parsing.");
            return false;
        }
        if (feaLeftLength != feaRightLength) {
            logger.fatal("Unequal feature length!");
            return false;
        }
        for (int i = 0; i < feaLeftLength; ++i) {
            if (Math.abs(feaLeftFloat[i] - feaRightFloat[i]) >= 0.000001) {
                logger.fatal("Round " + i + ": bad feature");
                return false;
            }
        }
        return true;
    }

    /* Main function*/
    public static void main(String[] args) throws Exception {
     
        // It is necessary to change the directory as your practical situation.
        String trackletDataDir = "/user/vpe.cripac/test/da.li/database/tracklets/35";
        String dataType = "dli_test_20170802";
        Neo4jTest testNeo4j = new Neo4jTest();
        // Initialize.
        testNeo4j.init();
        // Test insert tracklet node.
        testNeo4j.insertTracklet(trackletDataDir, dataType);
        // Test got tracklet saving path.
        String gotPath = 
            testNeo4j.getTrackletSavingPath(
                "CAM01-20131223151500-20131223152048_tarid35", dataType
            );
	System.out.println("The path got from neo4j is: " + gotPath);
        /*
        // Test insert attributes.
        Attributes attrIn = testNeo4j.insertAttributes(trackletDataDir, dataType);
        // Test get attributes from neo4j.
        Attributes attrOut = testNeo4j.getAttributes(
            "CAM01-20131223151500-20131223152048_tarid35", dataType
        );
        boolean equal = attrIn.equals(attrOut);
        if (equal) {
            System.out.println("The input attributes equals to the one got from neo4j!");
        } else {
            System.out.println("Bad attributes insert and search, have a check!");
        }
        */
        // Test insert reid feature.
        Feature feaIn = testNeo4j.insertReidFeature(trackletDataDir, dataType);
        // Test get reid feature from neo4j.
        Feature feaOut = testNeo4j.getReidFeature(
            "CAM01-20131223151500-20131223152048_tarid35", dataType
        );
        boolean same = testNeo4j.sameReidFeature(feaIn, feaOut);
        if (same) {
            System.out.println("The input reid feature equals to the one got from neo4j!");
        } else {
            System.out.println("Bad reid feature insert and search, have a check!");
        }
    }
}

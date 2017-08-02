/**
 * To test the functions: storeTrackletNew and retrieveTrackletNew.
 *
 * @Author da.li on 2017-07-19
 */

package org.cripac.isee.vpe.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Level;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;

import java.net.URI;
import java.util.*;
import java.io.IOException;
import java.net.URISyntaxException;

import org.spark_project.guava.collect.ContiguousSet;
import org.spark_project.guava.collect.DiscreteDomain;
import org.spark_project.guava.collect.Range;

import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;

import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.getDefaultConf;
//import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.storeTracklet;
import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.retrieveTracklet;
import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.storeTrackletNew;
import static org.cripac.isee.vpe.util.hdfs.HadoopHelper.retrieveTrackletNew;

import static org.bytedeco.javacpp.opencv_core.CV_8UC3;
import static org.bytedeco.javacpp.opencv_imgcodecs.imdecode;
import static org.bytedeco.javacpp.opencv_imgcodecs.imencode;

public class SaveTrackletToOneFile {

    private ConsoleLogger logger;
    private FileSystem hdfs;

    // Main.
    public static void main(String[] args) throws Exception {
        SaveTrackletToOneFile app = new SaveTrackletToOneFile();
        app.init();
        // Stat to test.
        String storeDir = "/user/vpe.cripac/test/da.li/tracklets/35";
        String outputDir= "/user/vpe.cripac/test/da.li/tracklets";
        app.testS(storeDir, outputDir); // Save to a file.
        app.testL(outputDir); // Load tracklet from a "big file".
    }

    // Initialization.
    private void init() throws Exception {
        logger = new ConsoleLogger(Level.DEBUG);
        // Hadoop.
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",
             org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        hdfs = FileSystem.get(new URI("hdfs://rtask-nod8:8020"), conf);
    }

    // Test
    private void testS(String storeDir, String outputDir) throws Exception {
        Tracklet tracklet = null;
        try {
            tracklet = retrieveTracklet(storeDir, hdfs);
        } catch (IOException e1) {
        } catch (URISyntaxException e2) {
        }
        logger.info("Tracklet loaded SUCCESSFULLY!");
        logger.info("Length of the tracklet is: " + tracklet.locationSequence.length);
        String info = storeTrackletNew(outputDir, tracklet, hdfs);
        //logger.info("The file saved SUCCESSFULLY!");
        //saveTracklet(outputDir, tracklet);
        //storeTracklet(outputDir, tracklet, hdfs);
    }

    private void testL(String dir) throws Exception {
        Tracklet tracklet = null;
        
        try {
            tracklet = retrieveTrackletNew(dir, hdfs);
        } catch (IOException e1) {
        } catch (URISyntaxException e2) {
        }

        logger.info("Tracklet loaded SUCCESSFULLY!");
        logger.info("Length of the tracklet is: " + tracklet.locationSequence.length);
        // Save data.
        saveTracklet(dir, tracklet);
        logger.info("Get tracklet data from the big file SUCCESSFULLY!");
    }

    // Save tracklet.
    private void saveTracklet(String dir, Tracklet tracklet) {
        ContiguousSet.create(Range.closedOpen(0, tracklet.locationSequence.length), DiscreteDomain.integers())
        .parallelStream()
        .filter(idx -> tracklet.locationSequence[idx].patchData != null)
        .forEach(idx -> {
            final Tracklet.BoundingBox bbox = tracklet.locationSequence[idx];
            final BytePointer inputPointer = new BytePointer(bbox.patchData);
            final opencv_core.Mat image = new opencv_core.Mat(bbox.height, bbox.width, CV_8UC3, inputPointer);
            final BytePointer outputPointer = new BytePointer();
            imencode(".jpg", image, outputPointer);
            final byte[] bytes = new byte[(int) outputPointer.limit()];
            outputPointer.get(bytes);
            final FSDataOutputStream imgOutputStream;
            try {
                imgOutputStream = 
                    hdfs.create(new Path(dir + "/" + idx + ".jpg"));
                imgOutputStream.write(bytes);
                imgOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Free resources.
            image.release();
            inputPointer.deallocate();
            outputPointer.deallocate();
        });
    }
}


/**
 * SSDTrakcerTest.java
 * Test the code to do track using ssd for objects detection.
 *
 * Version 0.1.0
 * Date    2017/05/29
 */

package org.cripac.isee.alg.pedestrian.tracking;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Level;

import org.cripac.isee.vpe.alg.pedestrian.tracking.PedestrianTrackingApp;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
//import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Create by da.li on 2017/05/29 (original file by ken.yu on 16-10-13)
 */
public class SSDTrackerTest {
    
    // @Test
    // Note: If you want to test the code when implement mvn package, please make
    // sure the libcaffe.so.1.0.0-rc3 is compiled without cudnn. And other dependencies
    // - libjnissd-pedstrian-tracker.so, libssd-pedestrian-tracker.so and libobject-
    // detector-ssd.so do not depend cudnn as well.
    public void track() throws Exception {
        // Parameters
        PedestrianTrackingApp.AppPropertyCenter propCenter;
        propCenter = new PedestrianTrackingApp.AppPropertyCenter(new String[] {
          "-a", PedestrianTrackingApp.APP_NAME,
          "--system-property-file", "conf/system.properties",
          "--app-property-file", "conf/" + PedestrianTrackingApp.APP_NAME + "/app.properties",
          "-v"});

        String trackerMethod = "SSD-TRACKER: ";
        System.out.println(trackerMethod + "Performing validness test...");

        System.out.println(trackerMethod + "Reading video...");
        InputStream videoStream = new FileInputStream("src/test/resources/20131220184349-20131220184937.h264");
        
        System.out.println(trackerMethod + "Native library path: " + System.getProperty("java.library.path"));
        System.out.println(trackerMethod + "Creating tracker...");

        String gpuIndex = propCenter.caffeGPU;
        System.out.println(trackerMethod + "" + gpuIndex);
    
        float confidenceThreshold = 0.5f;
       
        byte[] conf = IOUtils.toByteArray(new FileInputStream(
            "conf/" + PedestrianTrackingApp.APP_NAME + "/isee-basic/CAM01_0.conf"));

        SSDTracker tracker = new SSDTracker(
            conf,
            gpuIndex,
            confidenceThreshold,
            new File("models/SSDCaffe/deploy.prototxt"),
            new File("models/SSDCaffe/deploy.caffemodel"),
            new ConsoleLogger(Level.DEBUG));

        System.out.println(trackerMethod + "Start tracking...");
        Tracklet[] tracklets = tracker.track(videoStream);

        System.out.println(trackerMethod + "Tracked " + tracklets.length + " pedestrians!");
    }
}

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

package org.cripac.isee.alg.pedestrian.tracking;

import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.bytedeco.javacv.OpenCVFrameConverter;
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffe2;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.CharacterCodingException;
import java.nio.file.AccessDeniedException;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.bytedeco.javacpp.avutil.AV_LOG_QUIET;
import static org.bytedeco.javacpp.avutil.av_log_set_level;

/**
 * The SSDTracker class is a JNI class of a pedestrian tracking algorithm used
 * within the Center for Research on Intelligent Perception and Computing(CRIPAC),
 * Institute of Automation, Chinese Academy of Science.
 *
 * @author Ken Yu, CRIPAC, 2016
 *         Modified by da.li on 2017/05/26 for ssd based Trakcer.
 */
public class SSDTracker implements Tracker {

    static {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(DeepMARCaffe2.class);
        try {
            logger.info("Loading native libraries for SSDTracker from "
                    + System.getProperty("java.library.path"));
            System.loadLibrary("jnissd_pedestrian_tracker");
            logger.info("Native libraries for SSDTracker successfully loaded!");
        } catch (Throwable t) {
            logger.error("Failed to load native library for BasicTracker", t);
            throw t;
        }
    }

    private SSDTrackerParams params = new SSDTrackerParams();
    private Logger logger;

    /*Select a gpu randomly.*/
    public static int selectGPURandomly(String gpus) {
        //Random random = new Random(System.currentTimeMillis());
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        String[] gpuIDs = gpus.split(",");
        //return Integer.parseInt(gpuIDs[random.nextInt(gpuIDs.length)]);
        return Integer.parseInt(gpuIDs[tlr.nextInt(0, gpuIDs.length)]);
    }

    /* Tracker parameters. */
    public static class SSDTrackerParams {

        public int width = 1280;    // Width of the input frame.
        public int height= 720;     // Height of the input frame.
        public int numChannels = 3; // Number of channels of the input frame.
        public int gpuIndex = -1;   // Selected gpu; -1 for cpu only.
        public byte[] conf = null;  // Bytes of a configuration the tracker uses.
        public float  confidenceThreshold = 0.5f;  // The threshold that specifies a sample to positive.
        public String pbPath;       // path for the file of a net architecture.
        public String modelPath;    // path for the weights of caffe model.

    }

    public SSDTracker(@Nonnull byte[] conf,
                      @Nonnull String gpuIndex,
                      @Nonnull float confidenceThreshold,
                      @Nonnull File pb,
                      @Nonnull File model) 
        throws FileNotFoundException, AccessDeniedException, CharacterCodingException {

        this(conf, gpuIndex, confidenceThreshold, pb, model, null);
    }

    /**
     * Construct a tracker with a configuration. The configuration should be
     * provided in a form of byte array.
     *
     * @param conf The byte data of the configuration file.
     */
    public SSDTracker(@Nonnull byte[] conf,
                      @Nonnull String gpuIndex,
                      @Nonnull float confidenceThreshold,
                      @Nonnull File pb,
                      @Nonnull File model,
                      @Nullable Logger logger) 
        throws FileNotFoundException, AccessDeniedException, CharacterCodingException {
        
        if (logger == null) {
            this.logger = new ConsoleLogger();
        } else {
            this.logger = logger;
        }
        
        params.conf = conf;
        params.gpuIndex = selectGPURandomly(gpuIndex);
        params.confidenceThreshold = confidenceThreshold;
        if (!pb.exists()) {
            throw new FileNotFoundException("Cannot find " + pb.getPath());
        }
        if (!model.exists()) {
            throw new FileNotFoundException("Cannot find " + model.getPath());
        }
        if (!pb.canRead()) {
            throw new AccessDeniedException("Cannot read " + pb.getPath());
        }
        if (!model.canRead()) {
            throw new AccessDeniedException("Cannot read " + model.getPath());
        }

        params.pbPath = pb.getPath();
        params.modelPath = model.getPath();
    }

    /*
     * (non-Javadoc)
     *
     * @see Tracker#track(java.lang.String)
     */
    @Nonnull
    @Override
    public Tracklet[] track(@Nonnull InputStream videoStream) throws FrameGrabber.Exception {
        FFmpegFrameGrabber frameGrabber = new FFmpegFrameGrabber(videoStream);
        av_log_set_level(AV_LOG_QUIET);
        frameGrabber.start();
        logger.debug("Initialized video decoder!");

        params.width = frameGrabber.getImageWidth();
        params.height= frameGrabber.getImageHeight();
        params.numChannels = 3;

        long trackerPointer = initialize(params);
        logger.debug("Initialized tracker!");
        logger.debug("In track, I select gpu: " + params.gpuIndex);
        int cnt = 0;
        // Every time a frame is retrieved during decoding, it is immediately fed into the tracker,
        // so as to save runtime memory.
        while (true) {
            Frame frame;
            try {
                frame = frameGrabber.grabImage();
            } catch (FrameGrabber.Exception e) {
                logger.error("On grabImage: " + e);
                break;
            }
            if (frame == null) {
                break;
            }
            final byte[] buf = new byte[frame.imageHeight * frame.imageWidth * frame.imageChannels];
            final opencv_core.Mat cvFrame = new OpenCVFrameConverter.ToMat().convert(frame);
            cvFrame.data().get(buf);
            int ret = feedFrame(trackerPointer, buf);
            if (ret != 0) {
                break;
            }
            ++cnt;
            if (cnt % 1000 == 0) {
                logger.debug("Tracked " + cnt + " frames!");
            }
        }

        logger.debug("Totally processed " + cnt + " framed!");
        logger.debug("Getting targets...");
        Tracklet[] targets = getTargets(trackerPointer);
        logger.debug("Got " + targets.length + " targets!");
        free(trackerPointer);

        for (int i = 0; i < targets.length; ++i) {
            targets[i].numTracklets = targets.length;
            targets[i].id.serialNumber = i;
        }

        return targets;
    }

    /**
     * Initialize a native tracker.
     *
     * @param params  parameters used by tracking with ssd for object detection.
     * @return The pointer of the initialized tracker.
     */
    private native long initialize(SSDTrackerParams params);

    /**
     * Feed a frame into the tracker. The tracker is expected to process the video frame by frame.
     *
     * @param p     The pointer of an initialized tracker.
     * @param frame BGR bytes of a decoded frame.
     * @return 0 on success and -1 on failure.
     */
    private native int feedFrame(long p,
                                 @Nonnull byte[] frame);

    /**
     * Get tracked targets in currently input frames.
     *
     * @param p The pointer of an initialized tracker the user has fed frames to.
     * @return An array of tracklets, each representing a target.
     */
    private native Tracklet[] getTargets(long p);

    private native void free(long p);
}

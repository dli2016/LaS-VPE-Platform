/**
 * \file MSCANFeatureExtracter.java
 *       Input bgr and attributes data of target pedestrains' tracklets;
 *       and return the mscan feature.
 *
 * \Author  by da.li on 2017/06/15
 * \Version 0.1
 */

package org.cripac.isee.alg.pedestrian.reid;

import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.alg.pedestrian.attr.DeepMARCaffe2;
import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.vpe.alg.pedestrian.tracking.TrackletOrURL;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.bytedeco.javacpp.*;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.file.AccessDeniedException;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.cripac.isee.util.ResourceManager.getResource;

public class MSCANFeatureExtracter implements ReIDFeatureExtracter {
    // Load the isee_mscan_reid native library.
    static {
        org.apache.log4j.Logger logger = 
            org.apache.log4j.Logger.getLogger(DeepMARCaffe2.class);
        
        try {
            logger.info("Loading native libraries for MSCANFeatureExtracter from "
                        + System.getProperty("java.library.path"));
            System.loadLibrary("jniisee_mscan_reid");
            logger.info("Native libraries for reid comparison successfully loaded!");
        } catch(Throwable t) {
            logger.error("Failed to load native library for reid", t);
            throw t;
        }
    }

    private long handle;
    private Logger logger;
    private MscanReIDParams params = new MscanReIDParams();

    private int inputWidth = 64;
    private int inputHeight = 160;
    private int inputChannels = 3;

    private float scale = 1.0f / 256.0f;

    private final int FEATURE_LEN = 128;

    /*ReID parameters*/
    public static class MscanReIDParams {
        public int inputWidth = 64;
        public int inputHeight = 160;
        public int inputChannels = 3;
        public int gpuIndex = -1;
        public String modelPath;
        public String protoPath;
    }

    /*Constructor*/
    public MSCANFeatureExtracter(String gpu, 
                                 @Nonnull File pb,
                                 @Nonnull File model,
                                 @Nonnull Logger logger)
        throws FileNotFoundException, AccessDeniedException, CharacterCodingException {
        if (logger == null) {
            this.logger = new ConsoleLogger();
        } else {
            this.logger = logger;
        }

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

        params.protoPath = pb.getPath();
        params.modelPath = model.getPath();
        params.inputWidth = this.inputWidth;
        params.inputHeight= this.inputHeight;
        params.inputChannels = this.inputChannels;
        params.gpuIndex = selectGPURandomly(gpu);
        this.logger.debug("GPU index: " + params.gpuIndex);

        // Initialize.
        handle = initialize(params);
        this.logger.debug("MSCAN reid is initialized!");
    }
    public MSCANFeatureExtracter(String gpu, @Nonnull Logger logger) throws Exception {
        //File pbFile = getResource("models/MSCANCaffe/deploy.prototxt");
        //File modelFile = getResource("models/MSCANCaffe/mscan.caffemodel");
        //this(gpu, pbFile, modelFile, logger);
        this(gpu,
             getResource("/models/MSCANCaffe/deploy.prototxt"),
             getResource("/models/MSCANCaffe/mscan.caffemodel"),
             logger);
    }

    /**
     *  Get the MSCAN reid feature.
     *  Note: we not don't consider attributes ...
     *
     *  @param person the brg data and attributes of the target person.
     *  @return MSCAN feature.
     *  @throws Exception
     */
    @Override
    public synchronized Feature extract(@Nonnull PedestrianInfo person) throws Exception {
        synchronized (this) {
            Tracklet tracklet = person.trackletOrURL.getTracklet();
            
            Tracklet.BoundingBox[] bboxes = tracklet.locationSequence;

            float[][] data = new float[bboxes.length][];

            for (int bi = 0; bi < bboxes.length; ++bi) {
                assert bboxes[bi] != null;
                data[bi] = preprocess(bboxes[bi]);
            }

            //final long startInside = System.currentTimeMillis();
            float[] featureFloat = new float[FEATURE_LEN];
            extractFeature(handle, data, featureFloat);
            Feature feature = new FeatureMSCAN(featureFloat, tracklet.id);
            //final long endInside = System.currentTimeMillis();
            //logger.debug("  -- cost time of only calculating the similarity is " + (endInside - startInside) + "ms");

            return feature;
        }
    }

    /**
     * The function here is to calculate the dissimilarity of two pedestrian
     * using mscan features. (Its only a function to test).
     */
    @Override
    public synchronized float calDissimilarity(@Nonnull PedestrianInfo personA, 
                          @Nonnull PedestrianInfo personB) throws Exception {
        synchronized (this) {
            Tracklet trackletA = personA.trackletOrURL.getTracklet();
            Tracklet trackletB = personB.trackletOrURL.getTracklet();
            Tracklet.BoundingBox[] bboxesA = trackletA.locationSequence;
            Tracklet.BoundingBox[] bboxesB = trackletB.locationSequence;
            float[][] dataA = new float[bboxesA.length][];
            float[][] dataB = new float[bboxesB.length][];
            
            for (int bi = 0; bi < bboxesA.length; ++bi) {
                assert bboxesA[bi] != null;
                dataA[bi] = preprocess(bboxesA[bi]);
            }
            for (int bi = 0; bi < bboxesB.length; ++bi) {
                assert bboxesB[bi] != null;
                dataB[bi] = preprocess(bboxesB[bi]);
            }

            float[] featureA = new float[128];
            float[] featureB = new float[128];

            extractFeature(handle, dataA, featureA);
            extractFeature(handle, dataB, featureB);

            float dissimilarity = calSimilarity(handle, featureA, featureB);
            return dissimilarity;
        }
    }
    @Override
    public synchronized float calDissimilarity(@Nonnull Feature featureA, 
                                               @Nonnull Feature featureB) {
        synchronized (this) {
            float[] featureAFloat = featureA.getVector();
            float[] featureBFloat = featureB.getVector();
            
            float dissimilarity = calSimilarity(handle, featureAFloat, featureBFloat);
            return dissimilarity;
        }
    }

    /**
     * Free the network.
     *
     * @throws Throwable on failure finalizing the super class.
     */
    @Override
    protected void finalize() throws Throwable {
        free(handle);
        super.finalize();
    }

    /** 
     * Select a gpu randomly.
     */
    public static int selectGPURandomly(String gpus) {
        ThreadLocalRandom tlr = ThreadLocalRandom.current();
        String[] gpuIDs = gpus.split(",");
        return Integer.parseInt(gpuIDs[tlr.nextInt(0, gpuIDs.length)]);
    }


    /**
     * Covert the format of rgb data.
     */
    private float[] preprocess(@Nonnull Tracklet.BoundingBox bbox) {
        // Process image.
        opencv_core.Mat image = bbox.getImage();
        opencv_imgproc.resize(image, image, new opencv_core.Size(inputWidth, inputHeight));
        image.convertTo(image, CV_32FC3);

        // Regularize pixel values.
        final int numPixelPerChannel = image.rows() * image.cols();
        final int numPixels = numPixelPerChannel * inputChannels;
        //final FloatPointer imgData = new FloatPointer(image.data());
        // Slice into channels.
        MatVector bgr = new MatVector(inputChannels);
        split(image, bgr);

        // Get pixel data by channel.
        final float[] pixelFloats = new float[numPixels];
        for (int ci = 0; ci < inputChannels; ++ci) {
            final FloatPointer fp = new FloatPointer(bgr.get(ci).data());
            FloatPointer pMeanVal = new FloatPointer(getMeanVal(handle, ci));
            FloatPointer pRegCoeff= new FloatPointer(scale);
            DoublePointer pScale  = new DoublePointer(1.);
            sub32f(fp, 4, pMeanVal, 0, fp, 4, 1, numPixelPerChannel, null);
            mul32f(fp, 4, pRegCoeff, 0, fp, 4, 1, numPixelPerChannel, pScale);
            fp.get(pixelFloats, ci * numPixelPerChannel, numPixelPerChannel);
            // Release.
            fp.deallocate();
            pMeanVal.deallocate();
            pRegCoeff.deallocate();
            pScale.deallocate();
        }
        //imgData.deallocate();
        image.deallocate();
       
        return pixelFloats;
    }

    /**
     * Initialize a native reid feature extracter.
     *
     * @param  params parameters used by calculate the similarity.
     * @return The pointer of the initialized .
     */
    private native long initialize(MscanReIDParams params);

    /**
     * Get the mean value of B,G,R data respectively.
     *
     * @param  handle The pointer of an initialized feature extracter.
     * @param  color  B, G or R.
     * @return mean value of the selected channel.
     */ 
    private native float getMeanVal(long handle, int color);

    /**
     * Calculate the similarity.
     * We use the deep model named MSCAN which provides by Dangwei LI.
     *
     * @param  handle The pointer of an initialized feature extracter.
     * @param  featurePersonA The feature of a person.
     * @param  featurePersonB The feature of a person.
     * @return Dissimilarity of the two input pedestrian (L2 Distance).
     */
    private native float calSimilarity(long handle,
                                       @Nonnull float[] featurePersonA,
                                       @Nonnull float[] featurePersonB);
    /**
     * Extract Feature to do ReID (MSCAN here).
     *
     * @param handle The pointer of an initialized feature extracter.
     * @param trackletsData The tracklet data of target person (multi-frams).
     * @param feature buffer for returning the fc1_body layer.
     */
    private native void extractFeature(long handle, 
                                       @Nonnull float[][] trackletsData,
                                       @Nonnull float[]   feature);

    /**
     * Free resources.
     *
     * @param handle The pointer of an initialized comparer.
     */
    private native void free(long handle);
}

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

package org.cripac.isee.pedestrian.attr;

import com.google.gson.Gson;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.opencv_core;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

import static org.bytedeco.javacpp.caffe.TEST;
import static org.bytedeco.javacpp.opencv_core.*;

/**
 * The class DeepMAR uses the algorithm proposed in
 * Li, D., Chen, X., Huang, K.:
 * Multi-attribute learning for pedestrian attribute recognition in surveillance scenarios. In: Proc. ACPR (2015)
 * to conduct pedestrian attribute recognizing.
 * <p>
 * Created by ken.yu on 17-1-10.
 */
public final class DeepMAR extends PedestrianAttrRecognizer {
    private final static FloatPointer pMean;
    private final static FloatPointer pRegCoeff;
    private final static DoublePointer pScale;

    private final static float MEAN_PIXEL = 128;
    private final static float REG_COEFF = 1.0f / 256;

    static {
        Loader.load(opencv_core.class);
        final float[] meanBuf = new float[1];
        final float[] regBuf = new float[1];
        final double[] scaleBuf = new double[1];
        meanBuf[0] = MEAN_PIXEL;
        regBuf[0] = REG_COEFF;
        scaleBuf[0] = 1;
        pMean = new FloatPointer(meanBuf);
        pRegCoeff = new FloatPointer(regBuf);
        pScale = new DoublePointer(scaleBuf);
    }

    /**
     * Instance of DeepMAR.
     */
    private caffe.FloatNet net = null;

    private final int INPUT_WIDTH = 227;
    private final int INPUT_HEIGHT = 227;
    private final Logger logger;

    /**
     * Create an instance of DeepMAR.
     *
     * @param gpu    The GPU to use.
     * @param logger An external logger.
     */
    public DeepMAR(int gpu,
                   @Nonnull String protocolPath,
                   @Nonnull String weightsPath,
                   @Nullable Logger logger) {
        if (logger == null) {
            this.logger = new ConsoleLogger();
        } else {
            this.logger = logger;
        }

        if (gpu >= 0) {
            this.logger.info("Use GPU with device ID " + gpu);
            caffe.Caffe.SetDevice(gpu);
            caffe.Caffe.set_mode(caffe.Caffe.GPU);
        } else {
            this.logger.info("Use CPU.");
            caffe.Caffe.set_mode(caffe.Caffe.CPU);
        }

        logger.info("Loading DeepMAR protocol from " + new File(protocolPath).getAbsolutePath());
        net = new caffe.FloatNet(protocolPath, TEST);
        logger.info("Loading DeepMAR weights from " + new File(weightsPath).getAbsolutePath());
        net.CopyTrainedLayersFrom(weightsPath);
        this.logger.debug("DeepMAR initialized!");
    }

    /**
     * Recognize attributes from a track of pedestrian.
     *
     * @param tracklet A pedestrian track.
     * @return The attributes of the pedestrian specified by the track.
     * @throws IOException Exception that might occur during recognition.
     */
    @Override
    public Attributes recognize(@Nonnull Tracklet tracklet) throws IOException {
        // Process image.
        final Tracklet.BoundingBox bbox = tracklet.locationSequence[tracklet.locationSequence.length >> 1];
        opencv_core.Mat image = new opencv_core.Mat(bbox.height, bbox.width, CV_8UC3);
        image.data(new BytePointer(bbox.patchData));
        opencv_imgproc.resize(image, image, new opencv_core.Size(INPUT_WIDTH, INPUT_HEIGHT));
        image.convertTo(image, CV_32FC3);

        // Regularize pixel values.
        final int numPixelPerChannel = image.rows() * image.cols();
        final int numPixels = numPixelPerChannel * 3;
        final FloatPointer floatDataPointer = new FloatPointer(image.data());

//        float[] origin = new float[numPixels];
//        floatDataPointer.get(origin);
//        for (int i = 0; i < numPixels; ++i) {
//            origin[i] = (origin[i] - 128) / 256;
//        }
//        floatDataPointer.put(origin);
        // Subtract mean pixel.
        sub32f(floatDataPointer, // Pointer to minuends
                4, // Bytes per step (4 bytes for float)
                pMean, // Pointer to subtrahend
                0, // Bytes per step (using the value 128 circularly)
                floatDataPointer, // Pointer to result buffer.
                4, // Bytes per step (4 bytes for float)
                1, numPixels, // Data dimensions.
                null);
        // Regularize to -0.5 to 0.5. The additional scaling is disabled (set to 1).
        mul32f(floatDataPointer, 4, pRegCoeff, 0, floatDataPointer, 4, 1, numPixels, pScale);

        //Slice into channels.
        MatVector bgr = new MatVector(3);
        split(image, bgr);
        // Get pixel data by channel.
        final float[] pixelFloats = new float[numPixelPerChannel * 3];
        for (int i = 0; i < 3; ++i) {
            final FloatPointer fp = new FloatPointer(bgr.get(i).data());
            fp.get(pixelFloats, i * numPixelPerChannel, numPixelPerChannel);
        }

        // Put the data into the data blob.
        final caffe.FloatBlob dataBlob = net.blob_by_name("data");
        dataBlob.Reshape(1, 3, INPUT_HEIGHT, INPUT_WIDTH);
//        dataBlob.set_cpu_data(pixelFloats); // Seems like there is some bugs with this function.
        dataBlob.cpu_data().put(pixelFloats);

        // Forward the data.
        net.Forward();

        // Transform result to Attributes and return.
        return fillAttributes(net.blob_by_name("fc8"));
    }

    private Attributes fillAttributes(caffe.FloatBlob outputBlob) {
        final float[] outputArray = new float[outputBlob.count()];
        outputBlob.cpu_data().get(outputArray);

        int iter = 0;
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append('{');
        for (String attr : ATTR_LIST) {
            jsonBuilder.append('\"').append(attr).append('\"').append('=').append(outputArray[iter++]);
            if (iter < ATTR_LIST.length) {
                jsonBuilder.append(',');
            }
        }
        jsonBuilder.append('}');
        assert iter == ATTR_LIST.length;

        return new Gson().fromJson(jsonBuilder.toString(), Attributes.class);
    }

    private final static String[] ATTR_LIST;

    static {
        ATTR_LIST = new String[]{
                "action_pulling",
                "lower_green",
                "gender_female",
                "upper_cotton",
                "accessory_other",
                "occlusion_accessory",
                "upper_suit",
                "shoes_casual",
                "shoes_white",
                "lower_pants",
                "shoes_boot",
                "age_60",
                "accessory_backpack",
                "head_shoulder_mask",
                "upper_vest",
                "lower_white",
                "upper_black",
                "upper_white",
                "upper_shirt",
                "upper_silvery",
                "role_client",
                "upper_brown",
                "action_nipthing",
                "shoes_silver",
                "accessory_waistbag",
                "lower_short_skirt",
                "action_picking",
                "shoes_black",
                "occlusion_down",
                "shoes_yellow",
                "gender_other",
                "accessory_shoulderbag",
                "upper_cotta",
                "occlusion_right",
                "action_pushing",
                "shoes_green",
                "action_armstretching",
                "shoes_other",
                "shoes_red",
                "lower_mix_color",
                "occlusion_left",
                "view_angle_left",
                "shoes_sport",
                "lower_gray",
                "upper_other",
                "lower_yellow",
                "head_shoulder_sunglasses",
                "upper_tshirt",
                "accessory_cart",
                "age_16",
                "hair_style_null",
                "upper_hoodie",
                "shoes_mix_color",
                "upper_green",
                "age_older_60",
                "shoes_cloth",
                "action_chatting",
                "shoes_purple",
                "upper_other_color",
                "lower_black",
                "lower_tight_pants",
                "action_holdthing",
                "lower_pink",
                "action_other",
                "upper_orange",
                "lower_jean",
                "hair_style_long",
                "upper_red",
                "lower_silver",
                "lower_short_pants",
                "occlusion_up",
                "lower_blue",
                "upper_purple",
                "upper_pink",
                "shoes_pink",
                "shoes_shandle",
                "shoes_leather",
                "occlusion_environment",
                "view_angle_right",
                "shoes_other_color",
                "lower_one_piece",
                "head_shoulder_with_hat",
                "age_30",
                "shoes_gray",
                "accessory_plasticbag",
                "role_uniform",
                "shoes_brown",
                "action_crouching",
                "lower_purple",
                "weight_very_thin",
                "shoes_blue",
                "weight_normal",
                "action_running",
                "view_angle_front",
                "accessory_paperbag",
                "head_shoulder_black_hair",
                "accessory_box",
                "lower_long_skirt",
                "shoes_orange",
                "weight_little_fat",
                "action_lying",
                "lower_other_color",
                "upper_jacket",
                "upper_blue",
                "lower_orange",
                "upper_gray",
                "accessory_handbag",
                "age_45",
                "lower_skirt",
                "upper_sweater",
                "lower_brown",
                "accessory_kid",
                "occlusion_object",
                "head_shoulder_scarf",
                "gender_male",
                "action_gathering",
                "lower_red",
                "action_calling",
                "head_shoulder_glasses",
                "upper_mix_color",
                "view_angle_back",
                "upper_yellow",
                "weight_very_fat",
                "weight_little_thin",
                "occlusion_other"};
    }
}

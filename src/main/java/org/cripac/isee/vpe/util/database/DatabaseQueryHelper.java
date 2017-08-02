/**
 * DatabaseQueryHelper.java
 * It is a file that provides some auxilary functions to help the users query
 * the neo4j database conveniently.
 *
 * @Author  da.li on 2017/07/21
 * @Version 0.1
 */

package org.cripac.isee.vpe.util.database;

import com.google.gson.*;
import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.codec.binary.Base64;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;

import static org.bytedeco.javacpp.opencv_core.CV_8UC3;
import static org.bytedeco.javacpp.opencv_imgcodecs.imdecode;
import static org.bytedeco.javacpp.opencv_imgcodecs.imencode;

/**
 * Class: DatabaseQueryHelper
 */
public class DatabaseQueryHelper {

    static {
        // These two lines are used to solve the following problem:
        // RuntimeException: No native JavaCPP library
        // in memory. (Has Loader.load() been called?)
        Loader.load(org.bytedeco.javacpp.helper.opencv_core.class);
        Loader.load(opencv_imgproc.class);
    }

    /**
     * Generate the complete video url. 
     *
     * @param videoURL is a property in the person node.
     *                 Please query its value from neo4j.
     *                 e.g. "CAM01-20131223151500-20131223152048"
     * @param rootDir  is the root path stores all the source video data.
     *                 e.g. "user/vpe.cripac/source_data/video_rap_mp4"
     * @param filePosfix is the posfix of a video file.
     *                   e.g. "mp4"
     * @return A complete video url corresponding to a tracklet.
     */
    public static String generateWholeVideoURL(@Nonnull String videoURL,
                                               @Nonnull String rootDir,
                                               @Nonnull String filePosfix) {
        // Parse the property of videoURL
        String camID = videoURL.split("-")[0];
        String videoStartTime = videoURL.split("-")[1];
        String videoEndTime = videoURL.split("-")[2];
        // Get the year-month-day
        String year = videoStartTime.substring(0,4);
        String month= videoStartTime.substring(4,6);
        String day  = videoStartTime.substring(6,8);
        String timeDir = year + "-" + month + "-" + day;
        // Generate the complete video url.
        String wholeVideoURL = rootDir + "/" + camID + "/" + timeDir + "/" +
                               videoURL + "." + filePosfix;
        return wholeVideoURL;
    }

    /**
     * Generate the image by a line in file named tracklet.data.
     *
     * @param videoURL         a property in the person node.
     * @param trackletDataPath path of tracklet.data in local filesystem.
     * @param outputDir        the dir that you want the image output to local
     *                         filesystem.
     *
     * @return the image filename generate by the frame data.
     */
    public static String[] generateImage(@Nonnull String videoURL,
                                       @Nonnull String trackletDataPath,
                                       @Nonnull String outputDir) throws Exception {
        String imageFilenameTemp = outputDir + "/" + videoURL + "-";
        // Load file ...
        String trackletDataFilename = trackletDataPath + "/tracklet.data";
        final InputStreamReader dataReader = 
            new InputStreamReader(new FileInputStream(new File(trackletDataFilename)));
        BufferedReader br = new BufferedReader(dataReader);
        // Read line by line.
        JsonParser jParser = new JsonParser();
        String jsonData = null;
        List<String> imgFilenames = new ArrayList<String>();
        while ((jsonData = br.readLine()) != null) {
            JsonObject jObject = jParser.parse(jsonData).getAsJsonObject();
            int w = Integer.parseInt(jObject.get("width").getAsString());
            int h = Integer.parseInt(jObject.get("height").getAsString());
            int idx = Integer.parseInt(jObject.get("idx").getAsString());
            String dataBase64String = jObject.get("data").getAsString();
            byte[] data = Base64.decodeBase64(dataBase64String);
            // Generate images.
            final BytePointer inputPointer = new BytePointer(data);
            final opencv_core.Mat image = new opencv_core.Mat(h, w, CV_8UC3, 
                                          inputPointer);
            final BytePointer outputPointer = new BytePointer();
            imencode(".jpg", image, outputPointer);
            // Output image.
            String filename = imageFilenameTemp + idx + ".jpg";
            imgFilenames.add(filename);
            final byte[] bytes = new byte[(int) outputPointer.limit()];
            outputPointer.get(bytes);
            final FileOutputStream imgOutputStream;
            try {
                imgOutputStream = new FileOutputStream(new File(filename));
                imgOutputStream.write(bytes);
                imgOutputStream.close();
            } catch(IOException e) {
                e.printStackTrace();
            }
            // Free resources.
            image.release();
            inputPointer.deallocate();
            outputPointer.deallocate();
        }
        br.close();

        String[] names = new String[imgFilenames.size()];
        imgFilenames.toArray(names);
        return names;
    }

    // Test
    public static void main(String[] args) throws Exception {
        String videoURL = "CAM01-20131223141628-20131223142220";
        String rootDir = "/user/vpe.cripac/source_data/video_rap_mp4";
        String filePosfix = "mp4";
        // Completed file name.
        String filePath = generateWholeVideoURL(videoURL, rootDir, filePosfix);
        System.out.println("VideoName: " + filePath);
        // Generate images.
        String trackletDataPath = "/home/vpe.cripac/projects/test/da.li/ssd/LaS-VPE-Platform/test_data/tracklets/65";
        String outputDir = "/home/vpe.cripac/projects/test/da.li/ssd/LaS-VPE-Platform/test_data/tracklets/65";
        String[] imgFilenames = generateImage(videoURL, trackletDataPath, outputDir);
        for (int i = 0; i < imgFilenames.length; ++i) {
            System.out.println(imgFilenames[i]);
        }
    }
}

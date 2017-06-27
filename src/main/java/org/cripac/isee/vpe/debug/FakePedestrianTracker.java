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

package org.cripac.isee.vpe.debug;

import org.cripac.isee.alg.pedestrian.tracking.Tracker;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet.BoundingBox;
import org.cripac.isee.alg.pedestrian.tracking.Tracklet.Identifier;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.util.Random;

import java.io.File;
import java.io.FileInputStream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_imgproc;

import static org.bytedeco.javacpp.opencv_imgcodecs.IMREAD_COLOR;
import static org.bytedeco.javacpp.opencv_imgcodecs.imread;

public class FakePedestrianTracker implements Tracker {

    private Random random = new Random();

    private Tracklet generateRandomTracklet() {
        Tracklet tracklet = new Tracklet();
        tracklet.startFrameIndex = random.nextInt(10000) + 1;
        tracklet.id.videoID = "fake video";
        tracklet.id.serialNumber = -1;

        int appearSpan = random.nextInt(31) + 1;
        tracklet.locationSequence = new BoundingBox[appearSpan];
        for (int i = 0; i < appearSpan; ++i) {
            BoundingBox bbox = new BoundingBox();
            bbox.width = random.nextInt(640) + 1;
            bbox.height = random.nextInt(640) + 1;
            bbox.x = random.nextInt(bbox.width) + 1;
            bbox.y = random.nextInt(bbox.height) + 1;
            bbox.patchData = new byte[bbox.width * bbox.height * 3];
            random.nextBytes(bbox.patchData);

            tracklet.locationSequence[i] = bbox;
        }

        return tracklet;
    }

    private Tracklet[] generateRandomTrackSet() {
        int numTracks = random.nextInt(30) + 3;
        Tracklet[] tracklets = new Tracklet[numTracks];
        for (int i = 0; i < numTracks; ++i) {
            Tracklet tracklet = generateRandomTracklet();
            tracklet.id.serialNumber = i;
            tracklet.numTracklets = numTracks;
            tracklet.sample(5);
            tracklets[i] = tracklet;
        }

        return tracklets;
    }

    private Tracklet[] loadTestTracklets() {
        int numTracks = 2;
        Tracklet[] tracklets = new Tracklet[numTracks];
        String dirA = "src/test/resources/tracklets/1";
        String dirB = "src/test/resources/tracklets/2";
        tracklets[0] = loadTracklet(dirA);
        tracklets[1] = loadTracklet(dirB);

        return tracklets;
    }

    private Tracklet loadTracklet(String storedLocalDir) {
        File f = new File(storedLocalDir);
        if (!f.exists()) {
            System.out.println(storedLocalDir + " not exists!");
            return null;
        }
        File files[] = f.listFiles();
        List<BoundingBox> samples = new ArrayList<>();
        for (int i = 0; i < files.length; ++i) {
            File fs = files[i];
            if (fs.isDirectory()) {
                continue;
            } else {
                BoundingBox bbox = new BoundingBox();
                String imagePath = storedLocalDir + "/" + fs.getName();
                opencv_core.Mat img = imread(imagePath, IMREAD_COLOR);
                if (img == null || img.empty()) {
                    System.out.println("Error: Load image FAILED!");
                    return null;
                }
                bbox.width = img.cols();
                bbox.height= img.rows();
                bbox.patchData = new byte[img.rows() * img.cols() * img.channels()];
                img.data().get(bbox.patchData);
                img.release();
                samples.add(bbox);
            }
        }
        BoundingBox[] bboxes = new BoundingBox[samples.size()];
        samples.toArray(bboxes);
        Tracklet tracklet = new Tracklet();
        tracklet.id.videoID = storedLocalDir;
        tracklet.id.serialNumber = 1;
        tracklet.locationSequence = bboxes;

        return tracklet;
    }

    @Nonnull
    @Override
    public Tracklet[] track(@Nonnull InputStream videoStream) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return loadTestTracklets();
    }
}

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

package org.cripac.isee.alg.pedestrian.reid;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * The ReIDFeatureExtracter class is the super class for any ReID feature
 * extracter classes.
 *
 * @author da.li, CRIPAC, 2017
 */
public interface ReIDFeatureExtracter {

    /**
     * Extract ReID feature with a target pedestrian.
     *
     * @param pedestrian The target pedestrian.
     * @return ReID feature of the pedestrian.
     * @throws IOException On error conducting ReID.
     */
    Feature extract(@Nonnull PedestrianInfo pedestrian) throws Exception;

    /**
     * Get dissimilarity (only for test).
     */
    float calDissimilarity(@Nonnull PedestrianInfo pedestrianA,
                           @Nonnull PedestrianInfo pedestrianB) throws Exception;
    float calDissimilarity(@Nonnull Feature featureA,
                           @Nonnull Feature featureB) throws Exception;
}

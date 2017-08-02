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

import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.reid.Feature;
import org.cripac.isee.vpe.data.GraphDatabaseConnector;

import javax.annotation.Nonnull;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Simulate a database connector that provides tracklets and attributes.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class FakeDatabaseConnector extends GraphDatabaseConnector {

    private Random rand = new Random();

    /*
     * (non-Javadoc)
     *
     * @see
     * GraphDatabaseConnector#setTrackletSavingPath(
     * java.lang.String, java.lang.String)
     */
    @Override
    public void setTrackletSavingPath(@Nonnull String nodeID,
                                      @Nonnull String path) {
    }

    @Override
    public void setPedestrianTracklet(@Nonnull String nodeID,
                                      @Nonnull String dataType,
                                      @Nonnull String trackletPath,
                                      @Nonnull String trackletInfo) {

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * GraphDatabaseConnector#getTrackSavingPath(
     * java.lang.String, java.lang.String)
     */
    @Override
    public String getTrackletSavingDir(@Nonnull String nodeID,
                                       @Nonnull String dataType) throws NoSuchElementException {
        return "har:///user/labadmin/metadata/" + nodeID;
    }

    /*
     * (non-Javadoc)
     *
     * @see GraphDatabaseConnector#
     * setPedestrianSimilarity(java.lang.String, java.lang.String, float)
     */
    @Override
    public void setPedestrianSimilarity(@Nonnull String idA,
                                        @Nonnull String idB, float similarity) {
    }

    /*
     * (non-Javadoc)
     *
     * @see GraphDatabaseConnector#
     * getPedestrianSimilarity(java.lang.String, java.lang.String)
     */
    @Override
    public float getPedestrianSimilarity(@Nonnull String idA,
                                         @Nonnull String idB) throws NoSuchElementException {
        return rand.nextFloat();
    }

    /*
     * (non-Javadoc)
     *
     * @see GraphDatabaseConnector#
     * setPedestrianAttributes(java.lang.String,
     * Attributes)
     */
    @Override
    public void setPedestrianAttributes(@Nonnull String nodeID,
                                        @Nonnull String dataType,
                                        @Nonnull Attributes attr) {
    }

    /**
     * Fake: set extracted reid features.
     * Add by da.li on 2017/06/23
     */
    @Override
    public void setPedestrianReIDFeature(@Nonnull String nodeID,
                                         @Nonnull String dataType,
                                         @Nonnull Feature fea) {
    }

    /*
     * (non-Javadoc)
     *
     * @see GraphDatabaseConnector#
     * getPedestrianAttributes(java.lang.String)
     */
    @Override
    public Attributes getPedestrianAttributes(@Nonnull String nodeID,
                                              @Nonnull String dataType) throws NoSuchElementException {
        return new Attributes();
    }

    @Override
    public Feature getPedestrianReIDFeature(@Nonnull String nodeID,
                                            @Nonnull String dataType) throws NoSuchElementException {
        return null;
    }

    @Override
    public Link[] getLinkedPedestrians(@Nonnull String nodeID) throws NoSuchElementException {
        return null;
    }
}

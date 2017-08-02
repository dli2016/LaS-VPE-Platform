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

package org.cripac.isee.vpe.data;

import org.cripac.isee.alg.pedestrian.attr.Attributes;
import org.cripac.isee.alg.pedestrian.reid.Feature;

import javax.annotation.Nonnull;
import java.util.NoSuchElementException;

/**
 * The class GraphDatabaseConnector is the base class of graph databases connectors.
 * Each node in the graph database is a pedestrian.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public abstract class GraphDatabaseConnector {

    /**
     * Set the path of the directory saving the tracklet of a pedestrian.
     *
     * @param nodeID the id of the pedestrian.
     * @param path   the path of the directory saving the tracklet of the pedestrian.
     */
    public abstract void setTrackletSavingPath(@Nonnull String nodeID,
                                               @Nonnull String path);

    /**
     * Set tracklet information of a pedestrian.
     *
     * @param nodeID   new Singleton.
     * @param dataType it depends on the execute plan (the algorithm to generate such result).
     * @param trackletPath the path of the directory saving the tracklet of the pedestrian.
     * @param trackletInfo bounding box information of a tracklet.
     */
    public abstract void setPedestrianTracklet(@Nonnull String nodeID,
                                               @Nonnull String dataType,
                                               @Nonnull String trackletPath,
                                               @Nonnull String trackletInfo);

    /**
     * Get the path of the directory saving the tracklet of a pedestrian.
     *
     * @param nodeID the id of the pedestrian.
     * @return the path of the directory saving the tracklet of the pedestrian.
     * @throws NoSuchElementException On failure finding the pedestrian.
     */
    public abstract String getTrackletSavingDir(@Nonnull String nodeID,
                                                @Nonnull String dataType) throws NoSuchElementException;

    /**
     * Set the similarity between two pedestrians.
     *
     * @param idA        the ID of the first pedestrian.
     * @param idB        the ID of the second pedestrian.
     * @param similarity the similarity between them.
     */
    public abstract void setPedestrianSimilarity(@Nonnull String idA,
                                                 @Nonnull String idB,
                                                 float similarity);

    /**
     * Get the similarity between two pedestrians.
     *
     * @param idA the ID of the first pedestrian.
     * @param idB the ID of the second pedestrian.
     * @return The similarity between them.
     * @throws NoSuchElementException On failure finding any of these two pedestrian, or when there
     *                                is no link between them.
     */
    public abstract float getPedestrianSimilarity(@Nonnull String idA,
                                                  @Nonnull String idB) throws NoSuchElementException;

    /**
     * Set the attributes of a pedestrian.
     *
     * @param nodeID   the ID of the pedestrian.
     * @param dataType it depends on the execute plan (the algorithm to generate such result).
     * @param attr     the attributes of the pedestrian.
     */
    public abstract void setPedestrianAttributes(@Nonnull String nodeID,
                                                 @Nonnull String dataType,
                                                 @Nonnull Attributes attr);

    /**
     * Set the reid feature of a pedestrian.
     * 
     * @param nodeID   the ID of the pedestrian.
     * @param dataType it depends on the execute plan (the algorithm to generate such result).
     * @param fea      the reid feature of the pedestrian.
     */
    public abstract void setPedestrianReIDFeature(@Nonnull String nodeID,
                                                  @Nonnull String dataType,
                                                  @Nonnull Feature fea);

    /**
     * Get the attributes of a pedestrian.
     *
     * @param nodeID the ID of the pedestrian.
     * @param dataType user plan.
     * @return the attributes of the pedestrian.
     * @throws NoSuchElementException On failure finding the pedestrian.
     */
    public abstract Attributes getPedestrianAttributes(@Nonnull String nodeID,
                                                       @Nonnull String dataType) throws NoSuchElementException;

    /**
     * Get the reid feature of a pedestrian.
     *
     * @param nodeID the ID of the pedestrian.
     * @param dataType user plan.
     * @return the reid feature of the pedestrian.
     * @throws NoSuchElementException On failure finding the pedestrian.
     */
    public abstract Feature getPedestrianReIDFeature(@Nonnull String nodeID,
                                                     @Nonnull String dataType) throws NoSuchElementException;

    /**
     * Get relations: (nodA)-[SIMILARITY]-(nodeB)
     *
     * @param nodeID the ID of the pedestrian.
     * @return the relationships related to the input pedestrian.
     * @throws NoSuchElementException On failure finding the pedestrian.
     */
    public abstract Link[] getLinkedPedestrians(@Nonnull String nodeID) throws NoSuchElementException;

    /**
     * The class Link represents a link from one node to another in the graph
     * database.
     *
     * @author Ken Yu, CRIPAC, 2016
     */
    public static class Link {
        public String nodeA;
        public String nodeB;
        public float similarity;

        public Link() {
        }

        public Link(@Nonnull String nodeA,
                    @Nonnull String nodeB,
                    float similarity) {
            this.nodeA = nodeA;
            this.nodeB = nodeB;
            this.similarity = similarity;
        }
    }
}

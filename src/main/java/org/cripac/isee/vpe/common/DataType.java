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

package org.cripac.isee.vpe.common;

/**
 * Enumeration of data types that can be outputted or accepted by applications.
 * In current version, each data type correspond to one Kafka topic.
 * <p>
 * Created by ken.yu on 16-10-27.
 */
public enum DataType {
    ATTRIBUTES,
    COMMAND,
    IDRANK,
    TRACKLET,
    TRACKLET_ID,
    TRACKLET_ATTR,
    URL,
    FRAME_ARRAY,
    REID_FEATURE,
    /**
     * Login parameters for web cameras.
     *
     * @see LoginParam
     */
    WEBCAM_LOGIN_PARAM,
    /**
     * A terminal signal is a task UUID sent to the TERM_SIG topic.
     */
    TERM_SIG,
    NONE
}

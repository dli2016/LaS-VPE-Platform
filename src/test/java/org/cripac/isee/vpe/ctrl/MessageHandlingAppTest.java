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

package org.cripac.isee.vpe.ctrl;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Level;
import org.cripac.isee.vpe.common.DataType;
import org.cripac.isee.vpe.common.LoginParam;
import org.cripac.isee.vpe.util.logging.ConsoleLogger;
import org.junit.Before;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import static org.apache.commons.lang3.SerializationUtils.serialize;
import static org.cripac.isee.vpe.util.kafka.KafkaHelper.sendWithLog;

/**
 * The MessageHandlingAppTest class is for simulating commands sent to the message
 * handling application through Kafka.
 *
 * @author Ken Yu, CRIPAC, 2016
 */
public class MessageHandlingAppTest implements Serializable {

    private static final long serialVersionUID = 6788686506662339278L;
    private KafkaProducer<String, byte[]> producer;
    private ConsoleLogger logger;

    public static void main(String[] args) throws Exception {
        MessageHandlingAppTest app = new MessageHandlingAppTest();
        app.init(args);
        //app.generatePresetCommand();
        /*
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2013-12-24", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-12", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-15", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-17", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-21", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-25", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-26", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-02", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-03", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-10", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2013-12-23", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-11", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-13", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-14", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-15", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-16", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-20", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-22", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-23", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-24", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-27", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-02-28", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-01", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-04", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-05", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-07", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-08", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-11", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-12", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-13", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-18", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-20", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-21", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-24", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-25", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-26", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-27", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-28", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-03-31", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-04-15", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-04-16", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-04-17", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-04-18", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-04-19", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-04-20", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-04-21", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-04-22", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-04-23", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-04-24", "source_data");
        app.generatePresetCommand("/user/vpe.cripac/source_data/video/CAM01/2014-04-25", "source_data");
        */
        app.generatePresetCommand("/user/vpe.cripac/test/da.li/videos/2014-02-26", "test");
    }

    @Before
    public void init() throws Exception {
        init(new String[]{"-a", MessageHandlingApp.APP_NAME,
                "--system-property-file", "conf/system.properties",
                "--app-property-file", "conf/" + MessageHandlingApp.APP_NAME + "/app.properties",
                "-v"});
    }

    private void init(String[] args) throws Exception {
        List<String> argList = new ArrayList<>(Arrays.asList(args));
        argList.add("-a");
        argList.add(MessageHandlingApp.APP_NAME);
        args = new String[argList.size()];
        argList.toArray(args);
        SystemPropertyCenter propCenter = new SystemPropertyCenter(args);

        Properties producerProp = propCenter.getKafkaProducerProp(false);
        producer = new KafkaProducer<>(producerProp);
        logger = new ConsoleLogger(Level.DEBUG);
    }

    private String[] getFileList(String path) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.setBoolean("dfs.support.append", true);
        String hadoopHome = System.getenv("HADOOP_HOME");
        
        FileSystem hdfs = FileSystem.get(new URI("hdfs://rtask-nod8:8020"), conf);
        List<String> files = new ArrayList<String>();
        Path s_path = new Path(path);

        if (hdfs.exists(s_path)) {
            for (FileStatus status:hdfs.listStatus(s_path)) {
                files.add(status.getPath().toString());
            }
        } else {
            System.out.println("Hey guys, you are wrong!");
        }
        hdfs.close();

        return files.toArray(new String[]{});
    }

    public void generatePresetCommand(String path, String flag) throws Exception {
        String[] filenames = getFileList(path);
        int idx = 0;
        HashMap<String, Serializable> param = new HashMap<>();
        param.put(MessageHandlingApp.Parameter.TRACKING_CONF_FILE,
                "isee-basic/CAM01_0.conf");
        param.put(MessageHandlingApp.Parameter.WEBCAM_LOGIN_PARAM,
                new Gson().toJson(new LoginParam(InetAddress.getLocalHost(), 0,
                        "Ken Yu", "I love Shenzhen!")));
        for (String filename : filenames) {
            String video_name = filename.substring(filename.indexOf(flag), filename.length());
            idx = idx + 1;
            System.out.println("====================== video " +  ""+idx  + " =======================");
            System.out.println("Video Name: "+ video_name);
            param.put(MessageHandlingApp.Parameter.VIDEO_URL, video_name);
            /*
            */
            sendWithLog(DataType.COMMAND.name(),
                    MessageHandlingApp.CommandType.TRACK_ATTRRECOG_REIDFEATURE,
                    serialize(param),
                    producer,
                    logger);
            /*
            sendWithLog(DataType.COMMAND.name(),
                    MessageHandlingApp.CommandType.TRACK_ONLY,
                    serialize(param),
                    producer,
                    logger);
            */
            /*
            sendWithLog(DataType.COMMAND.name(),
                    MessageHandlingApp.CommandType.TRACK_ATTRRECOG,
                    serialize(param),
                    producer,
                    logger);
            */
        }
    }

    //    @Test
    public void generatePresetCommand() throws Exception {
        String[] cam01VideoURLs = {
                "source_data/video/CAM01/2013-12-20/20131220183101-20131220184349.h264",
                "source_data/video/CAM01/2013-12-20/20131220184349-20131220184937.h264",
                "source_data/video/CAM01/2013-12-23/20131223102147-20131223102739.h264",
                "source_data/video/CAM01/2013-12-23/20131223102739-20131223103331.h264",
                "source_data/video/CAM01/2013-12-23/20131223103331-20131223103919.h264",
                "source_data/video/CAM01/2013-12-23/20131223103919-20131223104515.h264",
                "source_data/video/CAM01/2013-12-23/20131223104515-20131223105107.h264",
                "source_data/video/CAM01/2013-12-23/20131223105107-20131223105659.h264",
                /*
                "source_data/video/CAM01/2013-12-23/20131223105659-20131223110255.h264",
                "source_data/video/CAM01/2013-12-23/20131223110255-20131223110847.h264",
                "source_data/video/CAM01/2013-12-23/20131223110847-20131223111447.h264",
                "source_data/video/CAM01/2013-12-23/20131223111447-20131223112043.h264",
                "source_data/video/CAM01/2013-12-23/20131223112043-20131223112635.h264",
                "source_data/video/CAM01/2013-12-23/20131223112635-20131223113227.h264",
                "source_data/video/CAM01/2013-12-23/20131223113227-20131223113815.h264",
                "source_data/video/CAM01/2013-12-23/20131223113815-20131223114407.h264",
                "source_data/video/CAM01/2013-12-23/20131223114407-20131223114959.h264",
                "source_data/video/CAM01/2013-12-23/20131223114959-20131223115551.h264",
                "source_data/video/CAM01/2013-12-23/20131223115551-20131223120147.h264",
                "source_data/video/CAM01/2013-12-23/20131223120147-20131223120735.h264",
                "source_data/video/CAM01/2013-12-23/20131223120735-20131223121327.h264",
                "source_data/video/CAM01/2013-12-23/20131223121327-20131223121915.h264",
                "source_data/video/CAM01/2013-12-23/20131223121915-20131223122515.h264",
                "source_data/video/CAM01/2013-12-23/20131223122515-20131223123103.h264",
                "source_data/video/CAM01/2013-12-23/20131223123103-20131223123651.h264",
                "source_data/video/CAM01/2013-12-23/20131223123651-20131223124239.h264",
                "source_data/video/CAM01/2013-12-23/20131223124239-20131223124831.h264",
                "source_data/video/CAM01/2013-12-23/20131223124831-20131223125419.h264",
                "source_data/video/CAM01/2013-12-23/20131223125419-20131223130015.h264",
                "source_data/video/CAM01/2013-12-23/20131223130015-20131223130607.h264",
                "source_data/video/CAM01/2013-12-23/20131223130608-20131223131203.h264",
                "source_data/video/CAM01/2013-12-23/20131223131203-20131223131755.h264",
                "source_data/video/CAM01/2013-12-23/20131223131756-20131223132347.h264",
                "source_data/video/CAM01/2013-12-23/20131223132348-20131223132939.h264",
                "source_data/video/CAM01/2013-12-23/20131223132940-20131223133527.h264",
                "source_data/video/CAM01/2013-12-23/20131223133528-20131223134119.h264",
                "source_data/video/CAM01/2013-12-23/20131223134120-20131223134707.h264",
                "source_data/video/CAM01/2013-12-23/20131223134708-20131223135259.h264",
                "source_data/video/CAM01/2013-12-23/20131223135300-20131223135851.h264",
                "source_data/video/CAM01/2013-12-23/20131223135852-20131223140439.h264",
                "source_data/video/CAM01/2013-12-23/20131223140440-20131223141032.h264",
                "source_data/video/CAM01/2013-12-23/20131223141032-20131223141628.h264",
                "source_data/video/CAM01/2013-12-23/20131223141628-20131223142220.h264",
                "source_data/video/CAM01/2013-12-23/20131223142220-20131223142812.h264",
                "source_data/video/CAM01/2013-12-23/20131223142812-20131223143408.h264",
                "source_data/video/CAM01/2013-12-23/20131223143408-20131223144000.h264",
                "source_data/video/CAM01/2013-12-23/20131223144000-20131223144548.h264",
                "source_data/video/CAM01/2013-12-23/20131223144548-20131223145144.h264",
                "source_data/video/CAM01/2013-12-23/20131223145144-20131223145732.h264",
                "source_data/video/CAM01/2013-12-23/20131223145732-20131223150320.h264",
                "source_data/video/CAM01/2013-12-23/20131223150320-20131223150912.h264",
                "source_data/video/CAM01/2013-12-23/20131223150912-20131223151500.h264",
                "source_data/video/CAM01/2013-12-23/20131223151500-20131223152048.h264",
                "source_data/video/CAM01/2013-12-23/20131223152048-20131223152640.h264",
                "source_data/video/CAM01/2013-12-23/20131223152640-20131223153228.h264",
                "source_data/video/CAM01/2013-12-23/20131223153228-20131223153820.h264",
                "source_data/video/CAM01/2013-12-23/20131223153820-20131223154416.h264",
                "source_data/video/CAM01/2013-12-23/20131223154416-20131223155008.h264",
                "source_data/video/CAM01/2013-12-23/20131223155008-20131223155600.h264",
                "source_data/video/CAM01/2013-12-23/20131223155600-20131223160152.h264",
                "source_data/video/CAM01/2013-12-23/20131223160152-20131223160740.h264",
                "source_data/video/CAM01/2013-12-23/20131223160740-20131223161332.h264",
                "source_data/video/CAM01/2013-12-23/20131223161332-20131223161924.h264",
                "source_data/video/CAM01/2013-12-23/20131223161924-20131223162516.h264",
                "source_data/video/CAM01/2013-12-23/20131223162516-20131223163108.h264",
                "source_data/video/CAM01/2013-12-23/20131223163108-20131223163656.h264",
                "source_data/video/CAM01/2013-12-23/20131223163656-20131223164248.h264",
                "source_data/video/CAM01/2013-12-23/20131223164248-20131223164836.h264",
                "source_data/video/CAM01/2013-12-23/20131223164836-20131223165428.h264",
                "source_data/video/CAM01/2013-12-23/20131223165428-20131223170020.h264",
                "source_data/video/CAM01/2013-12-23/20131223170020-20131223170620.h264",
                "source_data/video/CAM01/2013-12-23/20131223170620-20131223171208.h264",
                "source_data/video/CAM01/2013-12-23/20131223171208-20131223171800.h264",
                "source_data/video/CAM01/2013-12-23/20131223171800-20131223172352.h264",
                "source_data/video/CAM01/2013-12-23/20131223172352-20131223172948.h264",
                "source_data/video/CAM01/2013-12-23/20131223172948-20131223173540.h264",
                "source_data/video/CAM01/2013-12-23/20131223173540-20131223174132.h264",
                "source_data/video/CAM01/2013-12-23/20131223174132-20131223174724.h264",
                "source_data/video/CAM01/2013-12-23/20131223174724-20131223175316.h264",
                "source_data/video/CAM01/2013-12-23/20131223175316-20131223175916.h264",
                "source_data/video/CAM01/2013-12-23/20131223175916-20131223180508.h264",
                */
        };

        HashMap<String, Serializable> param = new HashMap<>();
        param.put(MessageHandlingApp.Parameter.TRACKING_CONF_FILE,
                "isee-basic/CAM01_0.conf");
        param.put(MessageHandlingApp.Parameter.WEBCAM_LOGIN_PARAM,
                new Gson().toJson(new LoginParam(InetAddress.getLocalHost(), 0,
                        "Ken Yu", "I love Shenzhen!")));

        for (String url : cam01VideoURLs) {
            param.put(MessageHandlingApp.Parameter.VIDEO_URL, url);

            sendWithLog(DataType.COMMAND.name(),
                    MessageHandlingApp.CommandType.TRACK_ONLY,
                    serialize(param),
                    producer,
                    logger);
            /*
            sendWithLog(DataType.COMMAND.name(),
                    MessageHandlingApp.CommandType.TRACK_ATTRRECOG_REIDFEATURE,
                    serialize(param),
                    producer,
                    logger);
            */
            /*
            sendWithLog(DataType.COMMAND.name(),
                    MessageHandlingApp.CommandType.TRACK_ATTRRECOG_REID,
                    serialize(param),
                    producer,
                    logger);
            */
        }
        /*
        param.put(MessageHandlingApp.Parameter.TRACKLET_INDEX, "1");

        sendWithLog(DataType.COMMAND.name(),
                MessageHandlingApp.CommandType.ATTRRECOG_ONLY,
                serialize(param),
                producer,
                logger);

        sendWithLog(DataType.COMMAND.name(),
                MessageHandlingApp.CommandType.ATTRRECOG_REID,
                serialize(param),
                producer,
                logger);

        sendWithLog(DataType.COMMAND.name(),
                MessageHandlingApp.CommandType.REID_ONLY,
                serialize(param),
                producer,
                logger);
        */
    }
}

/*
 * This file is part of las-vpe-platform.
 *
 * las-vpe-platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * las-vpe-platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with las-vpe-platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-3-13.
 */
package org.cripac.isee.vpe.ctrl;

import com.google.gson.Gson;
import com.sun.management.OperatingSystemMXBean;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.Logger;

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class MonitorThread extends Thread {

    public static final String REPORT_TOPIC = "monitor-report";

    private static class Report {
        long usedMem;
        long jvmMaxMem;
        long jvmTotalMem;
        long physicTotalMem;
        int procCpuLoad;
        int sysCpuLoad;
        DevInfo[] devInfos;

        private static class DevInfo {
            int fanSpeed;
            int utilRate;
            long usedMem;
            long totalMem;
            int temp;
            int slowDownTemp;
            int shutdownTemp;
            int powerUsage;
            int powerLimit;
        }
    }

    private final Logger logger;
    private final KafkaProducer<String, String> reportProducer;
    private final Runtime runtime = Runtime.getRuntime();
    private final OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    private final int deviceCount;
    private final String nodeName;

    private native int initNVML();

    private native int getDeviceCount();

    private native int getFanSpeed(int index);

    private native int getUtilizationRate(int index);

    private native long getFreeMemory(int index);

    private native long getTotalMemory(int index);

    private native long getUsedMemory(int index);

    private native int getTemperature(int index);

    private native int getSlowDownTemperatureThreshold(int index);

    private native int getShutdownTemperatureThreshold(int index);

    private native int getPowerLimit(int index);

    private native int getPowerUsage(int index);

    public MonitorThread(Logger logger, SystemPropertyCenter propCenter)
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {
        this.logger = logger;
        this.reportProducer = new KafkaProducer<>(propCenter.getKafkaProducerProp(true));

        String nodeName1;
        try {
            nodeName1 = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            nodeName1 = "Unknown host";
        }
        nodeName = nodeName1;

        KafkaHelper.createTopic(propCenter.zkConn, propCenter.zkSessionTimeoutMs, propCenter.zkConnectionTimeoutMS,
                REPORT_TOPIC,
                propCenter.kafkaNumPartitions, propCenter.kafkaReplFactor);

        logger.info("Running with " + osBean.getAvailableProcessors() + " " + osBean.getArch() + " processors");

        int nvmlInitRet = initNVML();
        if (nvmlInitRet == 0) {
            this.deviceCount = getDeviceCount();
            logger.info("Running with " + this.deviceCount + " GPUs.");
        } else {
            this.deviceCount = 0;
            logger.info("Cannot initialize NVML: " + nvmlInitRet);
        }
    }

    @Override
    public void run() {
        Report report = new Report();
        report.devInfos = new Report.DevInfo[deviceCount];
        for (int i = 0; i < deviceCount; ++i) {
            report.devInfos[i] = new Report.DevInfo();
        }

        logger.debug("Starting monitoring!");
        //noinspection InfiniteLoopStatement
        while (true) {
            report.usedMem = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
            report.jvmMaxMem = runtime.maxMemory() / (1024 * 1024);
            report.jvmTotalMem = runtime.totalMemory() / (1024 * 1024);
            report.physicTotalMem = osBean.getTotalPhysicalMemorySize() / (1024 * 1024);
            logger.info("Memory consumption: "
                    + report.usedMem + "/"
                    + report.jvmMaxMem + "/"
                    + report.jvmTotalMem + "/"
                    + report.physicTotalMem + "M");

            report.procCpuLoad = (int) (osBean.getProcessCpuLoad() * 100);
            report.sysCpuLoad = (int) (osBean.getSystemCpuLoad() * 100);
            logger.info("CPU load: " + report.procCpuLoad + "/" + report.sysCpuLoad + "%");

            StringBuilder stringBuilder = new StringBuilder("GPU Usage:");
            for (int i = 0; i < deviceCount; ++i) {
                Report.DevInfo info = report.devInfos[i];
                info.fanSpeed = getFanSpeed(i);
                info.utilRate = getUtilizationRate(i);
                info.usedMem = getUsedMemory(i);
                info.totalMem = getTotalMemory(i);
                info.temp = getTemperature(i);
                info.slowDownTemp = getSlowDownTemperatureThreshold(i);
                info.shutdownTemp = getShutdownTemperatureThreshold(i);
                info.powerUsage = getPowerUsage(i);
                info.powerLimit = getPowerLimit(i);

                stringBuilder.append("\n|Index\t|Fan\t|Util\t|Mem(MB)\t|Temp(C)\t|Pow");
                stringBuilder.append("\n|").append(i)
                        .append("\t|")
                        .append(info.fanSpeed)
                        .append("\t|")
                        .append(String.format("%3d", info.utilRate)).append('%')
                        .append("\t|")
                        .append(String.format("%5d", info.usedMem / (1024 * 1024)))
                        .append("/").append(String.format("%5d", info.totalMem / (1024 * 1024)))
                        .append("\t|")
                        .append(info.temp).append("/").append(info.slowDownTemp).append("/").append(info.shutdownTemp)
                        .append("\t|")
                        .append(info.powerUsage).append("/").append(info.powerLimit);
            }
            logger.info(stringBuilder.toString());

            this.reportProducer.send(new ProducerRecord<>(REPORT_TOPIC, nodeName, new Gson().toJson(report)));

            try {
                sleep(10000);
            } catch (InterruptedException ignored) {
            }
        }
    }

    static {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(MonitorThread.class);
        try {
            logger.info("Loading native libraries for MonitorThread from "
                    + System.getProperty("java.library.path"));
            System.loadLibrary("CudaMonitor4j");
            logger.info("Native libraries for BasicTracker successfully loaded!");
        } catch (Throwable t) {
            logger.error("Failed to load native library for MonitorThread", t);
            throw t;
        }
    }
}

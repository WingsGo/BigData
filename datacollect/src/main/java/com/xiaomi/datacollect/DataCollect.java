package com.xiaomi.datacollect;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DataCollect {
    private static String LOG_DIR = "~/logs/";
    private static String LOG_PREFIX = "access.log.";
    private static String UPLOAD_DIR = "~/upload/";
    private static String BACKUP_DIR = "~/backup/";

    public static void main(String[] args) {
        Timer timer = new Timer();
        // 开启定时任务
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
                String day = sdf.format(new Date());

                File srcDir = new File(LOG_DIR);

                File[] listFiles = srcDir.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.startsWith(LOG_PREFIX);
                    }
                });
                Logger logger = Logger.getLogger("logRollingFile");
                logger.info("探测到如下文件需要采集: " + Arrays.toString(listFiles));


                try {

                    // 将文件移动到上传目录
                    File toUploadDir = new File(UPLOAD_DIR);
                    for (File file : listFiles) {
                        FileUtils.moveFileToDirectory(file, toUploadDir, true);
                    }
                    logger.info("如下文件移动到了上传目录: " + toUploadDir.getAbsolutePath());

                    FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration(), "root");
                    File[] toUploadFiles = toUploadDir.listFiles();

                    Path hdfsDstPath = new Path("/logs/" + day);
                    if (!fs.exists(hdfsDstPath)) {
                        fs.mkdirs(hdfsDstPath);
                    }

                    File backupDir = new File(BACKUP_DIR + day + "/");
                    if (backupDir.exists()) {
                        backupDir.mkdirs();
                    }

                    for (File file : toUploadFiles) {
                        Path dstPath = new Path("/logs/" + day + "/access_log_" + UUID.randomUUID() + ".log");
                        fs.copyFromLocalFile(new Path(file.getAbsolutePath()), dstPath);

                        logger.info("文件传输到hdfs完成: " + toUploadDir.getAbsolutePath() + "-->" + dstPath);

                        FileUtils.moveFileToDirectory(file, backupDir, true);

                        logger.info("文件备份完成: " + file.getAbsolutePath() + "-->" + backupDir);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 0, 60 * 60 * 1000L);
    }
}

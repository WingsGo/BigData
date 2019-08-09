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
    public static void main(String[] args) {
        Timer timer = new Timer();
        // 开启定时任务
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    //ConfigHolder config = ConfigHolder.getInstance();

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
                    String day = sdf.format(new Date());

                    File srcDir = new File(Constants.LOG_SOURCE_DIR);

                    File[] listFiles = srcDir.listFiles(new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            return name.startsWith(Constants.LOG_LEGAL_PREFIX);
                        }
                    });
                    Logger logger = Logger.getLogger("logRollingFile");
                    logger.info("探测到如下文件需要采集: " + Arrays.toString(listFiles));

                    // 将文件移动到上传目录
                    File toUploadDir = new File(Constants.LOG_TO_UPLOAD_DIR);
                    for (File file : listFiles) {
                        FileUtils.moveFileToDirectory(file, toUploadDir, true);
                    }
                    logger.info("如下文件移动到了上传目录: " + toUploadDir.getAbsolutePath());

                    FileSystem fs = FileSystem.get(new URI(Constants.HDFS_URI), new Configuration(), "root");
                    File[] toUploadFiles = toUploadDir.listFiles();

                    Path hdfsDstPath = new Path(Constants.HDFS_DST_BASE_DIR + day);
                    if (!fs.exists(hdfsDstPath)) {
                        fs.mkdirs(hdfsDstPath);
                    }

                    File backupDir = new File(Constants.LOG_BACKUP_BASE_DIR + day + "/");
                    if (backupDir.exists()) {
                        backupDir.mkdirs();
                    }

                    for (File file : toUploadFiles) {
                        Path dstPath = new Path(hdfsDstPath + Constants.HDFS_FILE_PREFIX + "/" + UUID.randomUUID() + Constants.HDFS_FILE_SUFFIX);
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

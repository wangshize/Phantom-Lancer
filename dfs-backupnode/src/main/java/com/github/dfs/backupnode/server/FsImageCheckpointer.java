package com.github.dfs.backupnode.server;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 持久化文件目录树到fsimage文件
 * @author wangsz
 * @create 2020-01-31
 **/
public class FsImageCheckpointer extends Thread {

    public static final Integer CHECKPONIT_INTERVAL = 60 * 60 * 1000;

    private static final String fsImagePath = "/Users/wangsz/SourceCode/editslog/";

    private BackupNode backupNode;
    private FSNamesystem namesystem;

    public FsImageCheckpointer(BackupNode backupNode, FSNamesystem namesystem) {
        this.backupNode = backupNode;
        this.namesystem = namesystem;
    }

    @Override
    public void run() {
        System.out.println("fsimage checkpoint 线程定时调度");
        while (backupNode.isRunning()) {
           try {
                Thread.sleep(CHECKPONIT_INTERVAL);
                FSImage fsImage = namesystem.getFSImageJson();
                String filePath = fsImagePath + "fsImage-" + fsImage.getMaxTxid() + ".meta";
                String fsImageJson = fsImage.getFsImageJson();
                if(fsImageJson == null || fsImageJson.length() == 0) {
                    continue;
                }
               try(RandomAccessFile file = new RandomAccessFile(filePath, "rw");
                   FileOutputStream fout = new FileOutputStream(file.getFD());
                   FileChannel logFileChannel = fout.getChannel()) {
                   ByteBuffer byteBuffer = ByteBuffer.wrap(fsImageJson.getBytes());
                   logFileChannel.write(byteBuffer);
                   //强制刷盘
                   logFileChannel.force(false);
               } catch (IOException e) {
                   throw e;
               }
           } catch (Exception e) {
               e.printStackTrace();
           }
        }
    }
}

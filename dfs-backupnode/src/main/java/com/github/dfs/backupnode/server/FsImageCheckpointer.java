package com.github.dfs.backupnode.server;

import java.io.File;
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

    public static final Integer CHECKPONIT_INTERVAL = 10 * 1000;

    private static final String fsImagePath = "/Users/wangsz/SourceCode/backupnode/";

    private BackupNode backupNode;
    private FSNamesystem namesystem;

    private String lastFSImageFile = fsImagePath + "empty.meta";

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
               String fsImageJson = fsImage.getFsImageJson();
               if(fsImageJson == null || fsImageJson.length() == 0) {
                   continue;
               }
               System.out.println("开始执行checkpoint操作");
               removeLastFSImageFile();
               writeFSImageFile(fsImage);
               uploadFSImageFile(fsImage);
           } catch (Exception e) {
               e.printStackTrace();
           }
        }
    }

    private void writeFSImageFile(FSImage fsImage) throws IOException {
        String fsImageJson = fsImage.getFsImageJson();
        String filePath = fsImagePath + "fsImage-" + fsImage.getMaxTxid() + ".meta";
        lastFSImageFile = fsImageJson;
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
    }

    private void removeLastFSImageFile() {
        File file = new File(lastFSImageFile);
        if(file.exists()) {
            file .delete();
        }
    }

    private void uploadFSImageFile(FSImage fsimage) throws Exception {
        FSImageUploader fsimageUploader = new FSImageUploader(fsimage);
        fsimageUploader.start();
    }
}

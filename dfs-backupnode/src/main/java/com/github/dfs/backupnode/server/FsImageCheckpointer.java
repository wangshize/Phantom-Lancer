package com.github.dfs.backupnode.server;

import com.github.dfs.namenode.rpc.model.CheckPointTxIdRequest;

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

    public static final Integer CHECKPONIT_INTERVAL = 30 * 1000;

    private static final String fsImagePath = "/Users/wangsz/SourceCode/backupnode/";

    private BackupNode backupNode;
    private FSNamesystem namesystem;
    private NameNodeRpcClient rpcClient;

    private String lastFSImageFile = fsImagePath + "empty.meta";

    public FsImageCheckpointer(BackupNode backupNode, FSNamesystem namesystem,
                               NameNodeRpcClient rpcClient) {
        this.backupNode = backupNode;
        this.namesystem = namesystem;
        this.rpcClient = rpcClient;
    }

    @Override
    public void run() {
        System.out.println("fsimage checkpoint 线程定时调度");
        while (backupNode.isRunning()) {
           try {
               Thread.sleep(CHECKPONIT_INTERVAL);
               if(!namesystem.isNameNodeRunning()) {
                   System.out.println("NameNode无法访问。。。。。。");
                   continue;
               }
               if(!namesystem.isDirChange()) {
                   System.out.println("文件目录树没有更新，不需要checkpoint。。。。。。");
                   continue;
               }
               FSImage fsImage = namesystem.getFSImageJson();
               String fsImageJson = fsImage.getFsImageJson();
               if(fsImageJson == null || fsImageJson.length() == 0) {
                   continue;
               }
               System.out.println("开始执行checkpoint操作");
               doCheck(fsImage);
               System.out.println("checkpoint完成，设置文件目录树标志为false。。。。。。");
               namesystem.resetDirChangeFlag();
           } catch (Exception e) {
               e.printStackTrace();
           }
        }
    }

    private void doCheck(FSImage fsImage) throws Exception {
        removeLastFSImageFile();
        writeFSImageFile(fsImage);
        uploadFSImageFile(fsImage);
        updateCheckPointTxId(fsImage);
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

    private void updateCheckPointTxId(FSImage fsImage) {
        rpcClient.updateCheckPointTxId(fsImage.getMaxTxid());
    }
}

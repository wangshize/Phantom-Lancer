package com.github.dfs.datanode.server;

import com.github.dfs.client.NioClient;
import com.github.dfs.common.entity.DataNodeInfo;
import com.github.dfs.common.entity.FileInfo;
import com.github.dfs.common.entity.RemoveReplicaTask;
import com.github.dfs.common.entity.ReplicateTask;

import java.io.File;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 副本复制管理组件
 * @author wangsz
 * @create 2020-03-01
 **/
public class ReplicateManager {

    private ConcurrentLinkedQueue<ReplicateTask> replicateTaskQueue =
            new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<RemoveReplicaTask> removeTaskQueue =
            new ConcurrentLinkedQueue<>();

    private NioClient nioClient;
    private NameNodeRpcClient nameNodeRpcClient;

    public ReplicateManager(NioClient nioClient, NameNodeRpcClient nameNodeRpcClient) {
        this.nioClient = nioClient;
        this.nameNodeRpcClient = nameNodeRpcClient;
        for (Integer i = 0; i < DataNodeConfig.REPLICATE_THREAD_NUM; i++) {
            new ReplicateWorker().start();
        }
        new RemoveReplicaWorker().start();
    }

    public void addReplicateTask(ReplicateTask replicateTask) {
        this.replicateTaskQueue.offer(replicateTask);
    }

    public void addRemoveReplicateTask(RemoveReplicaTask removeReplicaTask) {
        this.removeTaskQueue.offer(removeReplicaTask);
    }

    class ReplicateWorker extends Thread {

        @Override
        public void run() {
            while (true) {
                try {
                    ReplicateTask replicateTask = replicateTaskQueue.poll();
                    if(replicateTask == null) {
                        Thread.sleep(1000);
                        continue;
                    }
                    String fileName = replicateTask.getFilename();
                    long fileLength = replicateTask.getFileLength();
                    DataNodeInfo srcDataNode = replicateTask.getSrcDataNode();
                    String hostName = srcDataNode.getHostname();
                    int nioPort = srcDataNode.getNioPort();
                    System.out.println("接收到复制任务，向源节点：" + hostName + " 复制文件" + fileName);
                    byte[] file = nioClient.readFile(hostName, nioPort, fileName);
                    String absoluteFilename = FileUtiles.getAbsoluteFileName(fileName);
                    FileUtiles.saveFile(absoluteFilename, file);

                    FileInfo fileInfo = new FileInfo(fileName, fileLength);
                    nameNodeRpcClient.informReplicaReceived(fileInfo, DataNodeConfig.DATANODE_HOSTNAME, DataNodeConfig.DATANODE_IP);
                    nameNodeRpcClient.ackReBalance(fileName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    class RemoveReplicaWorker extends Thread {

        @Override
        public void run() {
            while (true) {
                try {
                    RemoveReplicaTask removeReplicaTask = removeTaskQueue.poll();
                    if(removeReplicaTask == null) {
                        Thread.sleep(1000);
                        continue;
                    }
                    String fileName = removeReplicaTask.getFilename();
                    String absoluteFilename = FileUtiles.getAbsoluteFileName(fileName);
                    System.out.println("接收到删除文件任务，删除文件:" + absoluteFilename);
                    File file = new File(absoluteFilename);
                    if(file.exists()) {
                        file.delete();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }
}

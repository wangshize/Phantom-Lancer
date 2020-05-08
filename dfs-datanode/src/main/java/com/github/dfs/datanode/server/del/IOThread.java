package com.github.dfs.datanode.server.del;

import com.github.dfs.common.entity.FileInfo;
import com.github.dfs.datanode.server.DataNodeConfig;
import com.github.dfs.datanode.server.NameNodeRpcClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author wangsz
 * @create 2020-03-08
 **/
public class IOThread extends Thread {

    public static final Integer SEND_FILE = 1;
    public static final Integer READ_FILE = 2;

    private NameNodeRpcClient nameNodeRpcClient;

    private NetWorkRequestQueue requestQueue = NetWorkRequestQueue.getInstance();

    public IOThread(NameNodeRpcClient nameNodeRpcClient) {
        this.nameNodeRpcClient = nameNodeRpcClient;
    }

    @Override
    public void run() {
        while (true) {
            try {
                NetWorkRequest request = requestQueue.poll();
                if(request == null) {
                    Thread.sleep(1000);
                    continue;
                }
                Integer requestType = request.getRequestType();
                if(SEND_FILE.equals(requestType)) {
                    saveFileToDIsk(request);
                } else if(READ_FILE.equals(requestType)) {
                    sendFileToClient(request);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void sendFileToClient(NetWorkRequest request) {
        NetWorkRequest.CachedRequest cachedRequest = request.getCachedRequest();
        NetWorkRequest.Filename fileName = cachedRequest.filename;
        File file = new File(fileName.absoluteFilename);
        if(!file.exists()) {
            throw new IllegalArgumentException(fileName + " not exist!");
        }
        try(FileInputStream imageIn = new FileInputStream(file);
            FileChannel imageChannel = imageIn.getChannel()) {

            //8个字节表示文件大小
            ByteBuffer outBuffer = ByteBuffer.allocate((int)file.length() + 8);
            outBuffer.putLong(file.length());
            imageChannel.read(outBuffer);
            outBuffer.rewind();

            offResponseToQueue(request, outBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void saveFileToDIsk(NetWorkRequest request) throws Exception {
        NetWorkRequest.CachedRequest cacheRequest = request.getCachedRequest();
        NetWorkRequest.Filename fileName = cacheRequest.filename;
        ByteBuffer fileBuffer = cacheRequest.fileBuffer;
        try(FileOutputStream imageOut = new FileOutputStream(fileName.absoluteFilename);
            FileChannel imageChannel = imageOut.getChannel()) {
            imageChannel.position(imageChannel.size());
            imageChannel.write(fileBuffer);
            System.out.println("文件读取完毕，返回响应给客户端");
            //增量上报给namenode，接收到的文件信息
            FileInfo fileInfo = new FileInfo(fileName.relativeFilename, cacheRequest.fileLength);
            nameNodeRpcClient.informReplicaReceived(fileInfo, DataNodeConfig.DATANODE_HOSTNAME, DataNodeConfig.DATANODE_IP);

            ByteBuffer responseBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
            offResponseToQueue(request, responseBuffer);
        }
    }

    private void offResponseToQueue(NetWorkRequest request, ByteBuffer responseBuffer) {
        NetWorkResponse response = new NetWorkResponse();
        response.setBuffer(responseBuffer);
        response.setClient(request.getClient());
        NetWorkResponseQueue responseQueues = NetWorkResponseQueue.getInstance();
        responseQueues.offer(request.getProcessId(), response);
    }

}

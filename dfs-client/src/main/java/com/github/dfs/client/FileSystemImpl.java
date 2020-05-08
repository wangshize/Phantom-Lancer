package com.github.dfs.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.github.dfs.namenode.rpc.model.*;
import com.github.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

import java.util.List;

/**
 * 文件系统客户端的实现类
 * @author wangsz
 * @create 2020-01-28
 **/
public class FileSystemImpl implements FileSystem {

    private static final String NAMENODE_HOSTNAME = "localhost";
    private static final Integer NAMENODE_PORT = 56789;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    private NioClient nioClient;

    public FileSystemImpl() {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
        this.nioClient = new NioClient();
    }

    @Override
    public void shutdown() {
        ShutdownRequest request = ShutdownRequest.newBuilder()
                .setCode(1)
                .build();
        namenode.shutdown(request);
    }

    @Override
    public void upload(byte[] file, String fileName, long fileSize,
                       ResponseCallBack callBack) throws Exception {
        //1、先向namenode节点创建一个文件目录路径
        //需要查重，如果存在了即不允许上传
        CreateFileRequest request = CreateFileRequest.newBuilder()
                .setFileName(fileName)
                .build();
        CreateFileResponse response = namenode.createFile(request);
        System.out.println(Thread.currentThread().getName() + "上传文件，查重创建文件结果 = " + response.getStatus());
        //2、找namenode要多个数据节点的地址，因为需要向多个数据节点上传数据
        //尽可能在分配数据节点的时候，保证每个数据节点的数据量是均衡的
        AllocateDataNodesRequest dataNodesRequest = AllocateDataNodesRequest.newBuilder()
                .setFileName(fileName)
                .setFileSize(fileSize)
                .build();
        AllocateDataNodesResponse allocateDataNodesResponse = namenode.allocateDataNodesFile(dataNodesRequest);
        String dataNodesJson = allocateDataNodesResponse.getDatanodes();
        System.out.println(dataNodesJson);
        //3、依次吧文件上传到数据节点，
        // 需要考虑如果上传过程中，某个节点上传失败的容错机制
        List<DataNodeInfo> datanodes = JSONArray.parseArray(dataNodesJson, DataNodeInfo.class);
        for (int i = 0; i < datanodes.size(); i++) {
            DataNodeInfo datanode = datanodes.get(i);
            boolean sendResult = sendFile(file, fileName, fileSize, datanode, callBack);
            if(!sendResult) {
                DataNodeInfo dataNodeInfo = reAllocateDataNode(datanode, fileSize);
                sendResult = sendFile(file, fileName, fileSize, dataNodeInfo, callBack);
                if(!sendResult) {
                    //重试一次，再失败就抛出异常
                    throw new Exception("send file fail......");
                }
            }
        }

    }

    private boolean sendFile(byte[] file, String fileName, long fileSize, DataNodeInfo datanode,
                             ResponseCallBack callBack) throws Exception {
        String hostName = datanode.getHostname();
        int nioPort = datanode.getNioPort();
        return nioClient.sendFile(hostName, nioPort, file, fileSize, fileName, callBack);
    }

    public DataNodeInfo reAllocateDataNode(DataNodeInfo excludedDataNode, long fileSize) {
        ReallocateDataNodeRequest request = ReallocateDataNodeRequest.newBuilder()
                .setFileSize(fileSize)
                .setExcludedHostName(excludedDataNode.getHostname())
                .setExcludedIp(excludedDataNode.getIp())
                .build();
        ReallocateDataNodeResponse response = namenode.reallocateDataNode(request);
        DataNodeInfo dataNodeInfo = JSON.parseObject(response.getDatanodeInfo(), DataNodeInfo.class);
        return dataNodeInfo;
    }

    @Override
    public byte[] download(String fileName) {
        //1、调用NameNode接口，获取文件所在的摸一个副本地址
        DataNodeInfo dataNodeInfo = getDataNodeForFile(fileName, null, -1);
        //2、通过数据节点地址建立连接，发送文件名
        //3、接收文件数据
        byte[] fileByte = new byte[0];
        try {
            String hostName = dataNodeInfo.getHostname();
            int nioPort = dataNodeInfo.getNioPort();
            fileByte = nioClient.readFile(hostName, nioPort, fileName);
        } catch (Exception e) {
            e.printStackTrace();
            String hostName = dataNodeInfo.getHostname();
            int nioPort = dataNodeInfo.getNioPort();
            DataNodeInfo newDataNodeInfo = getDataNodeForFile(fileName, hostName, nioPort);
            String newHostName = newDataNodeInfo.getHostname();
            int newNioPort = newDataNodeInfo.getNioPort();
            try {
                fileByte = nioClient.readFile(newHostName, newNioPort, fileName);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }

        return fileByte;
    }

    private DataNodeInfo getDataNodeForFile(String fileName, String hostName, int nioPort) {
        ChooseDataNodeForFileRequest request;
        request = ChooseDataNodeForFileRequest.newBuilder()
                .setFilename(fileName)
                .setExcludedHostName(hostName)
                .setExcludedNioPort(nioPort)
                .build();
        ChooseDataNodeForFileResponse response = namenode.chooseDataNodeForFile(request);
        String dataNodeInfoJson = response.getDatanodeInfo();
        DataNodeInfo dataNodeInfo = JSON.parseObject(dataNodeInfoJson, DataNodeInfo.class);
        return dataNodeInfo;
    }
}

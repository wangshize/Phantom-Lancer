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

    private NIOClient nioClient;

    public FileSystemImpl() {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
        this.nioClient = new NIOClient();
    }

    /**
     * 创建目录
     * @param path 文件路径
     */
    @Override
    public void mkdir(String path) {
        MkdirRequest request = MkdirRequest.newBuilder()
                .setPath(path)
                .build();
        MkdirResponse response = namenode.mkdir(request);
        System.out.println("创建目录的响应：" + response.getStatus());
    }

    @Override
    public void shutdown() {
        ShutdownRequest request = ShutdownRequest.newBuilder()
                .setCode(1)
                .build();
        namenode.shutdown(request);
    }

    @Override
    public void upload(byte[] file, String fileName, long fileSize) throws Exception {
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
            String hostName = datanode.getHostname();
            int nioPort = datanode.getNioPort();
            nioClient.sendFile(hostName, nioPort, file, fileSize, fileName);
        }

    }

    @Override
    public byte[] download(String fileName) {
        //1、调用NameNode接口，获取文件所在的摸一个副本地址
        GetDataNodeForFileRequest request = GetDataNodeForFileRequest.newBuilder()
                .setFilename(fileName)
                .build();
        GetDataNodeForFileResponse response = namenode.getDataNodeForFile(request);
        String dataNodeInfoJson = response.getDatanodeInfo();
        DataNodeInfo dataNodeInfo = JSON.parseObject(dataNodeInfoJson, DataNodeInfo.class);
        //2、通过数据节点地址建立连接，发送文件名
        //3、接收文件数据
        String hostName = dataNodeInfo.getHostname();
        int nioPort = dataNodeInfo.getNioPort();
        byte[] fileByte = nioClient.readFile(hostName, nioPort, fileName);
        return fileByte;
    }
}

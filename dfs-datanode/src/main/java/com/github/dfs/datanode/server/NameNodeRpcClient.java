package com.github.dfs.datanode.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.github.dfs.common.RegisterResult;
import com.github.dfs.common.entity.FileInfo;
import com.github.dfs.namenode.rpc.model.*;
import com.github.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

/**
 * 负责跟NameNode集群中的某一个进行通信的线程组件
 * @author zhonghuashishan
 *
 */
public class NameNodeRpcClient {
	
	private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;
	
	public NameNodeRpcClient() {
		ManagedChannel channel = NettyChannelBuilder
				.forAddress(DataNodeConfig.NAMENODE_HOSTNAME, DataNodeConfig.NAMENODE_PORT)
				.negotiationType(NegotiationType.PLAINTEXT)
				.build();
		this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
	}

	/**
	 * 向自己负责通信的那个NameNode进行注册
	 *
	 * 1:注册成功 2-重复注册
	 */
	public RegisterResult register() throws Exception {
		// 发送rpc接口调用请求到NameNode去进行注册
		System.out.println("发送RPC请求到NameNode进行注册.......");

		// 通过RPC接口发送到NameNode他的注册接口上去
		RegisterRequest request = RegisterRequest.newBuilder()
				.setIp(DataNodeConfig.DATANODE_IP)
				.setHostname(DataNodeConfig.DATANODE_HOSTNAME)
				.setNioPort(DataNodeConfig.NIO_PORT)
				.build();
		RegisterResponse response = namenode.register(request);
		RegisterResult registerResult = RegisterResult.fromCode(response.getStatus());
		System.out.println("接收到NameNode返回的注册响应：" + registerResult.getDesc());
		return registerResult;
	}

	/**
	 * 发送心跳
	 * @throws Exception
	 */
	public HeartbeatResponse heartbeat() {
		System.out.println("准备心跳续约：" + DataNodeConfig.DATANODE_HOSTNAME);
		HeartbeatRequest request = HeartbeatRequest.newBuilder()
				.setIp(DataNodeConfig.DATANODE_IP)
				.setHostname(DataNodeConfig.DATANODE_HOSTNAME)
				.setNioPort(DataNodeConfig.NIO_PORT)
				.build();
		return namenode.heartbeat(request);
	}

	/**
	 * 增量上报文件副本信息
	 * @param fileInfo
	 * @param hostName
	 * @param ip
	 */
	public void informReplicaReceived(FileInfo fileInfo, String hostName, String ip) {
        InformReplicaReceivedRequest replicaReceivedRequest = InformReplicaReceivedRequest.newBuilder()
                .setFileInfo(JSON.toJSONString(fileInfo))
				.setHostname(hostName)
				.setIp(ip)
                .build();
	    namenode.informReplicaReceived(replicaReceivedRequest);
    }

	/**
	 * 全量上报文件副本存储信息
	 */
	public void reportCompleteStorageInfo(StorageInfo storageInfo) {
		ReportCompleteStorageInfoRequest request = ReportCompleteStorageInfoRequest.newBuilder()
				.setFileInfo(JSONArray.toJSONString(storageInfo.getFileInfos()))
				.setStoredDataSize(storageInfo.getStoredDataSize())
				.setHostname(DataNodeConfig.DATANODE_HOSTNAME)
				.setIp(DataNodeConfig.DATANODE_IP)
				.build();
		namenode.reportCompleteStorageInfo(request);
	}

	public void ackReBalance(String fileName) {
		AckReBalanceRequest request = AckReBalanceRequest.newBuilder()
				.setFileName(fileName)
				.build();
		namenode.ackReBalance(request);
	}
}

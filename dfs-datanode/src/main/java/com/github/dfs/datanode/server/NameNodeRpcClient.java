package com.github.dfs.datanode.server;

import com.alibaba.fastjson.JSONArray;
import com.github.dfs.namenode.RegisterResult;
import com.github.dfs.namenode.rpc.model.*;
import com.github.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

import static com.github.dfs.datanode.server.DataNodeConfig.*;

/**
 * 负责跟NameNode集群中的某一个进行通信的线程组件
 * @author zhonghuashishan
 *
 */
public class NameNodeRpcClient {
	
	private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;
	
	public NameNodeRpcClient() {
		ManagedChannel channel = NettyChannelBuilder
				.forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
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
				.setIp(DATANODE_IP)
				.setHostname(DATANODE_HOSTNAME)
				.setNioPort(NIO_PORT)
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
		HeartbeatRequest request = HeartbeatRequest.newBuilder()
				.setIp(DATANODE_IP)
				.setHostname(DATANODE_HOSTNAME)
				.setNioPort(NIO_PORT)
				.build();
		return namenode.heartbeat(request);
	}

	/**
	 * 增量上报文件副本信息
	 * @param fileName
	 * @param hostName
	 * @param ip
	 */
	public void informReplicaReceived(String fileName, String hostName, String ip) {
        InformReplicaReceivedRequest replicaReceivedRequest = InformReplicaReceivedRequest.newBuilder()
                .setFilename(fileName)
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
				.setFilenames(JSONArray.toJSONString(storageInfo.getFileNames()))
				.setStoredDataSize(storageInfo.getStoredDataSize())
				.setHostname(DATANODE_HOSTNAME)
				.setIp(DATANODE_IP)
				.build();
		namenode.reportCompleteStorageInfo(request);
	}

	/**
	 * 负责心跳的线程
	 * @author zhonghuashishan
	 *
	 */
	class HeartbeatThread extends Thread {
		
		@Override
		public void run() {
			try {
				while(true) {
					System.out.println("发送RPC请求到NameNode进行心跳.......");  
					
					// 通过RPC接口发送到NameNode他的注册接口上去
					
					HeartbeatRequest request = HeartbeatRequest.newBuilder()
							.setIp(DATANODE_IP)
							.setHostname(DATANODE_HOSTNAME)
							.build();
					HeartbeatResponse response = namenode.heartbeat(request);
					System.out.println("接收到NameNode返回的心跳响应：" + response.getStatus());
					// 每隔30秒发送一次心跳到NameNode上去
					Thread.sleep(30 * 1000);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
}

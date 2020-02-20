package com.github.dfs.datanode.server;

import com.github.dfs.namenode.RegisterResult;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * DataNode启动类
 * @author zhonghuashishan
 *
 */
public class DataNode {

	/**
	 * 是否还在运行
	 */
	private volatile Boolean shouldRun;
	/**
	 * 负责跟一组NameNode通信的组件
	 */
	private NameNodeRpcClient nameNodeRpcClient;
	/**
	 * 心跳管理组件
	 */
	private HeartbeatManager heartbeatManager;
	/**
	 * 磁盘存储管理组件
	 */
	private StorageManager storageManager;
	
	/**
	 * 初始化DataNode
	 */
	private void initialize() throws Exception {
		this.shouldRun = true;
		this.nameNodeRpcClient = new NameNodeRpcClient();
		this.storageManager = new StorageManager();
		RegisterResult registerResult = this.nameNodeRpcClient.register();
		if(registerResult.equals(RegisterResult.FAIL)) {
			System.out.println("向NameNode注册失败，直接退出......");
			System.exit(1);
		} else if(registerResult.equals(RegisterResult.SUCCESS)) {
			//启动时全量上传文件副本信息
			StorageInfo storageInfo = storageManager.getStorageInfo();
			if(storageInfo != null) {
				this.nameNodeRpcClient.reportCompleteStorageInfo(storageInfo);
			}
		}
		this.heartbeatManager = new HeartbeatManager(
				this.nameNodeRpcClient, this.storageManager);
		this.heartbeatManager.start();
		DataNodeNIOServer nioServer = new DataNodeNIOServer(this.nameNodeRpcClient);
		nioServer.start();
	}
	
	/**
	 * 运行DataNode
	 */
	private void run() {
		try {
			while(shouldRun) {
				Thread.sleep(1000);  
			}   
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		DataNode datanode = new DataNode();
		datanode.initialize();
		datanode.run();

	}
	
}

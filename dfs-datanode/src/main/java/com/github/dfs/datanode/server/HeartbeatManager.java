package com.github.dfs.datanode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.dfs.namenode.Command;
import com.github.dfs.namenode.rpc.model.HeartbeatResponse;

import java.util.List;

/**
 * 心跳管理组件
 * @author zhonghuashishan
 *
 */
public class HeartbeatManager {

	private NameNodeRpcClient namenodeRpcClient;
	private StorageManager storageManager;
	
	public HeartbeatManager(NameNodeRpcClient namenodeRpcClient, 
			StorageManager storageManager) {
		this.namenodeRpcClient = namenodeRpcClient;
		this.storageManager = storageManager;
	}
	
	public void start() {
		new HeartbeatThread().start();
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
				System.out.println("定时心跳线程启动......");  
				
				while(true) {
					// 通过RPC接口发送到NameNode他的注册接口上去
					HeartbeatResponse response = namenodeRpcClient.heartbeat();
					
					// 如果心跳失败了
					if(response.getStatus() == 2) {
						List<Command> commands = JSONArray.parseArray(response.getCommands(), Command.class);
						
						for(int i = 0; i < commands.size(); i++) {
							Command command = commands.get(i);
							Integer type = command.getType();
							
							// 如果是注册的命令
							if(type.equals(1)) {
								namenodeRpcClient.register();
							} 
							// 如果是全量上报的命令
							else if(type.equals(2)) {
								StorageInfo storageInfo = storageManager.getStorageInfo();
								if(storageInfo != null) {
									namenodeRpcClient.reportCompleteStorageInfo(storageInfo);  
								}
							}
						}
					}
					// 每隔30秒发送一次心跳到NameNode上去
					Thread.sleep(30 * 1000);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
}
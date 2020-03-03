package com.github.dfs.datanode.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.github.dfs.common.Command;
import com.github.dfs.common.HeartbeatResult;
import com.github.dfs.common.entity.RemoveReplicaTask;
import com.github.dfs.common.entity.ReplicateTask;
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
	private ReplicateManager replicateManager;
	
	public HeartbeatManager(NameNodeRpcClient namenodeRpcClient, 
			StorageManager storageManager, ReplicateManager replicateManager) {
		this.namenodeRpcClient = namenodeRpcClient;
		this.storageManager = storageManager;
		this.replicateManager = replicateManager;
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
				System.out.println("定时心跳线程启动......");
				
				while(true) {
					try {
						// 通过RPC接口发送到NameNode他的注册接口上去
						HeartbeatResponse response = namenodeRpcClient.heartbeat();
						HeartbeatResult heartbeatResult = HeartbeatResult.fromCode(response.getStatus());
						if(heartbeatResult.equals(HeartbeatResult.SUCCESS)) {
							List<Command> commands = JSONArray.parseArray(response.getCommands(), Command.class);
							for (int i = 0; i < commands.size(); i++) {
								Command command = commands.get(i);
								int commandType = command.getType();
								if(Command.REPLICATE == commandType) {
									ReplicateTask replicateTask = JSON.parseObject(command.getContent(), ReplicateTask.class);
									replicateManager.addReplicateTask(replicateTask);
								} else if(Command.REMOVE == commandType) {
									RemoveReplicaTask removeTask = JSON.parseObject(command.getContent(), RemoveReplicaTask.class);
									replicateManager.addRemoveReplicateTask(removeTask);
								}
							}
						}
						// 如果心跳失败了
						if (heartbeatResult.equals(HeartbeatResult.FAIL)) {
							List<Command> commands = JSONArray.parseArray(response.getCommands(), Command.class);

							for (int i = 0; i < commands.size(); i++) {
								Command command = commands.get(i);
								Integer type = command.getType();

								// 如果是注册的命令
								if (type.equals(Command.REGISTER)) {
									System.out.println("datanode重新注册");
									namenodeRpcClient.register();
								}
								// 如果是全量上报的命令
								else if (type.equals(Command.REPORT_COMPLETE_STORAGE_INFO)) {
									System.out.println("全量上报文件信息");
									StorageInfo storageInfo = storageManager.getStorageInfo();
									if (storageInfo != null) {
										namenodeRpcClient.reportCompleteStorageInfo(storageInfo);
									}
								}
							}
						}
					} catch (Exception e) {
						System.out.println("心跳失败。。。。。。");
						e.printStackTrace();
					}
					// 每隔30秒发送一次心跳到NameNode上去
					try {
						Thread.sleep(30 * 1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
		}
		
	}
	
}
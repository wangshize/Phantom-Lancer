package com.github.dfs.namenode.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这个组件，就是负责管理集群里的所有的datanode的
 * @author zhonghuashishan
 *
 */
public class DataNodeManager {

	/**
	 * 内存中维护的datanode
	 */
	private Map<String, DataNodeInfo> dataNodes = new ConcurrentHashMap<String, DataNodeInfo>();

	private String applicationName(String ip, String hostname) {
		return ip + "-" + hostname;
	}

	public DataNodeManager() {
		new DataNodeAliveMonitor().start();
	}

	/**
	 * datanode进行注册
	 * @param ip 
	 * @param hostname
	 */
	public Boolean register(String ip, String hostname) {
		DataNodeInfo datanode = new DataNodeInfo(ip, hostname);
		dataNodes.put(applicationName(ip, hostname), datanode);
		return true;
	}

	public Boolean heartbeat(String ip, String hostname) {
		DataNodeInfo dataNodeInfo = dataNodes.get(applicationName(ip, hostname));
		if(dataNodeInfo == null) {

		}
		dataNodeInfo.setLatestHeartbeatTime(System.currentTimeMillis());
		return true;
	}

	/**
	 * 节点是否存活监控线程
	 */
	class DataNodeAliveMonitor extends Thread {

		@Override
		public void run() {
			try {
				while (true) {
					List<String> toRemoveDataNodes = new ArrayList<String>();
					//这一瞬间需要遍历的数据
					Iterator<DataNodeInfo> iterator = dataNodes.values().iterator();
					DataNodeInfo dataNode = null;
					while (iterator.hasNext()) {
						dataNode = iterator.next();
						if(System.currentTimeMillis() - dataNode.getLatestHeartbeatTime() > 90 * 1000) {
							toRemoveDataNodes.add(applicationName(dataNode.getIp(), dataNode.getHostname()));
						}
					}
					if(!toRemoveDataNodes.isEmpty()) {
						for (String toRemoveDataNode : toRemoveDataNodes) {
							dataNodes.remove(toRemoveDataNode);
						}
					}
					sleep(30 * 1000);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
}

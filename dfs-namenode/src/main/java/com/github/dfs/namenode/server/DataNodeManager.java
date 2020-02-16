package com.github.dfs.namenode.server;

import com.github.dfs.namenode.NameNodeConstants;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这个组件，就是负责管理集群里的所有的datanode的
 * @author zhonghuashishan
 *
 */
public class DataNodeManager {

	/**
	 * 集群中所有的datanode
	 */
	private Map<String, DataNodeInfo> datanodes =
			new ConcurrentHashMap<String, DataNodeInfo>();
	
	public DataNodeManager() {
		new DataNodeAliveMonitor().start();
	}
	
	/**
	 * datanode进行注册
	 * @param ip 
	 * @param hostname
	 */
	public Boolean register(String ip, String hostname, Integer nioPort) {
		DataNodeInfo datanode = new DataNodeInfo(ip, hostname, nioPort);
		datanodes.put(ip + "-" + hostname, datanode);  
		System.out.println("DataNode注册：ip=" + ip + ",hostname=" + hostname + ",nioPort=" + nioPort);
		return true;
	}

	/**
	 * 分配存储文件的datanode节点列表
	 * @param fileSize
	 * @return
	 */
	public List<DataNodeInfo> allocateDataNodes(long fileSize) {
		synchronized (this) {
			List<DataNodeInfo> dataNodeInfoList = new ArrayList<>(datanodes.values());
			Collections.sort(dataNodeInfoList);
			List<DataNodeInfo> selectedDataNodes = new ArrayList<>(NameNodeConstants.DUPLICATE_DATANODE_NUM);
			if(dataNodeInfoList.size() >= NameNodeConstants.DUPLICATE_DATANODE_NUM) {
				selectedDataNodes.addAll(dataNodeInfoList.subList(0, NameNodeConstants.DUPLICATE_DATANODE_NUM));
			} else {
				selectedDataNodes.addAll(dataNodeInfoList.subList(0, dataNodeInfoList.size()));
			}
			for (DataNodeInfo selectedDataNode : selectedDataNodes) {
				selectedDataNode.addStoredFileSize(fileSize);
			}
			return selectedDataNodes;
		}
	}
	
	/**
	 * datanode进行心跳
	 * @param ip
	 * @param hostname
	 * @return
	 */
	public Boolean heartbeat(String ip, String hostname) {
		DataNodeInfo datanode = datanodes.get(ip + "-" + hostname);
		datanode.setLatestHeartbeatTime(System.currentTimeMillis());  
		System.out.println("DataNode发送心跳：ip=" + ip + ",hostname=" + hostname);
		return true;
	}
	
	/**
	 * datanode是否存活的监控线程
	 * @author zhonghuashishan
	 *
	 */
	class DataNodeAliveMonitor extends Thread {
		
		@Override
		public void run() {
			try {
				while(true) {
					List<String> toRemoveDatanodes = new ArrayList<String>();
					
					Iterator<DataNodeInfo> datanodesIterator = datanodes.values().iterator();
					DataNodeInfo datanode = null;
					while(datanodesIterator.hasNext()) {
						datanode = datanodesIterator.next();
						if(System.currentTimeMillis() - datanode.getLatestHeartbeatTime() > 90 * 1000) {
							toRemoveDatanodes.add(datanode.getIp() + "-" + datanode.getHostname());
						}
					}
					
					if(!toRemoveDatanodes.isEmpty()) {
						for(String toRemoveDatanode : toRemoveDatanodes) {
							datanodes.remove(toRemoveDatanode);
						}
					}
					
					Thread.sleep(30 * 1000); 
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
}

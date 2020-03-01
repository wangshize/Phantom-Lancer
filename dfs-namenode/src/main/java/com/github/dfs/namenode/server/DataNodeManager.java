package com.github.dfs.namenode.server;

import com.github.dfs.namenode.NameNodeConstants;
import com.github.dfs.namenode.RegisterResult;

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
	public RegisterResult register(String ip, String hostname, Integer nioPort) {
		if(datanodes.containsKey(createDataNodeKey(ip, hostname))) {
			return RegisterResult.REPEAT;
		}
		DataNodeInfo datanode = new DataNodeInfo(ip, hostname, nioPort);
		datanodes.put(createDataNodeKey(ip, hostname), datanode);
		System.out.println("DataNode注册：ip=" + ip + ",hostname=" + hostname + ",nioPort=" + nioPort);
		return RegisterResult.SUCCESS;
	}

	private String createDataNodeKey(String ip, String hostname) {
		return ip + "-" + hostname;
	}

	public DataNodeInfo getDataNodeInfo(String ip, String hostname) {
		return datanodes.get(createDataNodeKey(ip, hostname));
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

	public DataNodeInfo reAllocateDataNode(long fileSize, String excludedIp, String excludedHostName) {
		synchronized(this) {
			// 先把排除掉的那个数据节点的存储的数据量减少文件的大小
			DataNodeInfo excludedDataNode = datanodes.get(createDataNodeKey(excludedIp, excludedHostName));
			if(excludedDataNode != null) {
				excludedDataNode.addStoredFileSize(-fileSize);
			}

			// 取出来所有的datanode，并且按照已经存储的数据大小来排序
			List<DataNodeInfo> datanodeList = new ArrayList<>();
			for(DataNodeInfo datanode : datanodes.values()) {
				if(!excludedDataNode.equals(datanode)) {
					datanodeList.add(datanode);
				}
			}
			Collections.sort(datanodeList);

			// 选择存储数据最少的头两个datanode出来
			DataNodeInfo selectedDatanode = null;
			if(datanodeList.size() >= 1) {
				selectedDatanode = datanodeList.get(0);
				datanodeList.get(0).addStoredFileSize(fileSize);
			}

			return selectedDatanode;
		}
	}
	
	/**
	 * datanode进行心跳
	 * @param ip
	 * @param hostname
	 * @return
	 */
	public Boolean heartbeat(String ip, String hostname) {
		DataNodeInfo datanode = datanodes.get(createDataNodeKey(ip, hostname));
		if(datanode == null) {
			//需要重新注册
			return false;
		}
		datanode.setLatestHeartbeatTime(System.currentTimeMillis());  
		System.out.println("DataNode发送心跳：ip=" + ip + ",hostname=" + hostname);
		return true;
	}

	public void setStoredDataSize(String ip, String hostname, long dataSize) {
		DataNodeInfo dataNodeInfo = datanodes.get(createDataNodeKey(ip, hostname));
		dataNodeInfo.setStoredDataSize(dataSize);
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
							toRemoveDatanodes.add(createDataNodeKey(datanode.getIp(), datanode.getHostname()));
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

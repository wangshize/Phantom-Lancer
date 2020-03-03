package com.github.dfs.namenode.server;

import com.github.dfs.common.NameNodeConstants;
import com.github.dfs.common.RegisterResult;
import com.github.dfs.common.entity.DataNodeInfo;
import com.github.dfs.common.entity.FileInfo;
import com.github.dfs.common.entity.ReplicateTask;

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

	private FSNamesystem namesystem;
	
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

	public void setNamesystem(FSNamesystem namesystem) {
		this.namesystem = namesystem;
	}

	/**
	 * 创建丢失副本的复制任务
	 */
	public void createLostReplicaTask(DataNodeInfo deadDatanode) {
		List<FileInfo> files = namesystem.getFileInfoByDataNode(
				deadDatanode.getDataNodeKey());
		if(files == null) {
			return;
		}
		for(FileInfo file : files) {
			String filename = file.getFileName();
			Long fileLength = file.getFileLength();
			List<DataNodeInfo> replicateDatanodes = allocateDataNodes(fileLength);
			if(replicateDatanodes != null && replicateDatanodes.size() >= 1) {
				//最终需要复制过去的目标节点
				DataNodeInfo destDatanode = replicateDatanodes.get(0);
				//复制文件的源头数据节点
				DataNodeInfo srcDatanode = namesystem.getReplicateSource(filename, deadDatanode);
				if(srcDatanode != null) {
					ReplicateTask replicateTask = new ReplicateTask(
							filename, fileLength, destDatanode, srcDatanode);
					srcDatanode.addReplicateTask(replicateTask);
				}

			}
		}
	}

	public void createReblanceReplicateTasks() {
		synchronized (this) {
			long totalStoreDataSize = 0;
			long averageStoreDataSize;
			for (DataNodeInfo dataNode : datanodes.values()) {
				totalStoreDataSize = dataNode.getStoredDataSize();
			}
			averageStoreDataSize = totalStoreDataSize / datanodes.size();
			List<DataNodeInfo> sourceDataNodes = new ArrayList<>();
			List<DataNodeInfo> destDataNodes = new ArrayList<>();
			for (DataNodeInfo dataNode : datanodes.values()) {
				if(dataNode.getStoredDataSize() > averageStoreDataSize) {
					sourceDataNodes.add(dataNode);
				}
				if(dataNode.getStoredDataSize() < averageStoreDataSize) {
					destDataNodes.add(dataNode);
				}
			}
			for (DataNodeInfo sourceDataNode : sourceDataNodes) {
				long toRemoveDataSize = sourceDataNode.getStoredDataSize() - averageStoreDataSize;
				for (DataNodeInfo destDataNode : destDataNodes) {
					if(destDataNode.getStoredDataSize() + toRemoveDataSize <= averageStoreDataSize) {
						//获取sourceDataNode节点里的文件，遍历文件，决定哪些文件要迁移过
						//对这些文件生成迁移/复制文件任务

						//生成文件删除任务

						break;
					} else if(destDataNode.getStoredDataSize() < averageStoreDataSize) {

					}
				}
			}
		}
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
					List<DataNodeInfo> toRemoveDatanodes = new ArrayList<DataNodeInfo>();
					
					Iterator<DataNodeInfo> datanodesIterator = datanodes.values().iterator();
					DataNodeInfo datanode = null;
					while(datanodesIterator.hasNext()) {
						datanode = datanodesIterator.next();
						if(System.currentTimeMillis() - datanode.getLatestHeartbeatTime() > 30 * 1000) {
							toRemoveDatanodes.add(datanode);
						}
					}
					
					if(!toRemoveDatanodes.isEmpty()) {
						for(DataNodeInfo toRemoveDatanode : toRemoveDatanodes) {
							//数据节点和文件副本的关系 FSNamesystem里的replicasByFilename
							//采用了惰性删除，也就是在读取文件的时候，根据文件名获取到数据节点，
							// 如果发现数据节点已经宕机了，那么此时再删除他们的对应关系
							//对于filesByDatanode的维护，为每个文件生成一个复制任务，将该节点上
							//的文件复制一份到其他节点上，保证每个文件至少有两份副本
							//考虑一个问题，复制过程中如果又重启了呢？需要为重启的那个节点生成删除任务
							createLostReplicaTask(toRemoveDatanode);
							datanodes.remove(toRemoveDatanode.getDataNodeKey());
							namesystem.removeDeadDataNode(toRemoveDatanode);

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

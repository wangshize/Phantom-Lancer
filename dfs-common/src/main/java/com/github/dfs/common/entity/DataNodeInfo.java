package com.github.dfs.common.entity;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 用来描述datanode的信息
 * @author zhonghuashishan
 *
 */
@EqualsAndHashCode
public class DataNodeInfo implements Comparable<DataNodeInfo> {

	private String ip;
	private String hostname;
	private Integer nioPort;
	private long latestHeartbeatTime;
	private boolean active;
	/**
	 * 存储的数据大小
	 */
	private long storedDataSize;

	/**
	 * 副本复制任务队列
	 */
	private ConcurrentLinkedQueue<ReplicateTask> replicateTaskQueue =
			new ConcurrentLinkedQueue<>();
	private ConcurrentLinkedQueue<RemoveReplicaTask> removeTaskQueue =
			new ConcurrentLinkedQueue<>();
  	
	public DataNodeInfo(String ip, String hostname, Integer nioPort) {
		this.ip = ip;
		this.hostname = hostname;
		this.nioPort = nioPort;
		this.latestHeartbeatTime = System.currentTimeMillis();
		this.storedDataSize = 0L;
	}

	@Override
	public int compareTo(DataNodeInfo o) {
		if(this.storedDataSize - o.getStoredDataSize() > 0) {
			return 1;
		} else if(this.storedDataSize - o.getStoredDataSize() < 0) {
			return -1;
		}
		return 0;
	}

	public String getDataNodeKey() {
		return ip + "-" + hostname;
	}

	public void addStoredFileSize(long fileSize) {
		this.storedDataSize += fileSize;
	}

	public void addReplicateTask(ReplicateTask replicateTask) {
		this.replicateTaskQueue.offer(replicateTask);
	}

	public void addRemoveTask(RemoveReplicaTask removeTask) {
		this.removeTaskQueue.offer(removeTask);
	}

	public ReplicateTask pollReplicateTask() {
		if(!replicateTaskQueue.isEmpty()) {
			return replicateTaskQueue.poll();
		}
		return null;
	}
	public RemoveReplicaTask pollRemoveTask() {
		if(!removeTaskQueue.isEmpty()) {
			return removeTaskQueue.poll();
		}
		return null;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public Integer getNioPort() {
		return nioPort;
	}

	public void setNioPort(Integer nioPort) {
		this.nioPort = nioPort;
	}

	public long getLatestHeartbeatTime() {
		return latestHeartbeatTime;
	}

	public void setLatestHeartbeatTime(long latestHeartbeatTime) {
		this.latestHeartbeatTime = latestHeartbeatTime;
	}

	public long getStoredDataSize() {
		return storedDataSize;
	}

	public void setStoredDataSize(long storedDataSize) {
		this.storedDataSize = storedDataSize;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	@Override
	public String toString() {
		return "DataNodeInfo{" +
				"hostname='" + hostname + '\'' +
				'}';
	}
}

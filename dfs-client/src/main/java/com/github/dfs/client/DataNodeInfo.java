package com.github.dfs.client;

import lombok.Getter;
import lombok.Setter;

/**
 * 用来描述datanode的信息
 * @author zhonghuashishan
 *
 */
@Getter@Setter
public class DataNodeInfo implements Comparable<DataNodeInfo> {

	private String ip;
	private String hostname;
	private Integer nioPort;
	private long latestHeartbeatTime;
	/**
	 * 存储的数据大小
	 */
	private long storedDataSize;
  	
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

	public void addStoredFileSize(long fileSize) {
		this.storedDataSize += fileSize;
	}

	@Override
	public String toString() {
		return "DataNodeInfo{" +
				"ip='" + ip + '\'' +
				", hostname='" + hostname + '\'' +
				", latestHeartbeatTime=" + latestHeartbeatTime +
				", storedDataSize=" + storedDataSize +
				'}';
	}
}
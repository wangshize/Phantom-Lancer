package com.github.dfs.namenode.server;

import java.util.ArrayList;
import java.util.List;

/**
 * 这个组件，就是负责管理集群里的所有的datanode的
 * @author zhonghuashishan
 *
 */
public class DataNodeManager {

	/**
	 * 内存中维护的datanode列表
	 */
	private List<DataNodeInfo> datanodes = new ArrayList<DataNodeInfo>();
	
	/**
	 * datanode进行注册
	 * @param ip 
	 * @param hostname
	 */
	public Boolean register(String ip, String hostname) {
		DataNodeInfo datanode = new DataNodeInfo(ip, hostname);
		datanodes.add(datanode);
		return true;
	}
	
}

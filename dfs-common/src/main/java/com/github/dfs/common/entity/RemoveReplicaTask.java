package com.github.dfs.common.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * 删除副本任务
 *
 */
@Getter
@Setter
public class RemoveReplicaTask {

	private String filename;
	private DataNodeInfo onRemoveDataNode;

	public RemoveReplicaTask(String filename, DataNodeInfo removeDataNode) {
		this.filename = filename;
		this.onRemoveDataNode = removeDataNode;
	}
	
}

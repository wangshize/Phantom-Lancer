package com.github.dfs.common.entity;

import lombok.ToString;

/**
 * 副本复制任务
 *
 */
@ToString
public class ReplicateTask {

	private String filename;
	private Long fileLength;
	private DataNodeInfo destDataNode;
	private DataNodeInfo srcDataNode;

	public ReplicateTask(String filename, Long fileLength, DataNodeInfo destDataNode, DataNodeInfo srcDataNode) {
		this.filename = filename;
		this.fileLength = fileLength;
		this.destDataNode = destDataNode;
		this.srcDataNode = srcDataNode;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public Long getFileLength() {
		return fileLength;
	}

	public void setFileLength(Long fileLength) {
		this.fileLength = fileLength;
	}

	public DataNodeInfo getDestDataNode() {
		return destDataNode;
	}

	public void setDestDataNode(DataNodeInfo destDataNode) {
		this.destDataNode = destDataNode;
	}

	public DataNodeInfo getSrcDataNode() {
		return srcDataNode;
	}

	public void setSrcDataNode(DataNodeInfo srcDataNode) {
		this.srcDataNode = srcDataNode;
	}
}

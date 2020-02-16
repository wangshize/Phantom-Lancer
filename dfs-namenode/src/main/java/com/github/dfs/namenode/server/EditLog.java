package com.github.dfs.namenode.server;

import lombok.Data;

/**
 * 代表了一条edits log
 * @author zhonghuashishan
 *
 */
@Data
public class EditLog {

	FileOP opration;
	long txid;
	String path;

	public static EditLog builder() {
		return new EditLog();
	}

	public EditLog opration(FileOP opration) {
		this.setOpration(opration);
		return this;
	}

	public EditLog txid(long txid) {
		this.setTxid(txid);
		return this;
	}

	public EditLog path(String path) {
		this.setPath(path);
		return this;
	}

	public EditLog build() {
		return this;
	}

	enum FileOP {
		MKDIR,
		REMOVE,
		CREATE;
	}
}
package com.github.dfs.backupnode.server;

public class EditLog {

	long txid;
	String path;
	String opration;

	public long getTxid() {
		return txid;
	}

	public void setTxid(long txid) {
		this.txid = txid;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getOpration() {
		return opration;
	}

	public void setOpration(String opration) {
		this.opration = opration;
	}
}
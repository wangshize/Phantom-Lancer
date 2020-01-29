package com.github.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;

/**
 * 代表了一条edits log
 * @author zhonghuashishan
 *
 */
public class EditLog {

	long txid;
	String content;

	public EditLog(long txid, String content) {
		this.txid = txid;
		JSONObject jsonObject = JSONObject.parseObject(content);
		jsonObject.put("txid", this.txid);
		this.content = jsonObject.toJSONString();
	}

	public long getTxid() {
		return txid;
	}

	public void setTxid(long txid) {
		this.txid = txid;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public String toString() {
		return "EditLog{" +
				"txid=" + txid +
				", content='" + content + '\'' +
				'}';
	}
}
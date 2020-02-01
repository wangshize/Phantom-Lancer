package com.github.dfs.backupnode.server;

/**
 * 负责管理元数据的核心组件
 * @author zhonghuashishan
 *
 */
public class FSNamesystem {

	/**
	 * 负责管理内存文件目录树的组件
	 */
	private FSDirectory directory;

	public FSNamesystem() {
		this.directory = new FSDirectory();
	}
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否成功
	 */
	public Boolean mkdir(long txid, String path) throws Exception {
		this.directory.mkdir(txid, path);
		return true;
	}

	public FSImage getFSImageJson() {
		return directory.getFSImage();
	}



}

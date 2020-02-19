package com.github.dfs.datanode.server;

import com.github.dfs.namenode.RegisterResult;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * DataNode启动类
 * @author zhonghuashishan
 *
 */
public class DataNode {

	/**
	 * 是否还在运行
	 */
	private volatile Boolean shouldRun;
	/**
	 * 负责跟一组NameNode通信的组件
	 */
	private NameNodeRpcClient nameNodeRpcClient;
	
	/**
	 * 初始化DataNode
	 */
	private void initialize() throws Exception {
		this.shouldRun = true;
		this.nameNodeRpcClient = new NameNodeRpcClient();
		RegisterResult registerResult = this.nameNodeRpcClient.register();
		this.nameNodeRpcClient.startHeartbeat();
		if(registerResult.equals(RegisterResult.SUCCESS)) {
			//启动时全量上传文件副本信息
			StorageInfo storageInfo = getStorageInfo();
			if(storageInfo != null) {
				this.nameNodeRpcClient.reportCompleteStorageInfo(storageInfo);
			}
		}

		DataNodeNIOServer nioServer = new DataNodeNIOServer(this.nameNodeRpcClient);
		nioServer.start();
	}

	private StorageInfo getStorageInfo() {
		File dataDir = new File(DataNodeConfig.DATANODE_FILE_PATH);
		List<File> allFile = scanFiles(dataDir);
		if(allFile == null || allFile.size() == 0) {
			return null;
		}
		StorageInfo storageInfo = new StorageInfo();
		List<String> fileList = new ArrayList<>(allFile.size());
		long storedDataSize = 0;
		for (File file : allFile) {
			String path = file.getPath();
			fileList.add(path.substring(0, DataNodeConfig.DATANODE_FILE_PATH.length()));
			storedDataSize += file.length();
		}
		storageInfo.setFileNames(fileList);
		storageInfo.setStoredDataSize(storedDataSize);
		return storageInfo;
	}

	private static List<File> scanFiles(File dir) {
		File[] children = dir.listFiles();
		List<File> fileList = new ArrayList<>();
		for (File file : children) {
			if(file.isDirectory()) {
				fileList.addAll(scanFiles(file));
			}
			if(file.isFile()) {
				fileList.add(file);
			}
		}
		return fileList;
	}
	
	/**
	 * 运行DataNode
	 */
	private void run() {
		try {
			while(shouldRun) {
				Thread.sleep(1000);  
			}   
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		DataNode datanode = new DataNode();
		datanode.initialize();
		datanode.run();

	}
	
}

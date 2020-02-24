package com.github.dfs.datanode.server;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 磁盘存储管理组件
 * @author zhonghuashishan
 *
 */
public class StorageManager {

	public StorageInfo getStorageInfo() {
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
			fileList.add(path.substring(DataNodeConfig.DATANODE_FILE_PATH.length() + 1));
			storedDataSize += file.length();
		}
		storageInfo.setFileNames(fileList);
		storageInfo.setStoredDataSize(storedDataSize);
		return storageInfo;
	}

	public List<File> scanFiles(File dir) {
		File[] children = dir.listFiles();
		List<File> fileList = new ArrayList<>();
		for (File file : children) {
			if(file.isDirectory()) {
				fileList.addAll(scanFiles(file));
			}
			if(file.isFile()) {
				if(file.getName().equals(".DS_Store")) {
					continue;
				}
				fileList.add(file);
			}
		}
		return fileList;
	}
	
}
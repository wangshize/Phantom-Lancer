package com.github.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;
import com.github.dfs.namenode.NameNodeConstants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

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
	/**
	 * 负责管理edits log写入磁盘的组件
	 */
	private FSEditlog editlog;
	
	public FSNamesystem() {
		this.directory = new FSDirectory();
		this.editlog = new FSEditlog();
		receoverDirectory();
	}
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否成功
	 */
	public Boolean mkdir(String path) throws Exception {
		this.directory.mkdir(path);
		EditLog log = EditLog.builder()
				.opration(EditLog.FileOP.MKDIR)
				.path(path)
				.build();
		this.editlog.logEdit(log);
		return true;
	}

    /**
     * 创建文件
     * @param fileName  文件名，包含绝对路径：/productes/iamge01.jpg
     * @return
     * @throws Exception
     */
	public Boolean create(String fileName) throws Exception {
        if(!directory.create(fileName)) {
            return false;
        }
        EditLog log = EditLog.builder()
				.opration(EditLog.FileOP.CREATE)
				.path(fileName)
				.build();
        this.editlog.logEdit(log);
	    return true;
    }

	public void updateCheckPointTxId(long checkPointTxId) {
		System.out.println("接收到checkpoint txid = " + checkPointTxId);
		editlog.setCheckPointTxId(checkPointTxId);
	}

	/**
	 * 恢复元数据
	 */
	public void receoverDirectory() {
		try {
			loadFSImage();
			loadEditsLog();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void loadFSImage() throws IOException {
		File fsImage = new File(NameNodeConstants.fsimageFilePath);
		if(!fsImage.exists()) {
			return;
		}
		try(FileInputStream fin = new FileInputStream(fsImage);
			FileChannel channel = fin.getChannel()) {

			//每次接收到fsimage文件都记录一下文件的大小，shutdown的时候一起持久化到磁盘
			//重启的时候读取出来，这样就可以动态分配大小了
			ByteBuffer byteBuffer = ByteBuffer.allocate(1024*1024);
			int count = channel.read(byteBuffer);
			byteBuffer.flip();
			String fsImageJson = new String(byteBuffer.array(), 0, count);
			FSDirectory.INode dirTree = JSONObject.parseObject(fsImageJson,
					FSDirectory.INode.class);
			directory.setDirTree(dirTree);
		}
	}

	private void loadEditsLog() throws IOException {
		long checkPointTxId = loadCheckPointTxId(NameNodeConstants.checkPointTxIdPath);
		File editsLogDir = new File(NameNodeConstants.editelogPath);
		File[] editsLogs = editsLogDir.listFiles();
		Arrays.sort(editsLogs);
		for (File editsLog : editsLogs) {
			if(!editsLog.isFile()) {
				continue;
			}
			String fileName = editsLog.getName();
			fileName = fileName.substring(0, fileName.lastIndexOf("."));
			String[] fileNameSplited = fileName.split("-");
			long endTxid = Long.valueOf(fileNameSplited[1]);
			if(endTxid > checkPointTxId) {
				String filePath = editsLog.getPath();
				List<String> edisLogs = Files.readAllLines(Paths.get(filePath),
						StandardCharsets.UTF_8);
				for (String edisLog : edisLogs) {
					EditLog log = JSONObject.parseObject(edisLog, EditLog.class);
					if(log.getTxid() > checkPointTxId) {
						System.out.println("恢复edits log日志，txid = " + log.getTxid());
						this.directory.mkdir(log.getPath());
					}
				}
			}
		}

	}

	private long loadCheckPointTxId(String checkPointTxIdPath) throws IOException {
		File fsImage = new File(checkPointTxIdPath);
		if(!fsImage.exists()) {
			return 1;
		}
		try(FileInputStream fin = new FileInputStream(checkPointTxIdPath);
			FileChannel channel = fin.getChannel()) {
			ByteBuffer byteBuffer = ByteBuffer.allocate(64);
			int count = channel.read(byteBuffer);
			byteBuffer.flip();
			return Long.valueOf(new String(byteBuffer.array(), 0, count));
		}
	}

	public void flushForce() {
		this.editlog.flushForce();
	}

	public FSEditlog getEditlog() {
		return editlog;
	}

}

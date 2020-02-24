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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

	/**
	 * 每个文件对应的副本所在的DataNode
	 */
	private Map<String, List<DataNodeInfo>> replicasByFilename =
			new ConcurrentHashMap<>();
	private ReadWriteLock replicasLock = new ReentrantReadWriteLock();
	private Lock replicasWriteLock = replicasLock.writeLock();
	private Lock replicasReadLock = replicasLock.readLock();

	private DataNodeManager dataNodeManager;

	public FSNamesystem(DataNodeManager dataNodeManager) {
		this.directory = new FSDirectory();
		this.editlog = new FSEditlog();
		this.dataNodeManager = dataNodeManager;
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

	/**
	 * 添加文件的相关信息 文件处于哪些datanode节点
	 * @param fileName
	 */
    public void addRecivedReplica(String fileName, String hostName, String ip) {
        try {
            replicasWriteLock.lock();
            List<DataNodeInfo> replicas = replicasByFilename.get(fileName);
            if(replicas == null) {
                replicas = new ArrayList<>();
                replicasByFilename.put(fileName, replicas);
            }
            DataNodeInfo dataNodeInfo = dataNodeManager.getDataNodeInfo(ip, hostName);
            replicas.add(dataNodeInfo);
            System.out.println("收到增量上报，当前副本信息为：" + replicasByFilename);
        } finally {
            replicasWriteLock.unlock();
        }
	}

    /**
     * 获取文件所在的数据节点信息
     * @param fileName
     * @return
     */
	public DataNodeInfo getDataNodeInfo(String fileName) {
	    try {
	        replicasReadLock.lock();
            List<DataNodeInfo> dataNodeInfos = replicasByFilename.get(fileName);
            if(dataNodeInfos == null || dataNodeInfos.size() == 0) {
                throw new IllegalArgumentException("文件不存在于任何数据节点");
            }
            Iterator<DataNodeInfo> infoIterator =  dataNodeInfos.iterator();
            //移除已经被下线的数据节点
            while (infoIterator.hasNext()) {
                DataNodeInfo dataNodeInfo = infoIterator.next();
                boolean isRemovedFromRegister = dataNodeManager.getDataNodeInfo(
                        dataNodeInfo.getIp(),
                        dataNodeInfo.getHostname()) == null;
                if(isRemovedFromRegister) {
                    infoIterator.remove();
                }
            }
            return selectedDataNode(dataNodeInfos);
        } finally {
	        replicasReadLock.unlock();
        }
    }

    private DataNodeInfo selectedDataNode(List<DataNodeInfo> dataNodeInfos) {
        int size = dataNodeInfos.size();
        Random random = new Random();
        int index = random.nextInt(size);
        return dataNodeInfos.get(index);
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

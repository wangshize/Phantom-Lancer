package com.github.dfs.namenode.server;

import com.alibaba.fastjson.JSONObject;
import com.github.dfs.common.NameNodeConstants;
import com.github.dfs.common.entity.DataNodeInfo;
import com.github.dfs.common.entity.FileInfo;
import com.github.dfs.common.entity.RemoveReplicaTask;

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
import java.util.stream.Collectors;

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
	/**
	 * 每个文件对应的副本所在的DataNode
	 */
	private Map<String, List<FileInfo>> filesByDataNode =
			new ConcurrentHashMap<>();
	private ReadWriteLock replicasLock = new ReentrantReadWriteLock();
	private Lock replicasWriteLock = replicasLock.writeLock();
	private Lock replicasReadLock = replicasLock.readLock();

	private final static int REPLICATE_NUM = 2;

	private DataNodeManager dataNodeManager;

	public FSNamesystem(DataNodeManager dataNodeManager) {
		this.directory = new FSDirectory();
		this.editlog = new FSEditlog();
		this.dataNodeManager = dataNodeManager;
		receoverDirectory();
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
	 * @param fileInfo
	 */
    public void addRecivedReplica(FileInfo fileInfo, String hostName, String ip) {
		replicasWriteLock.lock();
		try {
            String fileName = fileInfo.getFileName();
            //维护每个文件副本所在的数据节点
			DataNodeInfo dataNodeInfo = dataNodeManager.getDataNodeInfo(ip, hostName);
			List<DataNodeInfo> replicas = replicasByFilename.get(fileName);
			if(replicas == null) {
				replicas = new ArrayList<>();
				replicasByFilename.put(fileName, replicas);
			}
			//移除已经下线的数据节点信息
//			removeDeadDataNode(replicas);

			//减少节点的存储数据量，保证每份文件的副本个数固定
			if(replicas.size() == REPLICATE_NUM) {
				System.out.println("文件副本超过" + REPLICATE_NUM + "个，需要生成删除副本任务") ;
				RemoveReplicaTask removeReplicaTask = new RemoveReplicaTask(fileName, dataNodeInfo);
				dataNodeInfo.addRemoveTask(removeReplicaTask);
				return;
			}
            replicas.add(dataNodeInfo);
            //维护每个数据节点拥有的文件副本
			String dataNodeKey = ip + "-" + hostName;
			List<FileInfo> files = filesByDataNode.get(dataNodeKey);
			if(files == null) {
				files = new ArrayList<>();
					filesByDataNode.put(dataNodeKey, files);
			}
			files.add(fileInfo);
            System.out.println("收到存储上报，当前副本信息为：" + replicasByFilename
				+ "," + filesByDataNode);
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
		return getDataNodeInfo(fileName, null, -1);
	}

	public DataNodeInfo getDataNodeInfo(String fileName, String excludedHostName, Integer excludedNioPort) {
		try {
			replicasReadLock.lock();
			List<DataNodeInfo> dataNodeInfos = replicasByFilename.get(fileName);
			if(dataNodeInfos == null || dataNodeInfos.size() == 0) {
				throw new IllegalArgumentException("文件不存在于任何数据节点");
			}
//			removeDeadDataNode(dataNodeInfos);
			List<DataNodeInfo> filterList = dataNodeInfos;
			if(excludedHostName != null && excludedNioPort != -1) {
				filterList = dataNodeInfos.stream()
						.filter(dataNodeInfo -> {
							return !dataNodeInfo.getNioPort().equals(excludedNioPort)
									&& !dataNodeInfo.getHostname().equals(excludedHostName);
						}).collect(Collectors.toList());
			}
			return selectedDataNode(filterList);
		} finally {
			replicasReadLock.unlock();
		}
	}

	private void removeDeadDataNode(List<DataNodeInfo> dataNodeInfos) {
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

	public List<FileInfo> getFileInfoByDataNode(String dataNodeKey) {
		try {
			replicasReadLock.lock();
			return filesByDataNode.get(dataNodeKey);
		} finally {
			replicasReadLock.unlock();
		}
	}

	public void removeReplicaFromDataNode(DataNodeInfo toRemoveNode, FileInfo fileInfo) {
		try {
			replicasWriteLock.lock();
			String dataNodeKey = toRemoveNode.getDataNodeKey();
			String fileName = fileInfo.getFileName();
			filesByDataNode.get(dataNodeKey)
					.remove(fileName);
			Iterator<DataNodeInfo> dataNodeIt = replicasByFilename.get(fileName).iterator();
			while (dataNodeIt.hasNext()) {
				DataNodeInfo dataNodeInfo = dataNodeIt.next();
				if(dataNodeInfo.getDataNodeKey().equals(dataNodeKey)) {
					dataNodeIt.remove();
				}
			}

		} finally {
			replicasWriteLock.unlock();
		}
	}

	public DataNodeInfo getReplicateSource(String fileName, DataNodeInfo deadDataNode) {
		try {
			replicasReadLock.lock();
			List<DataNodeInfo> dataNodeInfos = replicasByFilename.get(fileName);
			if(dataNodeInfos != null && dataNodeInfos.size() > 0) {
				for (DataNodeInfo dataNodeInfo : dataNodeInfos) {
					if(!deadDataNode.getDataNodeKey().equals(dataNodeInfo.getDataNodeKey())) {
						return dataNodeInfo;
					}
				}
			}
		} finally {
			replicasReadLock.unlock();
		}
		return null;
	}

	/**
	 * 删除数据节点的文件副本的数据结构
	 */
	public void removeDeadDataNode(DataNodeInfo deadDataNode) {
		try {
			replicasWriteLock.lock();
			List<FileInfo> fileInfoList = filesByDataNode.get(deadDataNode.getDataNodeKey());
			if(fileInfoList != null) {
				for (FileInfo fileInfo : fileInfoList) {
					List<DataNodeInfo> fileDataNodes = replicasByFilename.get(fileInfo.getFileName());
					fileDataNodes.remove(deadDataNode);
				}
			}
			filesByDataNode.remove(deadDataNode.getDataNodeKey());
		} finally {
			replicasWriteLock.unlock();
		}
	}
}

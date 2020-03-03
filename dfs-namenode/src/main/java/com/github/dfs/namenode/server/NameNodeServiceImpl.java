package com.github.dfs.namenode.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.dfs.common.Command;
import com.github.dfs.common.HeartbeatResult;
import com.github.dfs.common.RegisterResult;
import com.github.dfs.common.entity.DataNodeInfo;
import com.github.dfs.common.entity.FileInfo;
import com.github.dfs.common.entity.RemoveReplicaTask;
import com.github.dfs.common.entity.ReplicateTask;
import com.github.dfs.namenode.rpc.model.*;
import com.github.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * NameNode的rpc服务的接口
 * @author zhonghuashishan
 *
 */
public class NameNodeServiceImpl extends NameNodeServiceGrpc.NameNodeServiceImplBase {

	public static final Integer STATUS_SUCCESS = 1;
	public static final Integer STATUS_FAILURE = 2;
	public static final Integer STATUS_SHUTDOWN = 3;

	private static final String editelogPath = "/Users/wangsz/SourceCode/editslog/";

	/**
	 * 负责管理元数据的核心组件
	 */
	private FSNamesystem namesystem;
	/**
	 * 负责管理集群中所有的datanode的组件
	 */
	private DataNodeManager datanodeManager;

	private volatile boolean isRunning = true;


	public NameNodeServiceImpl(
			FSNamesystem namesystem, 
			DataNodeManager datanodeManager) {
		this.namesystem = namesystem;
		this.datanodeManager = datanodeManager;
	}
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否创建成功
	 * @throws Exception
	 */
	public Boolean mkdir(String path) throws Exception {
		return this.namesystem.mkdir(path);
	}

	/**
	 * datanode进行注册
	 * @return
	 * @throws Exception
	 */
	@Override
	public void register(RegisterRequest request, 
			StreamObserver<RegisterResponse> responseObserver) {
		RegisterResult registerResult = datanodeManager.register(request.getIp(), request.getHostname(), request.getNioPort());
		System.out.println("注册结果：" + registerResult.getDesc());
		RegisterResponse response = RegisterResponse.newBuilder()
				.setStatus(registerResult.getStatus())
				.build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * datanode进行心跳
	 * @return
	 * @throws Exception
	 */
	@Override
	public void heartbeat(HeartbeatRequest request, 
			StreamObserver<HeartbeatResponse> responseObserver) {
		String ip = request.getIp();
		String hostName = request.getHostname();
		boolean result = datanodeManager.heartbeat(ip, hostName);
		List<Command> commands = new ArrayList<Command>();
		HeartbeatResponse response = null;
		if(result) {
			DataNodeInfo dataNodeInfo = datanodeManager.getDataNodeInfo(ip, hostName);
			ReplicateTask replicateTask;
			while ((replicateTask = dataNodeInfo.pollReplicateTask()) != null) {
				Command replicateCommand = new Command(Command.REPLICATE);
				replicateCommand.setContent(JSON.toJSONString(replicateTask));
				commands.add(replicateCommand);
				//为了防止响应报文太大，允许一次心跳最多带回500个复制任务
				if(commands.size() >= 500) {
					break;
				}
			}
			RemoveReplicaTask removeReplicaTask;
			while ((removeReplicaTask = dataNodeInfo.pollRemoveTask()) != null) {
				Command removeCommand = new Command(Command.REMOVE);
				removeCommand.setContent(JSON.toJSONString(removeReplicaTask));
				commands.add(removeCommand);
				//为了防止响应报文太大，允许一次心跳最多带回500个复制任务
				if(commands.size() >= 500) {
					break;
				}
			}

			response = HeartbeatResponse.newBuilder()
					.setStatus(HeartbeatResult.SUCCESS.getStatus())
					.setCommands(JSONArray.toJSONString(commands))
					.build();
		} else {
			System.out.println("心跳失败，找不到对应实例，指示datanode执行重新注册和全量上报命令");
			Command registerCommand = new Command(Command.REGISTER);
			Command reportCompleteStorageInfoCommand = new Command(
					Command.REPORT_COMPLETE_STORAGE_INFO);
			commands.add(registerCommand);
			commands.add(reportCompleteStorageInfoCommand);

			response = HeartbeatResponse.newBuilder()
					.setStatus(HeartbeatResult.FAIL.getStatus())
					.setCommands(JSONArray.toJSONString(commands))
					.build();
		}

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * 创建目录
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void mkdir(MkdirRequest request, StreamObserver<MkdirResponse> responseObserver) {
		try {
			MkdirResponse response = null;
			if(!isRunning) {
				response = MkdirResponse.newBuilder()
						.setStatus(STATUS_SHUTDOWN)
						.build();
			} else {
				this.namesystem.mkdir(request.getPath());
				System.out.println("创建目录：path = " + request.getPath());
				response = MkdirResponse.newBuilder()
						.setStatus(STATUS_SUCCESS)
						.build();
			}

			responseObserver.onNext(response);
			responseObserver.onCompleted();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void createFile(CreateFileRequest request, StreamObserver<CreateFileResponse> responseObserver) {
		String fileName = request.getFileName();
		//先查重后创建
		//多线程情况下，查重和创建必须在一个同步代码块中
		int status;
		try {
			if(namesystem.create(fileName)){
				status = 1;
			} else {
				status =2;
			}
		} catch (Exception e) {
			e.printStackTrace();
			status = 3;
		}
		CreateFileResponse response = CreateFileResponse.newBuilder()
				.setStatus(status)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * 优雅关闭
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
		System.out.println("正在关闭namenode......");
		this.isRunning = false;
		this.namesystem.flushForce();
		//save checkPointTxId
		this.namesystem.getEditlog().saveCheckPointTxId();
		ShutdownResponse response = ShutdownResponse.newBuilder()
				.setStatus(STATUS_SUCCESS)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * 同步editslog
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void fetchEditsLog(FetchEditsLogRequest request, StreamObserver<FetchEditsLogResponse> responseObserver) {
		if(!isRunning) {
			FetchEditsLogResponse response = FetchEditsLogResponse.newBuilder()
					.setStatus(STATUS_SHUTDOWN)
					.build();

			responseObserver.onNext(response);
			responseObserver.onCompleted();
		}
		long expectBeginTxid = request.getEditsLogTxId();
		System.out.println("期望txid = " + expectBeginTxid + " 开始拉取数据");
		int expectFetchSize = request.getExpectFetchSize();
		FetchEditsLogResponse response = null;
		JSONArray fetchedEditsLog = new JSONArray();

		List<FlushedFileMapper> txidFileMappers = namesystem.getEditlog().getTxidFileMapper();
		//表示此时没有任何数据写入磁盘
		if(txidFileMappers.isEmpty()) {
			List<String> bufferedEditsLog = namesystem.getEditlog().getBufferEdisLog();
			fullFetchedEditLog(fetchedEditsLog, bufferedEditsLog, expectBeginTxid, expectFetchSize);
		} else {
			boolean isTxidOnFile = false;
			String filePath = null;
			int mapperIndex = 0;
			long fetchBeginTxid = expectBeginTxid + 1;
			for (FlushedFileMapper txidFileMapper : txidFileMappers) {
				isTxidOnFile = txidFileMapper.isBetween(fetchBeginTxid);
				if(isTxidOnFile) {
					filePath = txidFileMapper.getFilePath();
					break;
				}
				mapperIndex++;
			}
			//情况1、拉取的txid在某个磁盘文件
			if(isTxidOnFile) {
				//一个edits log文件包含的数据足够本次拉取
				try {
					List<String> editsLogs = Files.readAllLines(Paths.get(filePath), StandardCharsets.UTF_8);
					expectBeginTxid = fullFetchedEditLog(fetchedEditsLog, editsLogs, expectBeginTxid, expectFetchSize);
					//是否需要继续拉取下一个文件的数据
					int fetchCount = fetchedEditsLog.size();
					while (fetchCount < expectFetchSize) {
						if(txidFileMappers.size() >= mapperIndex + 1) {
							//已经没有更多落盘的eitslog文件
							break;
						}
						FlushedFileMapper nextMapper = txidFileMappers.get(++mapperIndex);
						String nextFilePath = nextMapper.getFilePath();
						editsLogs = Files.readAllLines(Paths.get(nextFilePath), StandardCharsets.UTF_8);
						expectBeginTxid = fullFetchedEditLog(fetchedEditsLog, editsLogs, expectBeginTxid, expectFetchSize);
						fetchCount = fetchedEditsLog.size();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			//情况2、拉取的txid已经比磁盘文件里的全部都新，还在内存缓冲
			int fetchCount = fetchedEditsLog.size();
			if(fetchCount < expectFetchSize) {
				List<String> bufferedEditsLog = namesystem.getEditlog().getBufferEdisLog();
				fullFetchedEditLog(fetchedEditsLog, bufferedEditsLog, expectBeginTxid, expectFetchSize);
			}
		}
		response = FetchEditsLogResponse.newBuilder()
				.setEditsLog(fetchedEditsLog.toJSONString())
				.build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void updateCheckPointTxId(CheckPointTxIdRequest request, StreamObserver<CheckPointTxIdResponse> responseObserver) {

		long checkPointTxId = request.getTxId();
		namesystem.updateCheckPointTxId(checkPointTxId);

		CheckPointTxIdResponse response = CheckPointTxIdResponse.newBuilder()
				.setCode(STATUS_SUCCESS)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * 为文件上传请求分配数据节点，来传输多个副本
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void allocateDataNodesFile(AllocateDataNodesRequest request, StreamObserver<AllocateDataNodesResponse> responseObserver) {
		//取出所有datanode，选择数据量最少的两个datanode，将文件上传到这两个节点
		long fileSize = request.getFileSize();
		List<DataNodeInfo> datanodes = datanodeManager.allocateDataNodes(fileSize);

		AllocateDataNodesResponse response = AllocateDataNodesResponse.newBuilder()
				.setDatanodes(JSONObject.toJSONString(datanodes))
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void reallocateDataNode(ReallocateDataNodeRequest request, StreamObserver<ReallocateDataNodeResponse> responseObserver) {
		long fileSize = request.getFileSize();
		String excludedHostName = request.getExcludedHostName();
		String excludedIp = request.getExcludedIp();
		DataNodeInfo datanode = datanodeManager.reAllocateDataNode(fileSize, excludedIp, excludedHostName);

		ReallocateDataNodeResponse response = ReallocateDataNodeResponse.newBuilder()
				.setDatanodeInfo(JSONObject.toJSONString(datanode))
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
     * 数据节点通知接收到的文件信息
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void informReplicaReceived(InformReplicaReceivedRequest request, StreamObserver<InformReplicaReceivedResponse> responseObserver) {
		String fileInfoJson = request.getFileInfo();
		FileInfo fileInfo = JSON.parseObject(fileInfoJson, FileInfo.class);
		String hostName = request.getHostname();
		String ip = request.getIp();

		//处理文件副本信息
        namesystem.addRecivedReplica(fileInfo, hostName, ip);

		InformReplicaReceivedResponse response = InformReplicaReceivedResponse.newBuilder()
				.setStatus(STATUS_SUCCESS)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * 全量上报文件存储信息
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void reportCompleteStorageInfo(ReportCompleteStorageInfoRequest request, StreamObserver<ReportCompleteStorageInfoResponse> responseObserver) {
		String ip = request.getIp();
		String hostName = request.getHostname();
		String fileInfosJson = request.getFileInfo();
		long storedDataSIze = request.getStoredDataSize();

		datanodeManager.setStoredDataSize(ip, hostName, storedDataSIze);
		List<FileInfo> fileNameList = JSONObject.parseArray(fileInfosJson, FileInfo.class);
		for (FileInfo fileInfo : fileNameList) {
			namesystem.addRecivedReplica(fileInfo, hostName, ip);
		}
		ReportCompleteStorageInfoResponse response = ReportCompleteStorageInfoResponse
				.newBuilder()
				.setStatus(STATUS_SUCCESS)
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void chooseDataNodeForFile(ChooseDataNodeForFileRequest request, StreamObserver<ChooseDataNodeForFileResponse> responseObserver) {
		String fileName = request.getFilename();
		String excludedHostName = request.getExcludedHostName();
		int excludedNioPort = request.getExcludedNioPort();
		DataNodeInfo dataNodeInfo = namesystem.getDataNodeInfo(fileName, excludedHostName, excludedNioPort);

		ChooseDataNodeForFileResponse response = ChooseDataNodeForFileResponse.newBuilder()
				.setDatanodeInfo(JSONArray.toJSONString(dataNodeInfo))
				.build();
		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	/**
	 * 集群数据重平衡
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void reBalance(ReBalanceRequest request, StreamObserver<ReBalanceResponse> responseObserver) {

	}

	/**
	 * 填充最终需要返回的editlog数据
	 * @param fetchedEditsLog
	 * @param editsLogs
	 * @param expectBeginTxid
	 * @return 本次填充进来的最大txid
	 */
	private long fullFetchedEditLog(JSONArray fetchedEditsLog, List<String> editsLogs,
									long expectBeginTxid, int expectFetchSize) {
		long fetchTxid = expectBeginTxid;
		JSONArray currentBufferedEditsLog = new JSONArray();
		for (String editsLog : editsLogs) {
			if(editsLog.length() > 0) {
				currentBufferedEditsLog.add(JSONObject.parseObject(editsLog));
			}
		}
		int fetchCount = fetchedEditsLog.size();

		// 此时就可以从刚刚内存缓冲里的数据开始取数据了
		int fetchSize = Math.min(expectFetchSize, currentBufferedEditsLog.size());

		for (int i = 0; i < currentBufferedEditsLog.size(); i++) {
			if (currentBufferedEditsLog.getJSONObject(i).getLong("txid") >= fetchTxid) {
				fetchedEditsLog.add(currentBufferedEditsLog.getJSONObject(i));
				fetchTxid = currentBufferedEditsLog.getJSONObject(i).getLong("txid");
				fetchCount++;
			}
			if (fetchCount == fetchSize) {
				break;
			}
		}
		return fetchTxid;
	}
}

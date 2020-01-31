package com.github.dfs.namenode.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.dfs.namenode.rpc.model.*;
import com.github.dfs.namenode.rpc.service.NameNodeServiceGrpc;

import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
		datanodeManager.register(request.getIp(), request.getHostname());
		
		RegisterResponse response = RegisterResponse.newBuilder()
				.setStatus(STATUS_SUCCESS)
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
		datanodeManager.heartbeat(request.getIp(), request.getHostname());

		HeartbeatResponse response = HeartbeatResponse.newBuilder()
				.setStatus(STATUS_SUCCESS)
				.build();
	
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

	/**
	 * 优雅关闭
	 * @param request
	 * @param responseObserver
	 */
	@Override
	public void shutdown(ShutdownRequest request, StreamObserver<ShutdownResponse> responseObserver) {
		this.isRunning = false;
		this.namesystem.flushForce();
		System.out.println("正在关闭namenode完成");
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

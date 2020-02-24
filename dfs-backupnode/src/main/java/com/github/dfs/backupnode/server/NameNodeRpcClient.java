package com.github.dfs.backupnode.server;

import com.alibaba.fastjson.JSON;
import com.github.dfs.namenode.rpc.model.CheckPointTxIdRequest;
import com.github.dfs.namenode.rpc.model.FetchEditsLogRequest;
import com.github.dfs.namenode.rpc.model.FetchEditsLogResponse;
import com.github.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

import java.util.Collections;

/**
 * @author wangsz
 * @create 2020-01-29
 **/
public class NameNodeRpcClient {

    private static final String NAMENODE_HOSTNAME = "localhost";
    private static final Integer NAMENODE_PORT = 56789;

    public static final Integer BACKUP_NODE_FETCH_SIZE = 100;

    public static final Integer STATUS_SUCCESS = 1;
    public static final Integer STATUS_FAILURE = 2;
    public static final Integer STATUS_SHUTDOWN = 3;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public NameNodeRpcClient() {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    public FetchEditsLogResult fetchEditsLog(Long fetchedTxIdBegin) {
        FetchEditsLogResult result = new FetchEditsLogResult();
        result.setStatus(STATUS_SUCCESS);
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder()
                .setEditsLogTxId(fetchedTxIdBegin)
                .setExpectFetchSize(BACKUP_NODE_FETCH_SIZE)
                .build();
        FetchEditsLogResponse response = namenode.fetchEditsLog(request);
        if(response.getStatus() == STATUS_SHUTDOWN) {
            result.setStatus(STATUS_SHUTDOWN);
            result.setEditLogs(Collections.emptyList());
            return result;
        }
        String content = response.getEditsLog();
        if(content.length() == 0) {
            result.setEditLogs(Collections.emptyList());
            return result;
        }
        result.setEditLogs(JSON.parseArray(content, EditLog.class));
        return result;
    }

    public void updateCheckPointTxId(long updateTxId) {
        CheckPointTxIdRequest request = CheckPointTxIdRequest.newBuilder()
                .setTxId(updateTxId)
                .build();
        namenode.updateCheckPointTxId(request);
    }
}

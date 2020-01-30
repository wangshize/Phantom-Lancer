package com.github.dfs.backupnode.server;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.github.dfs.namenode.rpc.model.FetchEditsLogRequest;
import com.github.dfs.namenode.rpc.model.FetchEditsLogResponse;
import com.github.dfs.namenode.rpc.service.NameNodeServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

import java.util.List;

/**
 * @author wangsz
 * @create 2020-01-29
 **/
public class NameNodeRpcClient {

    private static final String NAMENODE_HOSTNAME = "localhost";
    private static final Integer NAMENODE_PORT = 50070;

    private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;

    public NameNodeRpcClient() {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
        this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
    }

    public List<EditLog> fetchEditsLog(Long fetchedTxIdBegin) {
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder()
                .setEditsLogTxId(fetchedTxIdBegin)
                .build();
        FetchEditsLogResponse response = namenode.fetchEditsLog(request);
        String content = response.getEditsLog();
        return JSON.parseArray(content, EditLog.class);
    }
}

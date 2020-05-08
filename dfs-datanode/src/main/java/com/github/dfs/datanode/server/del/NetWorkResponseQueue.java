package com.github.dfs.datanode.server.del;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author wangsz
 * @create 2020-03-08
 **/
public class NetWorkResponseQueue {

    private static volatile  NetWorkResponseQueue instance = null;

    private Map<Integer, ConcurrentLinkedQueue<NetWorkResponse>> responseQueues =
            new ConcurrentHashMap<>();

    public static NetWorkResponseQueue getInstance() {
        if(instance == null) {
            synchronized (NetWorkResponseQueue.class) {
                if(instance == null) {
                    instance = new NetWorkResponseQueue();
                }
            }
        }
        return instance;
    }

    public void initResponseQueues(Integer processId) {
        ConcurrentLinkedQueue<NetWorkResponse> responseQueue =
                new ConcurrentLinkedQueue<>();
        responseQueues.put(processId, responseQueue);

    }

    public ConcurrentLinkedQueue<NetWorkResponse> getResponseQueue(Integer processId) {
        return responseQueues.get(processId);
    }

    public void offer(Integer processId, NetWorkResponse response) {
        responseQueues.get(processId).offer(response);
    }

    public NetWorkResponse poll(Integer processId) {
        return responseQueues.get(processId).poll();
    }
}

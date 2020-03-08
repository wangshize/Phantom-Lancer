package com.github.dfs.datanode.server;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author wangsz
 * @create 2020-03-08
 **/
public class NetWorkRequestQueue {

    private static volatile  NetWorkRequestQueue instance = null;

    /**
     * 全局请求队列
     */
    private ConcurrentLinkedQueue<NetWorkRequest> requestQueue =
            new ConcurrentLinkedQueue<>();

    public static NetWorkRequestQueue getInstance() {
        if(instance == null) {
            synchronized (NetWorkRequestQueue.class) {
                if(instance == null) {
                    instance = new NetWorkRequestQueue();
                }
            }
        }
        return instance;
    }

    private NetWorkRequestQueue() {
    }

    public void offer(NetWorkRequest request) {
        requestQueue.offer(request);
    }

    public NetWorkRequest poll() {
        return requestQueue.poll();
    }
}

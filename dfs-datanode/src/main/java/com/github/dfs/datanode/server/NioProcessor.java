package com.github.dfs.datanode.server;

import lombok.Getter;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 解析请求、发送响应
 * @author wangsz
 * @create 2020-03-07
 **/
public class NioProcessor extends Thread {

    private ConcurrentLinkedQueue<SocketChannel> channelsQueue =
            new ConcurrentLinkedQueue<>();

    private static final long POLL_BLOCK_MAX_TIME = 1000;

    @Getter
    private Integer processId;

    private Selector selector;

    private Map<String, NetWorkRequest> cacheRequests = new ConcurrentHashMap<>();
    private Map<String, NetWorkResponse> cacheResponses = new ConcurrentHashMap<>();
    private Map<String, SelectionKey> cacheKeys = new ConcurrentHashMap<>();

    public NioProcessor(Integer processId) {
        try {
            this.processId = processId;
            this.selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 为何不能直接在这里将channel注册到selector里，
     * 如果一边register，一边select可能会有问题？
     * @param channel
     */
    public void addChannel(SocketChannel channel) {
        channelsQueue.add(channel);
        selector.wakeup();
    }

    @Override
    public void run() {
        while (true) {
            try {
                //注册排队等待的连接
                registerQueueClients();
                //处理排队中的响应
                cacheQueuedResponse();
                //现实阻塞感知连接中的请求
                poll();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 暂存排队中的响应
     */
    public void cacheQueuedResponse() {
        NetWorkResponseQueue responseQueues = NetWorkResponseQueue.getInstance();
        NetWorkResponse response;
        while ((response = responseQueues.poll(processId)) != null) {
            String client = response.getClient();
            cacheResponses.put(client, response);
            SelectionKey key = cacheKeys.get(client);
            key.interestOps(SelectionKey.OP_WRITE);
        }
    }

    /**
     * 以多路复用的方式来监听各个连接的请求
     */
    private void poll() throws Exception {
        int keys = selector.select(POLL_BLOCK_MAX_TIME);
        if(keys > 0) {
            Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
            while (keyIterator.hasNext()) {
                SelectionKey key = keyIterator.next();
                keyIterator.remove();

                SocketChannel channel = (SocketChannel)key.channel();
                String client = channel.getRemoteAddress().toString();
                if(key.isReadable()) {
                    NetWorkRequest request = cacheRequests.get(client);
                    if(request == null) {
                        request = new NetWorkRequest();
                    }
                    request.setChannel(channel);
                    request.setKey(key);
                    request.read();
                    if(request.hasCompleteRead()) {
                        //请求读取完毕，就交给IO线程处理
                        request.setProcessId(processId);
                        request.setClient(client);

                        NetWorkRequestQueue requestQueue = NetWorkRequestQueue.getInstance();
                        requestQueue.offer(request);

                        cacheKeys.put(client, key);
                        cacheRequests.remove(client);

                        key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
                    } else {
                        cacheRequests.put(client, request);
                    }
                } else if(key.isWritable()) {
                    NetWorkResponse response = cacheResponses.get(client);
                    channel.write(response.getBuffer());

                    cacheResponses.remove(client);
                    cacheKeys.remove(client);

                    key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
                }
            }
        }
    }

    private void registerQueueClients() throws ClosedChannelException {
        SocketChannel channel = null;
        while ((channel = channelsQueue.poll()) != null) {
            channel.register(selector, SelectionKey.OP_READ);
        }
    }
}

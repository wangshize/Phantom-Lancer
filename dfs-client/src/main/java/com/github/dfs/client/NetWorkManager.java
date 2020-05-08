package com.github.dfs.client;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangsz
 **/
public class NetWorkManager {

    public static final Integer CONNECTING = 1;
    public static final Integer CONNECTED = 2;
    public static final Integer DISCONNECTED = 3;

    public static final Integer POLL_TIME_OUT = 500;

    private static final Integer WAIT_CONNECT_TIMEOUT = 3000;


    /**
     * 响应状态
     */
    private static final Integer RESPONSE_SUCCESS = 1;
    private static final Integer RESPONSE_FAILURE = 2;

    private Selector selector;

    /**
     * 已经建立好的连接
     * key：hostName value：连接
     */
    private Map<String, SelectionKey> connections;

    /**
     * 建立好的链接的状态
     * key：hostName value：状态
     */
    private Map<String, Integer> connectedStatus;

    /**
     * 等待建立连接的列表
     */
    private ConcurrentLinkedQueue<Host> waitingConnectHosts;

    /**
     * 等待发送的请求
     */
    private Map<String, ConcurrentLinkedQueue<NetWorkRequest>> waitingRequests;

    /**
     * 已经发送出去的请求，对于每个host来说，请求必须顺序发送，这个host的请求发送出去，等到它的
     * 响应回来了才能发送下一个。 即使上层应用是并行发送的
     * key：hostName
     */
    private Map<String, NetWorkRequest> toSendRequests;
    /**
     * 请求的响应
     * key：requestId
     */
    private Map<String, NetWorkResponse> waitingResponses;


    public NetWorkManager() {
        try {
            this.selector = Selector.open();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.connections = new ConcurrentHashMap<>();
        this.connectedStatus = new ConcurrentHashMap<>();
        this.waitingConnectHosts = new ConcurrentLinkedQueue<>();
        this.waitingRequests = new ConcurrentHashMap<>();
        this.toSendRequests = new ConcurrentHashMap<>();
        this.waitingResponses = new ConcurrentHashMap<>();
        new PollThread().start();
    }

    public Boolean hasConnected(String hostName) {
        return connectedStatus.containsKey(hostName) &&
                CONNECTED.equals(connections.get(hostName));
    }

    public void tryConnect(String hostName, int nioPort) throws Exception {
        synchronized (this) {
            if(!connectedStatus.containsKey(hostName) || DISCONNECTED.equals(connectedStatus.get(hostName))) {
                connectedStatus.put(hostName, CONNECTING);
                waitingConnectHosts.offer(new Host(hostName, nioPort));
            }
            while (CONNECTING.equals(connections.get(hostName))) {
                wait(WAIT_CONNECT_TIMEOUT);
            }
        }
    }

    public void sendRequest(NetWorkRequest request) {
        ConcurrentLinkedQueue<NetWorkRequest> requestQueue =
                waitingRequests.get(request.getHostName());
        requestQueue.offer(request);
        NetWorkResponse response = new NetWorkResponse();
        response.setRequestId(request.getRequestId());
        waitingResponses.put(request.getRequestId(), response);
    }

    public NetWorkResponse waitResponse(NetWorkRequest request) throws Exception {
        String requestId = request.getRequestId();
        NetWorkResponse response = waitingResponses.get(requestId);
        if(response == null) {
            return null;
        }
        CountDownLatch countDownLatch = response.getCountDownLatch();
        countDownLatch.wait(request.getTimeOut());
        return waitingResponses.remove(requestId);
    }

    class PollThread extends Thread {

        @Override
        public void run() {
            while (true) {
                try {
                    tryConnectOnWait();
                    prepareRequest();
                    poll();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private void tryConnectOnWait() throws Exception {
            Host host;
            while ((host = waitingConnectHosts.poll()) != null) {
                try {
                    SocketChannel channel = SocketChannel.open();
                    channel.configureBlocking(false);
                    channel.connect(new InetSocketAddress(host.hostName, host.nioPort));
                    channel.register(selector, SelectionKey.OP_CONNECT);
                } catch (IOException e) {
                    e.printStackTrace();
                    connectedStatus.put(host.getHostName(), DISCONNECTED);
                }
            }
        }

        /**
         * 准备好要发送的请求
         */
        private void prepareRequest() {
            for (String hostName : waitingRequests.keySet()) {
                ConcurrentLinkedQueue<NetWorkRequest> requestQueue =
                        waitingRequests.get(hostName);
                if(!requestQueue.isEmpty() && !toSendRequests.containsKey(hostName)) {
                    NetWorkRequest request = requestQueue.poll();
                    toSendRequests.put(hostName, request);

                    SelectionKey key = connections.get(hostName);
                    key.interestOps(SelectionKey.OP_WRITE);
                }
            }
        }

        private void poll() throws Exception {
            SocketChannel channel = null;
            try {
                int selectKeys = selector.select(POLL_TIME_OUT);

                if(selectKeys <= 0) {
                    return;
                }

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
                while(keysIterator.hasNext()){
                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();

                    channel = (SocketChannel) key.channel();
                    if(key.isConnectable()){
                        finishConnect(channel, key);
                    } else if(key.isWritable()) {
                        sendRequest(channel, key);
                    } else if(key.isReadable()) {
                        readResponse(channel, key);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                if(channel != null) {
                    channel.close();
                }
            }
        }

        private void sendRequest(SocketChannel channel, SelectionKey key) throws Exception {
            InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
            String hostName = remoteAddress.getHostName();

            NetWorkRequest request = toSendRequests.get(hostName);
            ByteBuffer buffer = request.getByteBuffer();
            channel.write(buffer);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            System.out.println("本次请求发送完毕。。。。。。");
            key.interestOps(SelectionKey.OP_READ);
        }

        private void readResponse(SocketChannel channel, SelectionKey key) throws Exception {
            InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
            String hostName = remoteAddress.getHostName();

            NetWorkRequest request = toSendRequests.get(hostName);
            String requestId = request.getRequestId();
            NetWorkResponse response = waitingResponses.get(requestId);
            if(NetWorkRequest.SEND_FILE.equals(request.getRequestType())) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                channel.read(byteBuffer);
                byteBuffer.flip();
                response.setRequestId(request.getRequestId());
                response.setBuffer(byteBuffer);
                response.setSendSuccess(true);
            }
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
            if(request.getAsync()) {
                waitingResponses.remove(requestId);
            }
            CountDownLatch countDownLatch = response.getCountDownLatch();
            countDownLatch.countDown();
            toSendRequests.remove(hostName);
        }

        private void finishConnect(SocketChannel channel, SelectionKey key) throws IOException, InterruptedException {
            if(channel.isConnectionPending()){
                //三次握手完毕，一个TCP链接建立完毕
                if(!channel.finishConnect()) {
                    Thread.sleep(100);
                }
            }
            System.out.println("完成与服务端的连接");
            InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
            String hostName = remoteAddress.getHostName();
            waitingRequests.put(hostName, new ConcurrentLinkedQueue<>());
            connections.put(hostName, key);
            connectedStatus.put(hostName, CONNECTED);
        }

    }

    @Setter
    @Getter
    class Host {
        private String hostName;
        private Integer nioPort;

        public Host(String hostName, Integer nioPort) {
            this.hostName = hostName;
            this.nioPort = nioPort;
        }
    }
}

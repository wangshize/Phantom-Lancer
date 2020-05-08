package com.github.dfs.datanode.server;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class DataNodeNIOServer extends Thread {

    public static final Integer PROCESSOR_THREAD_NUM = 3;
    public static final Integer IO_THREAD_NUM = 3;

    private Selector selector;

    private List<NioProcessor> processors = new ArrayList<>();

    private NameNodeRpcClient nameNodeRpcClient;

    public DataNodeNIOServer(NameNodeRpcClient nameNodeRpcClient) {
        this.nameNodeRpcClient = nameNodeRpcClient;
    }

    public void init() {

        ServerSocketChannel serverSocketChannel = null;
        try {
            selector = Selector.open();

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(DataNodeConfig.NIO_PORT), 100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            System.out.println("NIOServer已经启动，开始监听端口：" + DataNodeConfig.NIO_PORT);
            NetWorkResponseQueue responseQueue = NetWorkResponseQueue.getInstance();
            for (int i = 0; i < PROCESSOR_THREAD_NUM; i++) {
                NioProcessor processor = new NioProcessor(i);
                processors.add(processor);
                processor.start();

                responseQueue.initResponseQueues(i);
            }
            for (Integer i = 0; i < IO_THREAD_NUM; i++) {
                IOThread ioThread = new IOThread(nameNodeRpcClient);
                ioThread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while(true){
            try{
                selector.select();
                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

                while(keysIterator.hasNext()){
                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();
                    SocketChannel channel = null;
                    try {
                        if(key.isAcceptable()){
                            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                            channel = serverSocketChannel.accept();
                            if(channel != null) {
                                channel.configureBlocking(false);
                                //和某个客户端建立连接后，将该客户端均匀分发给process处理请求
                                int index = new Random().nextInt(PROCESSOR_THREAD_NUM);
                                NioProcessor processor = processors.get(index);
                                processor.addChannel(channel);
                            }
                        }
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                }
            }
            catch(Throwable t){
                t.printStackTrace();
            }
        }
    }
}

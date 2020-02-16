package com.github.dfs.namenode.server;

import com.github.dfs.namenode.NameNodeConstants;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

/**
 * fsimage文件上传
 * @author wangsz
 * @create 2020-01-31
 **/
public class FSImageUploadServer extends Thread {

    private Selector selector;

    public FSImageUploadServer() {
        init();
    }

    private void init() {
        ServerSocketChannel serverSocketChannel = null;
        try {
            selector = Selector.open();
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(9000), 100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        System.out.println("FSImageUploadServer启动，监听9000端口......");

        while(true){
            try{
                selector.select();
                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

                while(keysIterator.hasNext()){
                    SelectionKey key = (SelectionKey) keysIterator.next();
                    keysIterator.remove();
                    try {
                        handleRequest(key);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            catch(Throwable t){
                t.printStackTrace();
            }
        }
    }

    private void handleRequest(SelectionKey key)
            throws IOException, ClosedChannelException {
        if(key.isAcceptable()){
            System.out.println("接收到连接请求");
            handleConnectRequest(key);
        } else if(key.isReadable()){
            System.out.println("接收到read请求");
            handleReadableRequest(key);
        } else if(key.isWritable()) {
            System.out.println("接收到write请求");
            handleWritableRequest(key);
        }
    }

    /**
     * 处理BackupNode连接请求
     * @param key
     * @throws Exception
     */
    private void handleConnectRequest(SelectionKey key) throws IOException {
        SocketChannel channel = null;

        try {
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
            channel = serverSocketChannel.accept();
            if(channel != null) {
                channel.configureBlocking(false);
                channel.register(selector, SelectionKey.OP_READ);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if(channel != null) {
                channel.close();
            }
        }
    }

    /**
     * 处理发送fsimage文件的请求
     * @param key
     * @throws Exception
     */
    private void handleReadableRequest(SelectionKey key) throws IOException {
        SocketChannel channel = null;

        try {
            channel = (SocketChannel) key.channel();
            int count = -1;
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            if((count = channel.read(buffer)) > 0){
                File file = new File(NameNodeConstants.fsimageFilePath);
                if(file.exists()) {
                    file.delete();
                }
                try(RandomAccessFile fsimageImageRAF = new RandomAccessFile(NameNodeConstants.fsimageFilePath, "rw");
                     FileOutputStream fsimageOut = new FileOutputStream(fsimageImageRAF.getFD());
                     FileChannel fsimageFileChannel = fsimageOut.getChannel()) {

                    buffer.flip();
                    fsimageFileChannel.write(buffer);
                    buffer.clear();

                    while((count = channel.read(buffer)) > 0){
                        buffer.flip();
                        fsimageFileChannel.write(buffer);
                        buffer.clear();
                    }

                    fsimageFileChannel.force(false);
                    System.out.println("fsimage写入磁盘完毕......");
                    channel.register(selector, SelectionKey.OP_WRITE);
                }
            } else {
                channel.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if(channel != null) {
                channel.close();
            }
        }
    }

    /**
     * 处理返回响应给BackupNode
     * @param key
     * @throws Exception
     */
    private void handleWritableRequest(SelectionKey key) throws IOException {
        SocketChannel channel = null;

        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            buffer.put("返回响应给backupnode。。。。。。".getBytes());
            buffer.flip();

            channel = (SocketChannel) key.channel();
            channel.write(buffer);

            channel.register(selector, SelectionKey.OP_READ);
        } catch (Exception e) {
            e.printStackTrace();
            if(channel != null) {
                channel.close();
            }
        }
    }
}

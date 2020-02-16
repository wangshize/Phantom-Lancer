package com.github.dfs.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * 上传文件到datanode
 * @author wangsz
 * @create 2020-02-15
 **/
public class NIOClient {

    /**
     * 发送文件
     * @param hostName
     * @param nioPort
     * @param file
     * @param fileSize
     */
    public static void sendFile(String hostName, int nioPort,
                                byte[] file, long fileSize, String fileName) {
        //建立短链接，发送完一个文件就释放连接  简单实现
        SocketChannel channel = null;
        Selector selector = null;
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(hostName, nioPort));
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean sending = true;

            while(sending){
                selector.select();

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
                while(keysIterator.hasNext()){
                    SelectionKey key = (SelectionKey) keysIterator.next();
                    keysIterator.remove();
                    //NIOServer回应允许建立连接
                    if(key.isConnectable()){
                        channel = (SocketChannel) key.channel();

                        if(channel.isConnectionPending()){
                            //三次握手完毕，一个TCP链接建立完毕
                            channel.finishConnect();

                            ByteBuffer buffer = ByteBuffer.allocate((int)fileSize * 2 + fileName.length());
                            //文件名长度和文件名
                            buffer.putInt(fileName.length());
                            buffer.put(fileName.getBytes());
                            // long对应了8个字节，放到buffer里去 表示图片大小
                            buffer.putLong(fileSize);
                            buffer.put(file);
                            buffer.flip();
                            int sendData = channel.write(buffer);
                            System.out.println("已经发送" + sendData + "字节的数据");

                            channel.register(selector, SelectionKey.OP_READ);
                        }
                    }
                    //收到NIOServer的响应
                    else if(key.isReadable()){
                        channel = (SocketChannel) key.channel();

                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        int len = channel.read(buffer);

                        if(len > 0) {
                            System.out.println("[" + Thread.currentThread().getName()
                                    + "]收到响应：" + new String(buffer.array(), 0, len));
                            sending = false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            if(channel != null){
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if(selector != null){
                try {
                    selector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

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

    public static final Integer SEND_FILE = 1;
    public static final Integer READ_FILE = 2;

    /**
     * 发送文件
     * @param hostName
     * @param nioPort
     * @param file
     * @param fileSize
     */
    public void sendFile(String hostName, int nioPort,
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

                            ByteBuffer buffer = sendFileByteBuffer(file, fileSize, fileName);
                            int sendData = channel.write(buffer);
                            System.out.println("已经发送" + sendData + "字节的数据");

                            channel.register(selector, SelectionKey.OP_READ);
                        }
                    }
                    //收到NIOServer的响应
                    else if(key.isReadable()){
                        channel = (SocketChannel) key.channel();

                        sending = afterSendFile(channel, sending);
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

    public byte[] readFile(String hostName, int nioPort, String fileName) {
        //建立短链接，发送完一个文件就释放连接  简单实现
        SocketChannel channel = null;
        Selector selector = null;
        byte[] file = null;
        ReadFileResult readFileResult = new ReadFileResult();
        Long fileLength = null;
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress(hostName, nioPort));
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean reading = true;
            while (reading) {
                selector.select();

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
                while(keysIterator.hasNext()) {
                    SelectionKey key = (SelectionKey) keysIterator.next();
                    keysIterator.remove();
                    if(key.isConnectable()){
                        if(channel.isConnectionPending()){
                            channel.finishConnect();

                            ByteBuffer buffer = readFileByteBuffer(fileName);

                            int sendData = channel.write(buffer);
                            System.out.println("已经发送" + sendData + "字节的数据");

                            channel.register(selector, SelectionKey.OP_READ);
                        }

                    } else if(key.isReadable()) {
                        channel = (SocketChannel) key.channel();

                        boolean complete = completeReadFile(channel, fileLength, readFileResult);
                        if(complete) {
                            ByteBuffer fileBuffer = readFileResult.fileBuffer;
                            file = fileBuffer.array();
                            System.out.println("[" + Thread.currentThread().getName()
                                    + "]收到" + hostName + "的响应");
                            reading = false;
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
        return file;
    }

    private static ByteBuffer sendFileByteBuffer(byte[] file, long fileSize, String fileName) {
        ByteBuffer buffer = ByteBuffer.allocate((int)fileSize * 2 + fileName.length());
        buffer.putInt(SEND_FILE);
        //文件名长度和文件名
        buffer.putInt(fileName.length());
        buffer.put(fileName.getBytes());
        // long对应了8个字节，放到buffer里去 表示图片大小
        buffer.putLong(fileSize);
        buffer.put(file);
        buffer.flip();
        return buffer;
    }

    private static boolean afterSendFile(SocketChannel channel, boolean sending) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int len = channel.read(buffer);

        if(len > 0) {
            System.out.println("[" + Thread.currentThread().getName()
                    + "]收到响应：" + new String(buffer.array(), 0, len));
            sending = false;
        }
        return sending;
    }

    private static ByteBuffer readFileByteBuffer(String fileName) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + fileName.length());
        buffer.putInt(READ_FILE);
        //文件名长度和文件名
        buffer.putInt(fileName.length());
        buffer.put(fileName.getBytes());
        buffer.flip();
        return buffer;
    }

    private static boolean completeReadFile(SocketChannel channel, Long fileLength, ReadFileResult readFileResult) throws IOException {
        boolean completeRead = false;
        if(fileLength == null) {
            ByteBuffer fileLengthBuffer = ByteBuffer.allocate(8);
            channel.read(fileLengthBuffer);
            fileLengthBuffer.rewind();
            fileLength = fileLengthBuffer.getLong();
            System.out.println("接收到文件总长度为：" + fileLength);
        }

        if(readFileResult.fileBuffer == null) {
            readFileResult.fileBuffer = ByteBuffer.allocate(fileLength.intValue());
        }
        ByteBuffer fileBuffer = readFileResult.fileBuffer;
        channel.read(fileBuffer);
        System.out.println("本次接收到文件数据长度：" + fileBuffer.limit());
        if(!fileBuffer.hasRemaining()) {
            completeRead = true;
        }
        return completeRead;
    }

    class ReadFileResult {
        ByteBuffer fileBuffer;
        boolean complete;
    }

}

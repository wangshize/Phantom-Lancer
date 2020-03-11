package com.github.dfs.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.UUID;

/**
 * 上传文件到datanode
 * @author wangsz
 * @create 2020-02-15
 **/
public class NioClient {

    public static final Integer FILENAME_LENGTH = 4;
    public static final Integer FILE_LENGTH = 8;

    private NetWorkManager netWorkManager;

    public NioClient() {
        this.netWorkManager = new NetWorkManager();
    }

    /**
     * 发送文件
     * @param hostName
     * @param nioPort
     * @param file
     * @param fileSize
     */
    public boolean sendFile(String hostName, int nioPort,
                                byte[] file, long fileSize, String fileName) throws Exception {
        netWorkManager.tryConnect(hostName, nioPort);
        NetWorkRequest request = NetWorkRequest.builder()
                .requestId(UUID.randomUUID().toString())
                .requestType(NetWorkRequest.SEND_FILE)
                .byteBuffer(sendFileByteBuffer(file, fileSize, fileName))
                .hostName(hostName)
                .nioPort(nioPort)
                .timeOut(200L)
                .build();
        netWorkManager.sendRequest(request);
        NetWorkResponse response =  netWorkManager.waitResponse(request);
        ByteBuffer buffer = response.getBuffer();
        String result = new String(buffer.array(), 0, buffer.remaining());

        if(result.equals(NetWorkResponse.SUCCESS)) {
            return true;
        }
        return false;
    }

    public byte[] readFile(String hostName, int nioPort, String fileName) throws Exception {
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
                    SelectionKey key = keysIterator.next();
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
            throw new Exception("download file fail......");
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

    private ByteBuffer sendFileByteBuffer(byte[] file, long fileSize, String fileName) {
        ByteBuffer buffer = ByteBuffer.allocate(
                NetWorkRequest.REQUEST_TYPE +
                FILENAME_LENGTH +
                fileName.length() +
                FILE_LENGTH +
                (int)fileSize );
        buffer.putInt(NetWorkRequest.SEND_FILE);
        //文件名长度和文件名
        buffer.putInt(fileName.length());
        buffer.put(fileName.getBytes());
        // long对应了8个字节，放到buffer里去 表示图片大小
        buffer.putLong(fileSize);
        buffer.put(file);
        buffer.rewind();
        return buffer;
    }

    private static ByteBuffer readFileByteBuffer(String fileName) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + fileName.length());
        buffer.putInt(NetWorkRequest.READ_FILE);
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
            readFileResult.complete = true;
        }
        return completeRead;
    }

    class ReadFileResult {
        ByteBuffer fileBuffer;
        boolean complete;
    }

}

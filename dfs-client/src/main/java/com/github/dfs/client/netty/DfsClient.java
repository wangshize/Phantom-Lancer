package com.github.dfs.client.netty;

import com.github.dfs.client.ResponseCallBack;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * @author wangsz
 * @create 2020-05-08
 **/
public class DfsClient {

    private NetWorkManager netWorkManager;

    public DfsClient() {
        this.netWorkManager = new NetWorkManager();
    }

    public void sendFileAsync(String hostName, int nioPort,
                              NetWorkRequest request,
                              ResponseCallBack callBack) throws Exception {
        ChannelFuture resFuture = doSend(hostName, nioPort, request);
    }

    public boolean sendFile(String hostName, int nioPort,
                            NetWorkRequest request,
                            ResponseCallBack callBack) throws InterruptedException {
        ChannelFuture resFuture = doSend(hostName, nioPort, request);
        resFuture.sync();
        return resFuture.isSuccess();
    }

    private ChannelFuture doSend(String hostName, int nioPort, NetWorkRequest request) throws InterruptedException {
        byte[] file = request.getFileBytes();
        String fileName = request.getFileName();
        Channel channel = netWorkManager.getAndCreateChannel(hostName, nioPort);
        ByteBuf reqBuf = sendFileByteBuffer(file, fileName);
        return channel.writeAndFlush(reqBuf);
    }
    public static final Integer FILENAME_LENGTH = 4;
    public static final Integer FILE_LENGTH = 8;
    public static final Integer REQUEST_TYPE_LENGTH = 4;

    private ByteBuf sendFileByteBuffer(byte[] file, String fileName) {
        long frameLength =  REQUEST_TYPE_LENGTH + FILENAME_LENGTH + fileName.length()
                + FILE_LENGTH + file.length;
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeLong(frameLength);
        buffer.writeInt(NetWorkRequest.SEND_FILE);
        //文件名长度和文件名
        buffer.writeInt(fileName.length());
        buffer.writeBytes(fileName.getBytes());
        // long对应了8个字节，放到buffer里去 表示图片大小
        buffer.writeLong(file.length);
        buffer.writeBytes(file);
        return buffer;
    }

    public byte[] readFile(String hostName, int nioPort, String fileName) throws Exception {
        return new byte[0];
    }

    private ByteBuf readFileByteBuffer(String fileName) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeInt(NetWorkRequest.READ_FILE);
        //文件名长度和文件名
        buffer.writeInt(fileName.length());
        buffer.writeBytes(fileName.getBytes());
        return buffer;
    }
}

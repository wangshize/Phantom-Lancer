package com.github.dfs.client.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author wangsz
 * @create 2020-04-03
 **/
public class DfsClientEncoder extends MessageToByteEncoder<NetWorkRequest> {

    @Override
    protected void encode(ChannelHandlerContext ctx, NetWorkRequest request, ByteBuf out) throws Exception {
        ByteBuf requestByteBuf;
        Integer requestType = request.getRequestType();
        if (NetWorkRequest.SEND_FILE.equals(requestType)) {
            String fileName = request.getFileName();
            byte[] fileBytes = request.getFileBytes();
            long fileSize = fileBytes.length;
            requestByteBuf = sendFileByteBuffer(fileBytes, fileSize, fileName);
        } else if (NetWorkRequest.READ_FILE.equals(requestType)) {
            String fileName = request.getFileName();
            requestByteBuf = readFileByteBuffer(fileName);
        } else {
            requestByteBuf = Unpooled.wrappedBuffer("Hello".getBytes());
        }
        out.writeBytes(requestByteBuf);
    }

    private ByteBuf readFileByteBuffer(String fileName) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeInt(NetWorkRequest.READ_FILE);
        //文件名长度和文件名
        buffer.writeInt(fileName.length());
        buffer.writeBytes(fileName.getBytes());
        return buffer;
    }

    private ByteBuf sendFileByteBuffer(byte[] file, long fileSize, String fileName) {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeInt(NetWorkRequest.SEND_FILE);
        //文件名长度和文件名
        buffer.writeInt(fileName.length());
        buffer.writeBytes(fileName.getBytes());
        // long对应了8个字节，放到buffer里去 表示图片大小
        buffer.writeLong(fileSize);
        buffer.writeBytes(file);
        return buffer;
    }
}

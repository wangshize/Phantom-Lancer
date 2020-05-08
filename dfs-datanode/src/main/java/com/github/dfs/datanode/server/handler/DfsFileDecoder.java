package com.github.dfs.datanode.server.handler;

import com.github.dfs.datanode.server.FileUtiles;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;

import java.util.List;

/**
 * @author wangsz
 * @create 2020-04-02
 **/
public class DfsFileDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msgBuffer, List<Object> out) throws Exception {
        NetWorkRequest request = new NetWorkRequest();
        Integer requestType = msgBuffer.readInt();
        request.setRequestType(requestType);
        Integer filenameLength = msgBuffer.readInt();
        ByteBuf fileNameBuf = msgBuffer.readBytes(filenameLength);
        String relativeFilename = new String(fileNameBuf.array(), CharsetUtil.UTF_8);
        request.setRelativeFilename(relativeFilename);
        String absoluteFilename = FileUtiles.getAbsoluteFileName(relativeFilename);
        request.setAbsoluteFilename(absoluteFilename);
        Long fileLength = msgBuffer.readLong();
        request.setFileLength(fileLength);
        byte[] fileBytes = new byte[fileLength.intValue()];
        ByteBuf fileBuf = msgBuffer.readBytes(fileBytes);
        request.setFileBuffer(fileBuf.nioBuffer());
        out.add(request);
    }
}

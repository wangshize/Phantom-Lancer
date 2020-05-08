package com.github.dfs.datanode.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author wangsz
 * @create 2020-04-02
 **/
public class DfsFileEncoder extends MessageToByteEncoder<NetWorkResponse> {

    @Override
    protected void encode(ChannelHandlerContext ctx, NetWorkResponse msg, ByteBuf out) throws Exception {
        byte[] responseByte = msg.getResponse().getBytes();
        out.writeBytes(Unpooled.wrappedBuffer(responseByte));
        ctx.writeAndFlush(msg);
    }
}

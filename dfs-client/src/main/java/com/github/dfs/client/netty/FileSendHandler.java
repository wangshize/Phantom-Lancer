package com.github.dfs.client.netty;

import com.github.dfs.client.NetWorkRequest;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author wangsz
 * @create 2020-04-03
 **/
public class FileSendHandler extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ByteBuf fileByteBuf = (ByteBuf) msg;
        ctx.writeAndFlush(fileByteBuf);
    }

}

package com.github.dfs.client.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author wangsz
 * @create 2020-04-03
 **/
public class NetWorkManager {

    public static final Integer FILENAME_LENGTH = 4;
    public static final Integer FILE_LENGTH = 8;

    private Channel channel;

    public NetWorkManager(String hostName, int port) {
        EventLoopGroup workThreadGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        try {
            bootstrap.group(workThreadGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new FileSendHandler());
                            ch.pipeline().addLast(new DfsClientEncoder());
                        }
                    });
            ChannelFuture channelFuture = bootstrap.connect(hostName, port).sync();
            channel = channelFuture.channel();
            channel.closeFuture().sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            workThreadGroup.shutdownGracefully();
        }
    }

    /**
     * 发送文件
     * @param file
     */
    public ChannelFuture sendFile(byte[] file, String fileName) throws Exception {
        NetWorkRequest request = NetWorkRequest.builder()
                .requestId(UUID.randomUUID().toString())
                .requestType(NetWorkRequest.SEND_FILE)
                .fileBytes(file)
                .fileName(fileName)
                .timeOut(200L)
                .build();
        ChannelFuture channelFuture = channel.write(request);
        channelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {

            }
        });
        return channelFuture;
    }

    public ChannelFuture readFile(String fileName) {
        NetWorkRequest request = NetWorkRequest.builder()
                .requestId(UUID.randomUUID().toString())
                .requestType(NetWorkRequest.SEND_FILE)
                .fileName(fileName)
                .build();
        ChannelFuture channelFuture = channel.write(request);
        channelFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {

            }
        });
        return channelFuture;
    }
}

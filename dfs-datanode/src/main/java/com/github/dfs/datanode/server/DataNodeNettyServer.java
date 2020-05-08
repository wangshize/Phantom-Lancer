package com.github.dfs.datanode.server;


import com.github.dfs.datanode.server.handler.DfsFileDecoder;
import com.github.dfs.datanode.server.handler.DfsFileEncoder;
import com.github.dfs.datanode.server.handler.FileServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;

/**
 * 基于Netty的nio服务端
 */
public class DataNodeNettyServer {

    private NameNodeRpcClient nameNodeRpcClient;

    public DataNodeNettyServer(NameNodeRpcClient nameNodeRpcClient) {
        this.nameNodeRpcClient = nameNodeRpcClient;
    }

    public void init() {

        EventLoopGroup acceptorThreadGroup = new NioEventLoopGroup();
        EventLoopGroup ioThreadGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(acceptorThreadGroup, ioThreadGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception {
                            ByteBuf delimiterTag = Unpooled.wrappedBuffer("$".getBytes());
                            channel.pipeline().addLast(new DelimiterBasedFrameDecoder(DataNodeConfig.MAX_SIZE_FILE, delimiterTag));
                            channel.pipeline().addLast(new DfsFileDecoder());
                            channel.pipeline().addLast(new DfsFileEncoder());
                            channel.pipeline().addLast(new FileServerHandler(nameNodeRpcClient));
                        }
                    });
            ChannelFuture channelFuture = bootstrap.bind(DataNodeConfig.NIO_PORT).sync();
            channelFuture.channel().close().sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            acceptorThreadGroup.shutdownGracefully();
            ioThreadGroup.shutdownGracefully();
        }
    }


}

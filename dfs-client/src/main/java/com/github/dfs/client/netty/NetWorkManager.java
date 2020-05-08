package com.github.dfs.client.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author wangsz
 * @create 2020-04-03
 **/
public class NetWorkManager {

    /**
     * key : host@port
     * value : channel
     */
    private Map<String, Channel> dataNodeChannels = new ConcurrentHashMap<>();

    private Bootstrap bootstrap = new Bootstrap();

    public NetWorkManager() {
        EventLoopGroup workThreadGroup = new NioEventLoopGroup();
        try {
            bootstrap.group(workThreadGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {

                            ch.pipeline().addLast(new FileSendHandler());
                        }
                    });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Channel getAndCreateChannel(String hostName, int port) throws InterruptedException {
        String key = hostName + "@" + port;
        Channel channel = dataNodeChannels.get(key);
        if(channel != null) {
            return channel;
        }
        ChannelFuture channelFuture = bootstrap.connect(hostName, port);
        channelFuture.sync();
        channel = channelFuture.channel();
        dataNodeChannels.put(key, channel);
        return channel;
    }
}

package com.github.dfs.backupnode.server;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * 负责上传fsimage到NameNode线程
 * @author wangsz
 * @create 2020-01-31
 **/
public class FSImageUploader extends Thread {

    private static final String fsimageFilePath = "/Users/wangsz/SourceCode/backupnode/fsimage.meta";

    private FSImage fsimage;

    public FSImageUploader(FSImage fsimage) {
        this.fsimage = fsimage;
    }

    @Override
    public void run() {
        SocketChannel socketChannel = null;
        try(SocketChannel channel = SocketChannel.open();
            Selector selector = Selector.open();
            ) {
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress("localhost", 9000));
            channel.register(selector, SelectionKey.OP_CONNECT);
            boolean uploading = true;

            while (uploading) {
                selector.select();
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();
                    if(key.isConnectable()) {
                        socketChannel = (SocketChannel) key.channel();
                        if(socketChannel.isConnectionPending()) {
                            socketChannel.finishConnect();

                            ByteBuffer byteBuffer = ByteBuffer.wrap(fsimage.getFsImageJson().getBytes());
                            System.out.println("准备上传fsimage文件数据，大小为：" + byteBuffer.capacity());
                            socketChannel.write(byteBuffer);
                        }
                        channel.register(selector, SelectionKey.OP_READ);
                    } else if(key.isReadable()) {
                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        socketChannel = (SocketChannel) key.channel();
                        int count = socketChannel.read(buffer);
                        if(count > 0) {
                            System.out.println("上传fsimage文件成功，响应消息为：" +
                                    new String(buffer.array(), 0, count));
                            channel.close();
                            uploading = false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(socketChannel != null) {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

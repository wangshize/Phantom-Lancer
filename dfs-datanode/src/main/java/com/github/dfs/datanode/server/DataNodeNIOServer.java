package com.github.dfs.datanode.server;


import com.github.dfs.namenode.ByteUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static com.github.dfs.datanode.server.DataNodeConfig.NIO_PORT;

public class DataNodeNIOServer extends Thread {

    private Selector selector;

    private NameNodeRpcClient nameNodeRpcClient;

    private List<LinkedBlockingQueue<SelectionKey>> queues =
            new ArrayList<>();
    //未读取完成的文件缓存
    private Map<String, CachedImage> cachedImages = new HashMap<>();

    public DataNodeNIOServer(NameNodeRpcClient nameNodeRpcClient){
        ServerSocketChannel serverSocketChannel = null;
        this.nameNodeRpcClient = nameNodeRpcClient;
        try {
            selector = Selector.open();

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(NIO_PORT), 100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            for(int i = 0; i < 3; i++) {
                queues.add(new LinkedBlockingQueue<SelectionKey>());
            }

            for(int i = 0; i < 3; i++) {
                new Worker(queues.get(i)).start();
            }

            System.out.println("NIOServer已经启动，开始监听端口：" + NIO_PORT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while(true){
            try{
                selector.select();
                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

                while(keysIterator.hasNext()){
                    SelectionKey key = (SelectionKey) keysIterator.next();
                    keysIterator.remove();
                    handleRequest(key);
                }
            }
            catch(Throwable t){
                t.printStackTrace();
            }
        }
    }

    private void handleRequest(SelectionKey key)
            throws IOException, ClosedChannelException {
        SocketChannel channel = null;

        try{
            if(key.isAcceptable()){
                ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                channel = serverSocketChannel.accept();
                if(channel != null) {
                    channel.configureBlocking(false);
                    channel.register(selector, SelectionKey.OP_READ);
                }
            }
            else if(key.isReadable()){
                //同一个地址的的请求路由到同一个队列中，由同一个线程进行处理
                //不能直接放入线程池，因为涉及到拆包的问题，并发下，很难保证
                //不同多线程处理一个数据包的拆包问题
                channel = (SocketChannel) key.channel();
                String remoteAddr = channel.getRemoteAddress().toString();
                int queueIndex = remoteAddr.hashCode() % queues.size();
                queues.get(queueIndex).put(key);
            }
        }
        catch(Throwable t){
            t.printStackTrace();
            if(channel != null){
                channel.close();
            }
        }
    }

    class Worker extends Thread {

        private LinkedBlockingQueue<SelectionKey> queue;

        public Worker(LinkedBlockingQueue<SelectionKey> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while(true) {
                SocketChannel channel = null;

                try {
                    SelectionKey key = queue.take();
                    channel = (SocketChannel) key.channel();
                    if(!channel.isOpen()) {
                        channel.close();
                        continue;
                    }
                    String remoteAddr = channel.getRemoteAddress().toString();
                    System.out.println("接收到客户端请求：" + remoteAddr);
                    ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);

                    Filename fileName = getFileName(channel, buffer);
                    System.out.println("解析出文件名：" + fileName.absoluteFilename);
                    if(fileName == null) {
                        channel.close();
                        continue;
                    }
                    //文件大小
                    long imageLength = getImageLength(channel, buffer);
                    System.out.println("解析出文件大小：" + imageLength);
                    long hasReadImageLength = getHasReadImageLength(channel);
                    System.out.println("初始化已经读取的文件大小：" + hasReadImageLength);
                    FileOutputStream imageOut = new FileOutputStream(fileName.absoluteFilename);
                    FileChannel imageChannel = imageOut.getChannel();
                    imageChannel.position(imageChannel.size());

                    // 如果是第一次接收到请求，就应该把buffer里剩余的数据写入到文件里去
                    if(!cachedImages.containsKey(remoteAddr)) {
                        hasReadImageLength += imageChannel.write(buffer);
                        System.out.println("已经向本地磁盘文件写入了" + hasReadImageLength + "字节的数据");
                        buffer.clear();
                    }

                    // 循环不断的从channel里读取数据，并写入磁盘文件
                    int len = -1;
                    while((len = channel.read(buffer)) > 0) {
                        hasReadImageLength += len;
                        System.out.println("已经向本地磁盘文件写入了" + hasReadImageLength + "字节的数据");
                        buffer.flip();
                        imageChannel.write(buffer);
                        buffer.clear();
                    }

                    if(cachedImages.get(remoteAddr) != null) {
                        if(hasReadImageLength == cachedImages.get(remoteAddr).hasReadImageLength) {
                            channel.close();
                            continue;
                        }
                    }

                    imageChannel.close();
                    imageOut.close();

                    // 如果已经读取完毕，就返回一个成功给客户端
                    if(hasReadImageLength == imageLength) {
                        ByteBuffer outBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
                        channel.write(outBuffer);
                        cachedImages.remove(remoteAddr);
                        System.out.println("文件读取完毕，返回响应给客户端: " + remoteAddr);
                        //增量上报给namenode，接收到的文件信息
                        nameNodeRpcClient.informReplicaReceived(fileName.relativeFilename, DataNodeConfig.DATANODE_HOSTNAME, DataNodeConfig.DATANODE_IP);
                    }
                    // 如果一个文件没有读完，缓存起来，等待下一次读取
                    else {
                        CachedImage cachedImage = new CachedImage(fileName, imageLength, hasReadImageLength);
                        cachedImages.put(remoteAddr, cachedImage);
                        key.interestOps(SelectionKey.OP_READ);
                        System.out.println("文件没有读取完毕，等待下一次OP_READ请求，缓存文件：" + cachedImage);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if(channel != null) {
                        try {
                            channel.close();
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }
        }

    }

    private long getImageLength(SocketChannel channel, ByteBuffer buffer) throws IOException {
        Long imageLength = 0L;
        String remoteAddr = channel.getRemoteAddress().toString();

        if(cachedImages.containsKey(remoteAddr)) {
            imageLength = cachedImages.get(remoteAddr).imageLength;
        } else {
            byte[] imageLengthBytes = new byte[8];
            buffer.get(imageLengthBytes, 0, 8);

            ByteBuffer imageLengthBuffer = ByteBuffer.allocate(8);
            imageLengthBuffer.put(imageLengthBytes);
            imageLengthBuffer.flip();
            imageLength = imageLengthBuffer.getLong();
        }

        return imageLength;
    }

    private Long getHasReadImageLength(SocketChannel channel) throws Exception {
        long hasReadImageLength = 0;
        String remoteAddr = channel.getRemoteAddress().toString();
        if(cachedImages.containsKey(remoteAddr)) {
            hasReadImageLength = cachedImages.get(remoteAddr).hasReadImageLength;
        }
        return hasReadImageLength;
    }

    private Filename getFileName(SocketChannel channel, ByteBuffer buffer) throws Exception {
        Filename filename = new Filename();
        String remoteAddr = channel.getRemoteAddress().toString();

        if(cachedImages.containsKey(remoteAddr)) {
            filename = cachedImages.get(remoteAddr).filename;
        } else {
            String relativeFilename = getFilenameFromChannel(channel, buffer);
            if(filename == null) {
                return null;
            }
            filename.relativeFilename = relativeFilename;
            // /image/product/iphone.jpg
            //NameNodeConstants.imagePath + DataNodeConfig.DATANODE_HOSTNAME
            String[] relativeFilenameSplited = relativeFilename.split("/");

            StringBuilder sbDirPath = new StringBuilder(DataNodeConfig.DATANODE_FILE_PATH);
            for(int i = 0; i < relativeFilenameSplited.length - 1; i++) {
                sbDirPath.append("/")
                        .append(relativeFilenameSplited[i]);
            }

            File dir = new File(sbDirPath.toString());
            if(!dir.exists()) {
                dir.mkdirs();
            }

            String absoluteFilename = sbDirPath
                    .append("/")
                    .append(relativeFilenameSplited[relativeFilenameSplited.length - 1])
                    .toString();
            filename.absoluteFilename = absoluteFilename;
        }

        return filename;
    }

    private String getFilenameFromChannel(SocketChannel channel, ByteBuffer buffer) throws IOException {
        int read = channel.read(buffer);
        if(read > 0) {
            buffer.flip();
            byte[] fileNameLengthBytes = new byte[4];
            buffer.get(fileNameLengthBytes, 0, 4);

            int fileNameLength = ByteUtils.bytes2Int(fileNameLengthBytes);

            byte[] fileNameBytes = new byte[fileNameLength];
            buffer.get(fileNameBytes, 0, fileNameLength);
            String fileName = new String(fileNameBytes);
            return fileName;
        }
        return null;
    }

    /**
     * 文件名
     * @author zhonghuashishan
     *
     */
    class Filename {

        // 相对路径名
        String relativeFilename;
        // 绝对路径名
        String absoluteFilename;

        @Override
        public String toString() {
            return "Filename [relativeFilename=" + relativeFilename + ", absoluteFilename=" + absoluteFilename + "]";
        }

    }

    class CachedImage {

        Filename filename;
        long imageLength;
        long hasReadImageLength;

        public CachedImage(Filename filename, long imageLength, long hasReadImageLength) {
            this.filename = filename;
            this.imageLength = imageLength;
            this.hasReadImageLength = hasReadImageLength;
        }

        @Override
        public String toString() {
            return "CachedImage [filename=" + filename + ", imageLength=" + imageLength + ", hasReadImageLength="
                    + hasReadImageLength + "]";
        }

    }
}

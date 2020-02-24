package com.github.dfs.datanode.server;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static com.github.dfs.datanode.server.DataNodeConfig.NIO_PORT;

public class DataNodeNIOServer extends Thread {

    public static final Integer SEND_FILE = 1;
    public static final Integer READ_FILE = 2;

    private Selector selector;

    private NameNodeRpcClient nameNodeRpcClient;

    private List<LinkedBlockingQueue<SelectionKey>> queues =
            new ArrayList<>();
    // 缓存没读取完的文件数据
    private Map<String, CachedRequest> cachedRequests = new ConcurrentHashMap<>();
    // 缓存没读取完的请求类型
    private Map<String, ByteBuffer> requestTypeByClient = new ConcurrentHashMap<>();
    // 缓存没读取完的文件名大小
    private Map<String, ByteBuffer> filenameLengthByClient = new ConcurrentHashMap<>();
    // 缓存没读取完的文件名
    private Map<String, ByteBuffer> filenameByClient = new ConcurrentHashMap<>();
    // 缓存没读取完的文件大小
    private Map<String, ByteBuffer> fileLengthByClient = new ConcurrentHashMap<>();

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
                    //获取请求类型，1-发送文件 2-读取文件 0-不处理
                    Integer requestType = getRequestType(channel);
                    if(requestType == null) {
                        channel.close();
                        continue;
                    }
                    Filename fileName = getFileName(channel);
                    if(requestType.equals(SEND_FILE)) {
                        saveFileToDIsk(channel, key, remoteAddr, fileName);
                    } else if(requestType.equals(READ_FILE)) {
                        sendFileToClient(channel, key, fileName, remoteAddr);
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

    private void sendFileToClient(SocketChannel channel, SelectionKey key, Filename fileName, String remoteAddr) {
        File file = new File(fileName.absoluteFilename);
        if(!file.exists()) {
            throw new IllegalArgumentException(fileName + " not exist!");
        }
        try(FileInputStream imageIn = new FileInputStream(file);
            FileChannel imageChannel = imageIn.getChannel()) {

            //8个字节表示文件大小
            ByteBuffer outBuffer = ByteBuffer.allocate((int)file.length() + 8);
            outBuffer.putLong(file.length());
            imageChannel.read(outBuffer);
            outBuffer.rewind();
            channel.write(outBuffer);
            System.out.println("发送文件" + fileName +"返回客户端" + remoteAddr);

            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void saveFileToDIsk(SocketChannel channel, SelectionKey key, String remoteAddr, Filename fileName) throws Exception {
        //文件大小
        Long imageLength = getFileLength(channel);
        //如果没有获取到文件长度，不再继续往下走
        if(imageLength == null) {
            return;
        }
        System.out.println("解析出文件大小：" + imageLength);
        long hasReadImageLength = getHasReadImageLength(remoteAddr);
        System.out.println("初始化已经读取的文件大小：" + hasReadImageLength);
        try(FileOutputStream imageOut = new FileOutputStream(fileName.absoluteFilename);
            FileChannel imageChannel = imageOut.getChannel();) {
            imageChannel.position(imageChannel.size());

            // 循环不断的从channel里读取数据，并写入磁盘文件
            ByteBuffer fileBuffer = ByteBuffer.allocate(1024);
            int len = -1;
            while((len = channel.read(fileBuffer)) > 0) {
                hasReadImageLength += len;
                System.out.println("已经向本地磁盘文件写入了" + hasReadImageLength + "字节的数据");
                fileBuffer.flip();
                imageChannel.write(fileBuffer);
                fileBuffer.clear();
            }
        }

        // 如果已经读取完毕，就返回一个成功给客户端
        if(hasReadImageLength == imageLength) {
            ByteBuffer outBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
            channel.write(outBuffer);
            removeCache(remoteAddr);
            System.out.println("文件读取完毕，返回响应给客户端: " + remoteAddr);
            //增量上报给namenode，接收到的文件信息
            nameNodeRpcClient.informReplicaReceived(fileName.relativeFilename, DataNodeConfig.DATANODE_HOSTNAME, DataNodeConfig.DATANODE_IP);
            //结束后取消对事件的监听
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        }
        // 如果一个文件没有读完，缓存起来，等待下一次读取
        else {
            getCachedRequest(remoteAddr).hasReadFileLength = hasReadImageLength;
            System.out.println("文件没有读取完毕，等待下一次OP_READ请求，缓存文件：" + cachedRequests);
        }
    }

    private void removeCache(String remoteAddr) {
        cachedRequests.remove(remoteAddr);
        fileLengthByClient.remove(remoteAddr);
        filenameByClient.remove(remoteAddr);
        requestTypeByClient.remove(remoteAddr);
    }

    private Long getHasReadImageLength(String remoteAddr) throws Exception {
        long hasReadImageLength = 0;
        if(cachedRequests.containsKey(remoteAddr)) {
            hasReadImageLength = cachedRequests.get(remoteAddr).hasReadFileLength;
        }
        return hasReadImageLength;
    }

    private Filename getFileName(SocketChannel channel) throws Exception {
        Filename filename = new Filename();
        String remoteAddr = channel.getRemoteAddress().toString();
        Filename cacheFileName = getCachedRequest(remoteAddr).filename;
        if(cacheFileName != null) {
            filename = cacheFileName;
        } else {
            String relativeFilename = getFilenameFromChannel(channel);
            if(relativeFilename == null) {
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
            System.out.println("解析出文件名：" + absoluteFilename);
            CachedRequest cachedRequest = getCachedRequest(remoteAddr);
            cachedRequest.filename = filename;
        }

        return filename;
    }

    private Integer getRequestType(SocketChannel channel) throws Exception {
        Integer requestType = null;
        String client = channel.getRemoteAddress().toString();

        if(getCachedRequest(client).reqeustType != null) {
            return getCachedRequest(client).reqeustType;
        }

        ByteBuffer requestTypeBuffer = null;
        if(requestTypeByClient.containsKey(client)) {
            requestTypeBuffer = requestTypeByClient.get(client);
        } else {
            requestTypeBuffer = ByteBuffer.allocate(4);
        }
        channel.read(requestTypeBuffer);

        if(!requestTypeBuffer.hasRemaining()) {
            // 已经读取出来了4个字节，可以提取出来requestType了
            requestTypeBuffer.rewind(); // 将position变为0，limit还是维持着4
            requestType = requestTypeBuffer.getInt();
            System.out.println("从请求中解析出来本次请求的类型：" + requestType);

            requestTypeByClient.remove(client);

            CachedRequest cachedRequest = getCachedRequest(client);
            cachedRequest.reqeustType = requestType;
        } else {
            requestTypeByClient.put(client, requestTypeBuffer);
        }

        return requestType;
    }

    /**
     * 获取缓存的请求
     * @param client
     * @return
     */
    private CachedRequest getCachedRequest(String client) {
        CachedRequest cachedRequest = cachedRequests.get(client);
        if(cachedRequest == null) {
            cachedRequest = new CachedRequest();
            cachedRequests.put(client, cachedRequest);
        }
        return cachedRequest;
    }

    private String getFilenameFromChannel(SocketChannel channel) throws IOException {
        String client = channel.getRemoteAddress().toString();
        ByteBuffer fileNameLengthBuffer = ByteBuffer.allocate(4);
        channel.read(fileNameLengthBuffer);
        fileNameLengthBuffer.rewind();

        //todo 文件名长度的拆包处理  待完善
        int fileNameLength = fileNameLengthBuffer.getInt();

        // 读取文件名
        String fileName = null;
        ByteBuffer filenameBuffer = null;
        if(filenameByClient.containsKey(client)) {
            filenameBuffer = filenameByClient.get(client);
        } else {
            filenameBuffer = ByteBuffer.allocate(fileNameLength);
        }

        channel.read(filenameBuffer);

        if(!filenameBuffer.hasRemaining()) {
            filenameBuffer.rewind();
            fileName = new String(filenameBuffer.array());
            filenameByClient.remove(client);
        } else {
            filenameByClient.put(client, filenameBuffer);
        }
        return fileName;
    }

    /**
     * 从网络请求中获取文件大小
     * @param channel
     * @return
     * @throws Exception
     */
    private Long getFileLength(SocketChannel channel) throws IOException {
        Long fileLength = null;
        String client = channel.getRemoteAddress().toString();

        if(getCachedRequest(client).fileLength != null) {
            return getCachedRequest(client).fileLength;
        } else {
            ByteBuffer fileLengthBuffer = ByteBuffer.allocate(8);
            channel.read(fileLengthBuffer);
            if(!fileLengthBuffer.hasRemaining()) {
                fileLengthBuffer.rewind();
                fileLength = fileLengthBuffer.getLong();
                fileLengthByClient.remove(client);
                getCachedRequest(client).fileLength = fileLength;
            } else {
                fileLengthByClient.put(client, fileLengthBuffer);
            }
        }

        return fileLength;
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

    /**
     *
     */
    class CachedRequest {

        Integer reqeustType;
        Filename filename;
        Long fileLength;
        long hasReadFileLength;

        @Override
        public String toString() {
            return "CachedRequest{" +
                    "reqeustType=" + reqeustType +
                    ", filename=" + filename +
                    ", fileLength=" + fileLength +
                    ", hasReadFileLength=" + hasReadFileLength +
                    '}';
        }
    }
}

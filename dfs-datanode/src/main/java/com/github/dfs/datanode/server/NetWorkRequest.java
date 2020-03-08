package com.github.dfs.datanode.server;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * @author wangsz
 * @create 2020-03-08
 **/
public class NetWorkRequest {

    private Boolean hasCompleteRead;

    private SocketChannel channel;

    private SelectionKey key;

    /**
     * 缓存没读取完的文件数据
     */
    private CachedRequest cachedRequest;
    /**
     * 缓存没读取完的请求类型
     */
    private ByteBuffer requestTypeBuffer;
    /**
     * 缓存没读取完的文件名大小
     */
    private ByteBuffer filenameLengthBuffer;
    /**
     * 缓存没读取完的文件名
     */
    private ByteBuffer filenameBuffer;
    /**
     * 缓存没读取完的文件大小
     */
    private ByteBuffer fileLengthBuffer;

    @Setter
    @Getter
    private Integer processId;
    @Setter
    @Getter
    private String client;

    public void read() throws Exception {
        if(!channel.isOpen()) {
            channel.close();
            return;
        }
        String remoteAddr = channel.getRemoteAddress().toString();
        System.out.println("接收到客户端请求：" + remoteAddr);
        //获取请求类型，1-发送文件 2-读取文件 0-不处理
        Integer requestType = getRequestType(channel);
        if(requestType == null) {
            channel.close();
            return;
        }
        readFileName(channel);
        readDataFromChannle(channel, remoteAddr);
    }

    public boolean hasCompleteRead() {
        return hasCompleteRead;
    }

    private void readDataFromChannle(SocketChannel channel, String remoteAddr) throws Exception {
        //文件大小
        Long imageLength = getFileLength(channel);
        //如果没有获取到文件长度，不再继续往下走
        if(imageLength == null) {
            return;
        }
        System.out.println("解析出文件大小：" + imageLength);
        long hasReadImageLength = getHasReadImageLength(remoteAddr);
        System.out.println("初始化已经读取的文件大小：" + hasReadImageLength);
        ByteBuffer fileBuffer = getCachedFileBuffer(imageLength.intValue());
        int len = -1;
        while((len = channel.read(fileBuffer)) > 0) {
            hasReadImageLength += len;
            System.out.println("已经读取了" + hasReadImageLength + "字节的数据");
        }

        if(!fileBuffer.hasRemaining()) {
            fileBuffer.rewind();
            hasCompleteRead = true;
            System.out.println("本次文件读取完毕，总大小 = " + hasReadImageLength);
        }
        // 如果一个文件没有读完，缓存起来，等待下一次读取
        else {
            hasCompleteRead = false;
            getCachedRequest().hasReadFileLength = hasReadImageLength;
            System.out.println("文件没有读取完毕，等待下一次OP_READ请求");
        }
    }

    private Long getHasReadImageLength(String remoteAddr) throws Exception {
        long hasReadImageLength = 0;
        if(cachedRequest != null) {
            hasReadImageLength = cachedRequest.hasReadFileLength;
        }
        return hasReadImageLength;
    }

    private Filename readFileName(SocketChannel channel) throws Exception {
        Filename filename = new Filename();
        Filename cacheFileName = getCachedRequest().filename;
        if(cacheFileName != null) {
            filename = cacheFileName;
        } else {
            String relativeFilename = getFilenameFromChannel(channel);
            if(relativeFilename == null) {
                return null;
            }
            filename.relativeFilename = relativeFilename;
            String absoluteFilename = FileUtiles.getAbsoluteFileName(relativeFilename);
            filename.absoluteFilename = absoluteFilename;
            System.out.println("解析出文件名：" + absoluteFilename);
            CachedRequest cachedRequest = getCachedRequest();
            cachedRequest.filename = filename;
        }

        return filename;
    }

    private Integer getRequestType(SocketChannel channel) throws Exception {
        Integer requestType = null;
        CachedRequest cachedRequest = getCachedRequest();
        if(cachedRequest.reqeustType != null) {
            return cachedRequest.reqeustType;
        }

        if(requestTypeBuffer == null) {
            requestTypeBuffer = ByteBuffer.allocate(4);
        }
        channel.read(requestTypeBuffer);

        if(!requestTypeBuffer.hasRemaining()) {
            // 已经读取出来了4个字节，可以提取出来requestType了
            requestTypeBuffer.rewind(); // 将position变为0，limit还是维持着4
            requestType = requestTypeBuffer.getInt();
            System.out.println("从请求中解析出来本次请求的类型：" + requestType);
            cachedRequest.reqeustType = requestType;
        }

        return requestType;
    }

    /**
     * 获取缓存的请求
     * @return
     */
    public CachedRequest getCachedRequest() {
        if(cachedRequest == null) {
            cachedRequest = new CachedRequest();
        }
        return cachedRequest;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public SelectionKey getKey() {
        return key;
    }

    private ByteBuffer getCachedFileBuffer(int fileLength) {
        CachedRequest cachedRequest = getCachedRequest();
        if(cachedRequest.fileBuffer == null) {
            ByteBuffer fileBuffer = ByteBuffer.allocate(fileLength);
            cachedRequest.fileBuffer = fileBuffer;
        }
        return cachedRequest.fileBuffer;
    }

    private String getFilenameFromChannel(SocketChannel channel) throws IOException {
        CachedRequest cachedRequest = getCachedRequest();
        int fileNameLength;
        if(cachedRequest.fileLength != null) {
            fileNameLength = cachedRequest.fileLength.intValue();
        } else {
            if(filenameLengthBuffer == null) {
                filenameLengthBuffer = ByteBuffer.allocate(4);
            }
            channel.read(filenameLengthBuffer);
            filenameLengthBuffer.rewind();

            fileNameLength = filenameLengthBuffer.getInt();
        }

        // 读取文件名
        String fileName = null;
        if(filenameBuffer == null) {
            filenameBuffer = ByteBuffer.allocate(fileNameLength);
        }

        channel.read(filenameBuffer);

        if(!filenameBuffer.hasRemaining()) {
            filenameBuffer.rewind();
            fileName = new String(filenameBuffer.array());
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
        CachedRequest cachedRequest = getCachedRequest();
        if(cachedRequest.fileLength != null) {
            return cachedRequest.fileLength;
        } else {
            if(fileLengthBuffer == null) {
                fileLengthBuffer = ByteBuffer.allocate(8);
            }
            channel.read(fileLengthBuffer);
            if(!fileLengthBuffer.hasRemaining()) {
                fileLengthBuffer.rewind();
                fileLength = fileLengthBuffer.getLong();
                cachedRequest.fileLength = fileLength;
            }
        }

        return fileLength;
    }

    public void setChannel(SocketChannel channel) {
        this.channel = channel;
    }

    public void setKey(SelectionKey key) {
        this.key = key;
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
        ByteBuffer fileBuffer;
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

    public Integer getRequestType() {
        CachedRequest cacheRequest = getCachedRequest();
        return cacheRequest.reqeustType;
    }
}

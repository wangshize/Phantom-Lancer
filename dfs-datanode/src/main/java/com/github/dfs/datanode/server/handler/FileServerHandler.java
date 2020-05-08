package com.github.dfs.datanode.server.handler;

import com.github.dfs.common.entity.FileInfo;
import com.github.dfs.datanode.server.DataNodeConfig;
import com.github.dfs.datanode.server.NameNodeRpcClient;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.*;

/**
 * @author wangsz
 * @create 2020-03-30
 **/
public class FileServerHandler extends ChannelInboundHandlerAdapter {

    private NameNodeRpcClient nameNodeRpcClient;

    public static final Integer SEND_FILE = 1;
    public static final Integer READ_FILE = 2;

    public FileServerHandler(NameNodeRpcClient nameNodeRpcClient) {
        this.nameNodeRpcClient = nameNodeRpcClient;
    }

    private ExecutorService ioExcutor = new ThreadPoolExecutor(
            1, 3, 10000, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(100), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "dfsFileThread");
        }
    });

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        NetWorkRequest request = (NetWorkRequest) msg;
        ioExcutor.submit(() -> {
            Integer requestType = request.getRequestType();
            NetWorkResponse response;
            try {
                if (SEND_FILE.equals(requestType)) {
                    response = saveFileToDIsk(request);
                } else if (READ_FILE.equals(requestType)) {
                    response = sendFileToClient(request);
                } else {
                    response = new NetWorkResponse();
                    response.setResponse(NetWorkResponse.UN_SUPPORT_OP);
                }
                ctx.writeAndFlush(response);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private NetWorkResponse sendFileToClient(NetWorkRequest request) {
        NetWorkResponse response = new NetWorkResponse();
        String absoluteFilename = request.getAbsoluteFilename();
        File file = new File(absoluteFilename);
        if(!file.exists()) {
            //返回不存在的响应
            response.setResponse(NetWorkResponse.FILE_NOT_EXIST);
            return response;
        }
        try(FileInputStream imageIn = new FileInputStream(file);
            FileChannel fileChannel = imageIn.getChannel()) {
            //8个字节表示文件大小
            ByteBuffer outBuffer = ByteBuffer.allocate((int)file.length() + 8);
            outBuffer.putLong(file.length());
            fileChannel.read(outBuffer);
            outBuffer.rewind();
            response.setResBuffer(outBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        response.setResponse(NetWorkResponse.SUCCESS);
        return response;
    }

    private NetWorkResponse saveFileToDIsk(NetWorkRequest request) throws Exception {
        try (FileOutputStream imageOut = new FileOutputStream(request.getAbsoluteFilename());
             FileChannel imageChannel = imageOut.getChannel()) {
            imageChannel.position(imageChannel.size());
            imageChannel.write(request.getFileBuffer());
            System.out.println("文件读取完毕，返回响应给客户端");
            //增量上报给namenode，接收到的文件信息
            FileInfo fileInfo = new FileInfo(request.getRelativeFilename(), request.getFileLength());
            nameNodeRpcClient.informReplicaReceived(fileInfo, DataNodeConfig.DATANODE_HOSTNAME, DataNodeConfig.DATANODE_IP);

            NetWorkResponse response = new NetWorkResponse();
            response.setResponse(NetWorkResponse.SUCCESS);
            return response;
        }
    }
}

package com.github.dfs.datanode.server.handler;

import lombok.Data;
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
@Data
public class NetWorkRequest {

    private String relativeFilename;

    private String absoluteFilename;

    Integer requestType;

    Long fileLength;

    ByteBuffer fileBuffer;

}

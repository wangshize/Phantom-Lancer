package com.github.dfs.datanode.server;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

/**
 * @author wangsz
 * @create 2020-03-08
 **/
public class NetWorkResponse {

    @Setter
    @Getter
    private ByteBuffer buffer;
    @Setter
    @Getter
    private String client;

}

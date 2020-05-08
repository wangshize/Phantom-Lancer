package com.github.dfs.datanode.server.handler;

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
    private Integer response;

    @Setter
    @Getter
    private ByteBuffer resBuffer;

    public static int SUCCESS = 0;
    public static int FILE_NOT_EXIST = 1;
    public static int UN_SUPPORT_OP = 2;
    public static int UNKNOWN_ERROR = 3;

}

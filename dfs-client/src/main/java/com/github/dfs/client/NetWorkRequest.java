package com.github.dfs.client;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangsz
 * @create 2020-03-09
 **/
@Setter
@Getter
@Builder
public class NetWorkRequest {

    public static final Integer SEND_FILE = 1;
    public static final Integer READ_FILE = 2;

    public static final Integer REQUEST_TYPE = 4;

    private String requestId;
    private Integer requestType;
    private String hostName;
    private Integer nioPort;
    private ByteBuffer byteBuffer;
    private Long timeOut = 100L;
    private Boolean async;
}

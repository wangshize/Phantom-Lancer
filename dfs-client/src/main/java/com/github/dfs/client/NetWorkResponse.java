package com.github.dfs.client;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

/**
 * @author wangsz
 * @create 2020-03-10
 **/
@Setter
@Getter
public class NetWorkResponse {

    public static final String SUCCESS = "SUCCESS";

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private ByteBuffer buffer;
    private String requestId;
}

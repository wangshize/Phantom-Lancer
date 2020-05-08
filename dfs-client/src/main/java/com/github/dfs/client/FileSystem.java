package com.github.dfs.client;

/**
 * @author wangsz
 * @create 2020-01-28
 **/
public interface FileSystem {

    void shutdown();

    void upload(byte[] file, String fileName, long fileSize,
                ResponseCallBack callBack) throws Exception;

    byte[] download(String fileName);
}

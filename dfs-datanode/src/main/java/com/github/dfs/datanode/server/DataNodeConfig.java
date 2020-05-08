package com.github.dfs.datanode.server;

import com.github.dfs.common.NameNodeConstants;

/**
 * @author wangsz
 * @create 2020-02-15
 **/
public class DataNodeConfig {

    public static String NAMENODE_HOSTNAME = "localhost";
    public static Integer NAMENODE_PORT = 56789;

    public static String DATANODE_IP = "127.0.0.1";
    public static String DATANODE_HOSTNAME = "dfs-data-02";
    public static String DATANODE_FILE_PATH = NameNodeConstants.imagePath + DATANODE_HOSTNAME;

    public static Integer NIO_PORT = 9002;

    public static Integer REPLICATE_THREAD_NUM = 3;

    public static int MAX_SIZE_FILE = 50 * 1024 * 1024;

}

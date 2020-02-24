package com.github.dfs.datanode.server;

import com.github.dfs.namenode.NameNodeConstants;

/**
 * @author wangsz
 * @create 2020-02-15
 **/
public class DataNodeConfig {

    public static final String NAMENODE_HOSTNAME = "localhost";
    public static final Integer NAMENODE_PORT = 56789;

    public static final String DATANODE_IP = "127.0.0.1";
    public static final String DATANODE_HOSTNAME = "dfs-data-03";
    public static final String DATANODE_FILE_PATH = NameNodeConstants.imagePath + DATANODE_HOSTNAME;

    public static final Integer NIO_PORT = 9003;
}

package com.github.dfs.common;

/**
 * @author wangsz
 * @create 2020-02-01
 **/
public class NameNodeConstants {

    public static final int DUPLICATE_DATANODE_NUM = 2;

    /**
     * 单块缓冲区大小 512kb
     */
    public static final Integer EDIT_LOG_BUFFER_LIMIT = 25 * 1024;

    public static final String root = "/Users/wangsz/dfsfile/";

    public static final String imagePath =  root + "fsimage/";

    public static final String fsimageFilePath = root + "editslog/fsimage/fsimage.meta";

    public static final String editelogPath = root + "editslog/";

    public static final String checkPointTxIdPath = root+ "editslog/fsimage/checkPointTxId.meta";

    public static final String CHECKPOINTTXIDPATH_BACKUPNODE = root + "backupnode/checkPointTxId.meta";

}

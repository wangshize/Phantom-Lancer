package com.github.dfs.namenode;

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

    public static final String imagePath = "/Users/wangsz/SourceCode/fsimage/";

    public static final String fsimageFilePath = "/Users/wangsz/SourceCode/editslog/fsimage/fsimage.meta";

    public static final String editelogPath = "/Users/wangsz/SourceCode/editslog/";

    public static final String checkPointTxIdPath = "/Users/wangsz/SourceCode/editslog/fsimage/checkPointTxId.meta";

    public static final String CHECKPOINTTXIDPATH_BACKUPNODE = "/Users/wangsz/SourceCode/backupnode/checkPointTxId.meta";

}

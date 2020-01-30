package com.github.dfs.namenode.server;

/**
 * 刷盘的txid范围
 * @author wangsz
 * @create 2020-01-29
 **/
public class FlushedFileMapper {

    private long startTxid;
    private long endTxid;
    private String filePath;

    public FlushedFileMapper(long startTxid, long endTxid, String filePath) {
        this.startTxid = startTxid;
        this.endTxid = endTxid;
        this.filePath = filePath;
    }

    public boolean isBetween(long txId) {
        return startTxid <= txId && endTxid >=txId;
    }

    public long getStartTxid() {
        return startTxid;
    }

    public void setStartTxid(long startTxid) {
        this.startTxid = startTxid;
    }

    public long getEndTxid() {
        return endTxid;
    }

    public void setEndTxid(long endTxid) {
        this.endTxid = endTxid;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}

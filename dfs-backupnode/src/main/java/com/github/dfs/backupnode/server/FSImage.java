package com.github.dfs.backupnode.server;

/**
 * @author wangsz
 * @create 2020-01-31
 **/
public class FSImage {

    private long maxTxid;
    private String fsImageJson;

    public FSImage(long maxTxid, String fsImageJson) {
        this.maxTxid = maxTxid;
        this.fsImageJson = fsImageJson;
    }

    public long getMaxTxid() {
        return maxTxid;
    }

    public void setMaxTxid(long maxTxid) {
        this.maxTxid = maxTxid;
    }

    public String getFsImageJson() {
        return fsImageJson;
    }

    public void setFsImageJson(String fsImageJson) {
        this.fsImageJson = fsImageJson;
    }
}

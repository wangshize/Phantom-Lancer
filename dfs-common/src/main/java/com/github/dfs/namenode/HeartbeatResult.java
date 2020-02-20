package com.github.dfs.namenode;

/**
 * @author wangsz
 * @create 2020-02-19
 **/
public enum HeartbeatResult {
    SUCCESS(1,"心跳续约成功"),
    FAIL(2,"心跳续约失败");

    private int status;
    private String desc;

    HeartbeatResult(int status, String desc) {
        this.status = status;
        this.desc = desc;
    }

    public int getStatus() {
        return status;
    }

    public String getDesc() {
        return desc;
    }

    public static HeartbeatResult fromCode(int code) {
        for (HeartbeatResult value : HeartbeatResult.values()) {
            if(value.getStatus() == code) {
                return value;
            }
        }
        return null;
    }
}

package com.github.dfs.namenode;

/**
 * @author wangsz
 * @create 2020-02-19
 **/
public enum RegisterResult {
    SUCCESS(1,"注册成功"),
    REPEAT(2,"重复注册"),
    FAIL(3,"注册失败");

    private int status;
    private String desc;

    RegisterResult(int status, String desc) {
        this.status = status;
        this.desc = desc;
    }

    public int getStatus() {
        return status;
    }

    public String getDesc() {
        return desc;
    }

    public static RegisterResult fromCode(int code) {
        for (RegisterResult value : RegisterResult.values()) {
            if(value.getStatus() == code) {
                return value;
            }
        }
        return null;
    }
}

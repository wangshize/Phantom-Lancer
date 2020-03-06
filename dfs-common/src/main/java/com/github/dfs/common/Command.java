package com.github.dfs.common;

import lombok.Getter;
import lombok.Setter;

/**
 * @author wangsz
 * @create 2020-02-19
 **/
public class Command {

    public static final Integer REGISTER = 1;
    public static final Integer REPORT_COMPLETE_STORAGE_INFO = 2;
    public static final Integer REPLICATE = 3;
    public static final Integer REMOVE = 4;

    @Setter
    @Getter
    private Integer type;
    @Setter
    @Getter
    private String content;

    public Command(Integer type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "Command{" +
                "type=" + type +
                ", content='" + content + '\'' +
                '}';
    }
}

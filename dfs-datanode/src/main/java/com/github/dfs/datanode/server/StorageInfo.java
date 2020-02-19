package com.github.dfs.datanode.server;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangsz
 * @create 2020-02-18
 **/
@Getter
@Setter
public class StorageInfo {

    private List<String> fileNames = new ArrayList<>();
    private Long storedDataSize;

    public void addFilename(String fileNames) {
        this.fileNames.add(fileNames);
    }
    public void addStoredDataSize(Long storedDataSize) {
        this.storedDataSize += storedDataSize;
    }
}

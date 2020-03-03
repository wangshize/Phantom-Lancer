package com.github.dfs.datanode.server;

import com.github.dfs.common.entity.FileInfo;
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

    private List<FileInfo> fileInfos = new ArrayList<>();
    private Long storedDataSize;

    public void addFilename(FileInfo fileInfo) {
        this.fileInfos.add(fileInfo);
    }
    public void addStoredDataSize(Long storedDataSize) {
        this.storedDataSize += storedDataSize;
    }
}

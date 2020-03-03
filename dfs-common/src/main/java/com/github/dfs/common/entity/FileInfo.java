package com.github.dfs.common.entity;

/**
 * @author wangsz
 * @create 2020-03-01
 **/
public class FileInfo {
    private String fileName;
    private Long fileLength;

    public FileInfo(String fileName, Long fileLength) {
        this.fileName = fileName;
        this.fileLength = fileLength;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Long getFileLength() {
        return fileLength;
    }

    public void setFileLength(Long fileLength) {
        this.fileLength = fileLength;
    }
}

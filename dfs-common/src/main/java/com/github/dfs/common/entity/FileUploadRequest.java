package com.github.dfs.common.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * @author wangsz
 * @create 2020-05-08
 **/
@Getter
@Setter
public class FileUploadRequest {

    private byte[] fileBytes;
    private String fileName;
}

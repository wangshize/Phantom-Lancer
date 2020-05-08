package com.github.dfs.common;

import cn.hutool.core.io.FileUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author wangsz
 * @create 2020-02-01
 **/
public class IOUitls {

    public static void writeFile(String filePath, byte[] bytes) throws IOException {
        File file = FileUtil.touch(filePath);
        try(RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            FileOutputStream fout = new FileOutputStream(randomAccessFile.getFD());
            FileChannel logFileChannel = fout.getChannel()) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            logFileChannel.write(byteBuffer);
            //强制刷盘
            logFileChannel.force(false);
        } catch (IOException e) {
            throw e;
        }
    }

}

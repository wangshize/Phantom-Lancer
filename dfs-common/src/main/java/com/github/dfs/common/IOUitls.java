package com.github.dfs.common;

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

    public static void wiriteFile(String filePath, byte[] bytes) throws IOException {
        try(RandomAccessFile file = new RandomAccessFile(filePath, "rw");
            FileOutputStream fout = new FileOutputStream(file.getFD());
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

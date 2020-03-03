package com.github.dfs.datanode.server;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author wangsz
 * @create 2020-03-02
 **/
public class FileUtiles {

    public static String getAbsoluteFileName(String relativeFilename) {
        // /image/product/iphone.jpg
        //NameNodeConstants.imagePath + DataNodeConfig.DATANODE_HOSTNAME
        String[] relativeFilenameSplited = relativeFilename.split("/");

        StringBuilder sbDirPath = new StringBuilder(DataNodeConfig.DATANODE_FILE_PATH);
        for(int i = 0; i < relativeFilenameSplited.length - 1; i++) {
            sbDirPath.append("/")
                    .append(relativeFilenameSplited[i]);
        }

        File dir = new File(sbDirPath.toString());
        if(!dir.exists()) {
            dir.mkdirs();
        }

        return sbDirPath
                .append("/")
                .append(relativeFilenameSplited[relativeFilenameSplited.length - 1])
                .toString();
    }

    public static void saveFile(String absoluteFilename, byte[] fileData) throws IOException {
        try(FileOutputStream imageOut = new FileOutputStream(absoluteFilename);
            FileChannel imageChannel = imageOut.getChannel()) {
            imageChannel.position(imageChannel.size());

            ByteBuffer fileBuffer = ByteBuffer.wrap(fileData);
            imageChannel.write(fileBuffer);
        }
    }
}

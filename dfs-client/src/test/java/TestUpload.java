import com.github.dfs.client.FileSystem;
import com.github.dfs.client.FileSystemImpl;
import com.github.dfs.client.NetWorkResponse;
import com.github.dfs.client.ResponseCallBack;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author wangsz
 * @create 2020-02-16
 **/
public class TestUpload {

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = new FileSystemImpl();
        String filePath = "/Users/wangsz/Desktop/mac.png";
        File file = new File(filePath);
        String uploadFileName = "test.png";
        long fileSize = file.length();

        ByteBuffer buffer = ByteBuffer.allocate((int)fileSize);
        FileInputStream imageIn = new FileInputStream(file);
        FileChannel channel = imageIn.getChannel();
        channel.read(buffer);
        buffer.flip();
        fileSystem.upload(buffer.array(), uploadFileName, fileSize, new ResponseCallBack() {
            @Override
            public void process(NetWorkResponse response) {
                try {
                    if(Boolean.FALSE.equals(response.getSendSuccess())) {
                        fileSystem.upload(buffer.array(), uploadFileName, fileSize, null);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

//        download(uploadFileName, fileSystem);
    }

    public static void download(String fileName, FileSystem fileSystem) throws IOException {
        byte[] fileBytes = fileSystem.download(fileName);
        String filePath = "/Users/wangsz/SourceCode/fsimage/download/" + fileName;
        ByteBuffer fileBuffer = ByteBuffer.wrap(fileBytes);
        FileOutputStream fout = new FileOutputStream(filePath);
        FileChannel fileChannel = fout.getChannel();

        fileChannel.write(fileBuffer);

        fileChannel.close();
        fout.close();
    }
}

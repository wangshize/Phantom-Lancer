import com.github.dfs.client.FileSystem;
import com.github.dfs.client.FileSystemImpl;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author wangsz
 * @create 2020-02-16
 **/
public class TestUpload {

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = new FileSystemImpl();
        String filePath = "/Users/wangsz/Desktop/WechatIMG130.jpeg";
        File file = new File(filePath);
        String fileName = file.getName();
        long fileSize = file.length();

        ByteBuffer buffer = ByteBuffer.allocate((int)fileSize);
        FileInputStream imageIn = new FileInputStream(file);
        FileChannel channel = imageIn.getChannel();
        channel.read(buffer);
        buffer.flip();
        fileSystem.upload(buffer.array(), fileName, fileSize);
    }
}

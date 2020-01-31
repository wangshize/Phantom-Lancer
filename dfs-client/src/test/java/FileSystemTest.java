import com.github.dfs.client.FileSystem;
import com.github.dfs.client.FileSystemImpl;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wangsz
 * @create 2020-01-28
 **/
public class FileSystemTest {

    public static void main(String[] args) {
        FileSystem fileSystem = new FileSystemImpl();
        for (int i = 0; i < 10; i++) {
            int kafkaIndex = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; j < 100; j++) {
                        fileSystem.mkdir("/usr/local/kafka" + kafkaIndex +"/data" + j);
                    }
                }
            }).start();
        }
//        fileSystem.shutdown();
    }
}

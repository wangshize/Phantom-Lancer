import com.github.dfs.client.FileSystem;
import com.github.dfs.client.FileSystemImpl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author wangsz
 * @create 2020-01-28
 **/
public class FileSystemTest {

    public static void main(String[] args) {
        FileSystem fileSystem = new FileSystemImpl();
//        for (int i = 0; i < 10; i++) {
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    for (int i = 0; i < 100; i++) {
//                        fileSystem.mkdir("/usr/local/kafka/data" + i);
//                    }
//                }
//            }).start();
//        }
        fileSystem.shutdown();
    }
}

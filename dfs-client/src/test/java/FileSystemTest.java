import com.github.dfs.client.FileSystem;
import com.github.dfs.client.FileSystemImpl;

import java.util.concurrent.CountDownLatch;

/**
 * @author wangsz
 * @create 2020-01-28
 **/
public class FileSystemTest {

    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = new FileSystemImpl();
//        for (int i = 0; i < 1; i++) {
//            int kafkaIndex = i;
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    for (int j = 0; j < 1; j++) {
////                        fileSystem.mkdir("/usr/local/es" + kafkaIndex +"/data" + j);
//                        byte[] file = new byte[1024];
//                        try {
//                            fileSystem.upload(file,"/usr/local/es" +"/data" + j, 100);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            }).start();
//        }
        fileSystem.shutdown();
    }
}

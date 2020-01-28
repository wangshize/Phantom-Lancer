import com.github.dfs.client.FileSystem;
import com.github.dfs.client.FileSystemImpl;

/**
 * @author wangsz
 * @create 2020-01-28
 **/
public class FileSystemTest {

    public static void main(String[] args) {
        FileSystem fileSystem = new FileSystemImpl();
        fileSystem.mkdir("/usr/local/kafka/data");
    }
}

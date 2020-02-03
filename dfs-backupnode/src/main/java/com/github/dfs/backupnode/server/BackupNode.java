package com.github.dfs.backupnode.server;

/**
 * @author wangsz
 * @create 2020-01-29
 **/
public class BackupNode {

    private volatile boolean isRunning = true;
    private FSNamesystem namesystem;
    private NameNodeRpcClient rpcClient;

    public static void main(String[] args) throws Exception {
        BackupNode backupNode = new BackupNode();
        backupNode.init();
        backupNode.start();
        backupNode.run();
    }

    public void init() {
        this.namesystem = new FSNamesystem();
        this.rpcClient = new NameNodeRpcClient();
    }

    public void start() throws Exception {
        EditsLogFetcher fetcher = new EditsLogFetcher(this, namesystem, rpcClient);
        fetcher.start();

        FsImageCheckpointer checkpointer = new FsImageCheckpointer(this, namesystem, rpcClient);
        checkpointer.start();
    }

    public void run() throws Exception {
        while (isRunning) {
            Thread.sleep(1000);
        }
    }

    public boolean isRunning() {
        return isRunning;
    }
}

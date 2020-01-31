package com.github.dfs.backupnode.server;

/**
 * @author wangsz
 * @create 2020-01-29
 **/
public class BackupNode {

    private volatile boolean isRunning = true;
    private FSNamesystem namesystem;

    public static void main(String[] args) throws Exception {
        BackupNode backupNode = new BackupNode();
        backupNode.init();
        backupNode.start();
        backupNode.run();
    }

    public void init() {
        this.namesystem = new FSNamesystem();;
    }

    public void start() throws Exception {
        EditsLogFetcher fetcher = new EditsLogFetcher(this, namesystem);
        fetcher.start();

        FsImageCheckpointer checkpointer = new FsImageCheckpointer(this, namesystem);
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

package com.github.dfs.backupnode.server;

/**
 * @author wangsz
 * @create 2020-01-29
 **/
public class BackupNode {

    private volatile boolean isRunning = true;

    public static void main(String[] args) throws Exception {
        BackupNode backupNode = new BackupNode();
        backupNode.start();
        backupNode.run();
    }

    public void start() throws Exception {
        EditsLogFetcher fetcher = new EditsLogFetcher(this);
        fetcher.start();
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

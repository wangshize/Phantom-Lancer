package com.github.dfs.backupnode.server;

import java.util.List;

/**
 * 从namenode同步editslog
 * @author wangsz
 * @create 2020-01-29
 **/
public class EditsLogFetcher extends Thread {

    private BackupNode backupNode;
    private NameNodeRpcClient nameNode;
    private FSNamesystem namesystem;

    /**
     * 已经拉取到的edits log的txid
     */
    private long fetchedEditsLogTxId;

    public EditsLogFetcher(BackupNode backupNode, FSNamesystem namesystem) {
        this.backupNode = backupNode;
        this.nameNode = new NameNodeRpcClient();
        this.namesystem = namesystem;
    }

    public long getFetchedEditsLogTxId() {
        return fetchedEditsLogTxId;
    }

    @Override
    public void run() {
        while (backupNode.isRunning()) {
            List<EditLog> editLogs = nameNode.fetchEditsLog(fetchedEditsLogTxId + 1);
            System.out.println("从namenode拉取到的数量：" + editLogs.size());
            try {
                if(editLogs.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }
                for (EditLog editLog : editLogs) {
                    String op = editLog.getoP();
                    if(op.equals("MKDIR")) {
                        String path = editLog.getPath();
                        namesystem.mkdir(editLog.getTxid(), path);
                        fetchedEditsLogTxId = editLog.getTxid();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}

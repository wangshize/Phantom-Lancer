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

    public EditsLogFetcher(BackupNode backupNode, FSNamesystem namesystem,
                           NameNodeRpcClient nameNode) {
        this.backupNode = backupNode;
        this.nameNode = nameNode;
        this.namesystem = namesystem;
    }

    public long getFetchedEditsLogTxId() {
        return fetchedEditsLogTxId;
    }

    @Override
    public void run() {
        while (backupNode.isRunning()) {
            FetchEditsLogResult result = nameNode.fetchEditsLog(fetchedEditsLogTxId + 1);
            List<EditLog> editLogs = result.getEditLogs();
            System.out.println("从namenode拉取到的数量：" + editLogs.size());
            try {
                if(result.getStatus() == 3) {
                    System.out.println("namenode暂不可用，改变标志位，一秒后重试。。。。。。");
                    namesystem.setNameNodeRunning(false);
                    Thread.sleep(1000);
                    continue;
                } else if (result.getStatus() == 1) {
                    namesystem.setNameNodeRunning(true);
                }
                if(editLogs.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }
                for (EditLog editLog : editLogs) {
                    String op = editLog.getOpration();
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

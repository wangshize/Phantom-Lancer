package com.github.dfs.backupnode.server;

import com.alibaba.fastjson.JSONObject;

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
            List<EditLog> editLogs = nameNode.fetchEditsLog(fetchedEditsLogTxId);
            try {
                if(editLogs.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }
                for (EditLog editLog : editLogs) {
                    JSONObject log = JSONObject.parseObject(editLog.getContent());
                    String op = log.getString("OP");
                    if(op.equals("MKDIR")) {
                        String path = log.getString("PATH");
                        namesystem.mkdir(editLog.getTxid(), path);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}

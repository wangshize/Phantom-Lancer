package com.github.dfs.backupnode.server;

import java.util.List;

/**
 * @author wangsz
 * @create 2020-02-01
 **/
public class FetchEditsLogResult {

    List<EditLog> editLogs;
    int status;

    public List<EditLog> getEditLogs() {
        return editLogs;
    }

    public void setEditLogs(List<EditLog> editLogs) {
        this.editLogs = editLogs;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}

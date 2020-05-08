package com.github.dfs.backupnode.server;

import com.github.dfs.common.IOUitls;
import com.github.dfs.common.NameNodeConstants;

import java.io.IOException;

/**
 * 负责管理元数据的核心组件
 * @author zhonghuashishan
 *
 */
public class FSNamesystem {

	/**
	 * 负责管理内存文件目录树的组件
	 */
	private FSDirectory directory;

	private volatile boolean nameNodeRunning;

	public FSNamesystem() {
		this.directory = new FSDirectory();
	}
	
	/**
	 * 创建目录
	 * @return 是否成功
	 */
	public Boolean mkdir(EditLog log) throws Exception {
		this.directory.mkdir(log);
		return true;
	}

	public Boolean create(EditLog log) throws Exception {
		if(!directory.create(log)) {
			return false;
		}
		return true;
	}

	public void saveCheckPointTxId() {
		try {
			IOUitls.writeFile(NameNodeConstants.CHECKPOINTTXIDPATH_BACKUPNODE,
					String.valueOf(directory.getMaxTxid()).getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public FSImage getFSImageJson() {
		return directory.getFSImage();
	}

	public void resetDirChangeFlag() {
		directory.setDirTreeChange(false);
	}

	public boolean isDirChange() {
		return directory.isDirTreeChange();
	}

	public boolean isNameNodeRunning() {
		return nameNodeRunning;
	}

	public void setNameNodeRunning(boolean nameNodeRunning) {
		this.nameNodeRunning = nameNodeRunning;
	}
}

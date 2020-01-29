package com.github.dfs.namenode.server;

import com.alibaba.fastjson.JSON;

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
	/**
	 * 负责管理edits log写入磁盘的组件
	 */
	private FSEditlog editlog;
	
	public FSNamesystem() {
		this.directory = new FSDirectory();
		this.editlog = new FSEditlog();
	}
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否成功
	 */
	public Boolean mkdir(String path) throws Exception {
		this.directory.mkdir(path);
		EditLog log = new EditLog(FileOP.MKDIR, path);
		this.editlog.logEdit(JSON.toJSONString(log));
		return true;
	}

	class EditLog {
		FileOP OP;
		String path;

		public EditLog(FileOP OP, String path) {
			this.OP = OP;
			this.path = path;
		}

		public FileOP getOP() {
			return OP;
		}

		public void setOP(FileOP OP) {
			this.OP = OP;
		}

		public String getPath() {
			return path;
		}

		public void setPath(String path) {
			this.path = path;
		}
	}

	enum FileOP {
		MKDIR,
		REMOVE,
		CREATE;
	}

}

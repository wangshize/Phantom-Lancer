package com.github.dfs.backupnode.server;

import com.alibaba.fastjson.JSON;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 负责管理内存中的文件目录树的核心组件
 * @author zhonghuashishan
 *
 */
public class FSDirectory {
	
	/**
	 * 内存中的文件目录树
	 */
	private INode dirTree;

	private long maxTxid;

	private ReadWriteLock lock = new ReentrantReadWriteLock();

	private volatile boolean isDirTreeChange = false;

	public FSDirectory() {
		this.dirTree = new INode("/");
	}

	/**
	 * 获取文件目录树数据 json格式
	 * @return
	 */
	public FSImage getFSImage() {
		try {
			lock.readLock().lock();
			String fsimageJson = JSON.toJSONString(dirTree);
			FSImage fsImage = new FSImage(maxTxid, fsimageJson);
			//这里还需要当前文件目录树里最大的txid，这样才能去将该txid之前的edits log删除
			return fsImage;
		} finally {
			lock.readLock().unlock();
		}
	}
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 */
	public void mkdir(long txid, String path) {
		// path = /usr/warehouse/hive
		// 先判断一下，“/”根目录下有没有一个“usr”目录的存在
		// 如果说有的话，那么再判断一下，“/usr”目录下，有没有一个“/warehouse”目录的存在
		// 如果说没有，那么就得先创建一个“/warehosue”对应的目录，挂在“/usr”目录下
		// 接着再对“/hive”这个目录创建一个节点挂载上去

		//内存数据结构，更新的时候必须加锁
		try {
			lock.writeLock().lock();
			String[] pathes = path.split("/");
			INode parent = dirTree;
			
			for(String splitedPath : pathes) {
				if(splitedPath.trim().equals("")) {
					continue;
				}
				
				INode dir = findDirectory(parent, splitedPath);
				if(dir != null) {
					parent = dir;
					continue;
				}
				
				INode child = new INode(splitedPath);
				parent.addChild(child);
				parent = child;
			}
			maxTxid = txid;
			isDirTreeChange = true;
		} finally {
			lock.writeLock().unlock();
		}
	}

	public void remove(String path) {
		INode node = findDirectory(dirTree, path);
		if(node == null) {
			throw new IllegalArgumentException("目录不存在");
		}
	}

	public void update(String newPath, String oldPath) {

	}
	
	/**
	 * 对文件目录树递归查找目录
	 * @param dir
	 * @param path
	 * @return
	 */
	private INode findDirectory(INode dir, String path) {
		if(dir.getChildren().size() == 0) {
			return null;
		}
		
		for(INode child : dir.getChildren()) {
			if(child instanceof INode) {
				INode childDir = (INode) child;
				
				if((childDir.getPath().equals(path))) {
					return childDir;
				} 

			}
		}
		
		return null;
	}

    public boolean isDirTreeChange() {
        return isDirTreeChange;
    }

    public void setDirTreeChange(boolean dirTreeChange) {
        isDirTreeChange = dirTreeChange;
    }

	public long getMaxTxid() {
		return maxTxid;
	}

	public void setMaxTxid(long maxTxid) {
		this.maxTxid = maxTxid;
	}

	/**
	 * 代表文件目录树中的一个目录
	 * @author zhonghuashishan
	 *
	 */
	public static class INode {
		
		private String path;
		private List<INode> children;
		
		public INode(String path) {
			this.path = path;
			this.children = new LinkedList<INode>();
		}
		
		public void addChild(INode inode) {
			this.children.add(inode);
		}
		
		public String getPath() {
			return path;
		}
		public void setPath(String path) {
			this.path = path;
		}
		public List<INode> getChildren() {
			return children;
		}
		public void setChildren(List<INode> children) {
			this.children = children;
		}

		@Override
		public String toString() {
			return "INode{" +
					"path='" + path + '\'' +
					", children=" + children +
					'}';
		}
	}
	
}

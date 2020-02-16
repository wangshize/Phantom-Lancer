package com.github.dfs.namenode.server;

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

	private volatile boolean isDirTreeChange = false;

	private long maxTxid;

	private ReadWriteLock lock = new ReentrantReadWriteLock();
	
	public FSDirectory() {
		this.dirTree = new INode("/");
	}

	/**
	 * 创建目录
	 * @param path 目录路径
	 */
	public void mkdir(String path) {
		// path = /usr/warehouse/hive
		// 先判断一下，“/”根目录下有没有一个“usr”目录的存在
		// 如果说有的话，那么再判断一下，“/usr”目录下，有没有一个“/warehouse”目录的存在
		// 如果说没有，那么就得先创建一个“/warehosue”对应的目录，挂在“/usr”目录下
		// 接着再对“/hive”这个目录创建一个节点挂载上去

		//内存数据结构，更新的时候必须加锁
		synchronized(dirTree) {
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
		}
	}

	/**
	 * 创建文件
	 * @param fileName
	 * @return
	 */
	public Boolean create(String fileName) {

		synchronized (dirTree) {
			String[] splitedFilename = fileName.split("/");
			String realFilename = splitedFilename[splitedFilename.length - 1];
			INode parent = dirTree;

			for(int i = 0; i < splitedFilename.length - 1; i++) {
				if(i == 0) {
					continue;
				}

				INode dir = findDirectory(parent, splitedFilename[i]);
				if(dir != null) {
					parent = dir;
					continue;
				}

				INode child = new INode(splitedFilename[i]);
				parent.addChild(child);
				parent = child;
			}
			//此时parent = 文件的上一级目录
			List<INode> fileNodes = parent.getChildren();
			if(existFile(parent, realFilename)) {
				return false;
			}
			// 真正的在目录里创建一个文件出来
			INode file = new INode(realFilename);
			parent.addChild(file);
			return true;
		}
	}

	/**
	 * 目录下是否存在这个文件
	 * @param dir
	 * @param filename
	 * @return
	 */
	private Boolean existFile(INode dir, String filename) {
		if(dir.getChildren() != null && dir.getChildren().size() > 0) {
			for(INode child : dir.getChildren()) {
				if(child.getPath().equals(filename)) {
					return true;
				}
			}
		}
		return false;
	}

	public void remove(String path) {
		INode node = findDirectory(dirTree, path);
		if(node == null) {
			throw new IllegalArgumentException("目录不存在");
		}
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

	public INode getDirTree() {
		return dirTree;
	}

	public void setDirTree(INode dirTree) {
		this.dirTree = dirTree;
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

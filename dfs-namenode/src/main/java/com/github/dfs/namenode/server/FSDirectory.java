package com.github.dfs.namenode.server;

import java.util.LinkedList;
import java.util.List;

/**
 * 负责管理内存中的文件目录树的核心组件
 * @author zhonghuashishan
 *
 */
public class FSDirectory {
	
	/**
	 * 内存中的文件目录树
	 */
	private INodeDirectory dirTree;
	
	public FSDirectory() {
		this.dirTree = new INodeDirectory("/");  
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
			INodeDirectory parent = dirTree;
			
			for(String splitedPath : pathes) {
				if(splitedPath.trim().equals("")) {
					continue;
				}
				
				INodeDirectory dir = findDirectory(parent, splitedPath);
				if(dir != null) {
					parent = dir;
					continue;
				}
				
				INodeDirectory child = new INodeDirectory(splitedPath); 
				parent.addChild(child);
				parent = child;
			}
		}
//		printDirTree(dirTree, "-");
	}

	private void printDirTree(INodeDirectory dirTree, String blank) {
		if(dirTree.getChildren().size() == 0) {
			return;
		}
		for(INode dir : dirTree.getChildren()) {
			System.out.println(blank + ((INodeDirectory) dir).getPath());
			printDirTree((INodeDirectory) dir, blank + "-");
		}
	}

	public void remove(String path) {
		INodeDirectory node = findDirectory(dirTree, path);
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
	private INodeDirectory findDirectory(INodeDirectory dir, String path) {
		if(dir.getChildren().size() == 0) {
			return null;
		}
		
		for(INode child : dir.getChildren()) {
			if(child instanceof INodeDirectory) {
				INodeDirectory childDir = (INodeDirectory) child;
				
				if((childDir.getPath().equals(path))) {
					return childDir;
				} 

			}
		}
		
		return null;
	}
	
	
	/**
	 * 代表的是文件目录树中的一个节点
	 * @author zhonghuashishan
	 *
	 */
	private interface INode {
		
	}
	
	/**
	 * 代表文件目录树中的一个目录
	 * @author zhonghuashishan
	 *
	 */
	public static class INodeDirectory implements INode {
		
		private String path;
		private List<INode> children;
		
		public INodeDirectory(String path) {
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
			return "INodeDirectory{" +
					"path='" + path + '\'' +
					", children=" + children +
					'}';
		}
	}
	
	/**
	 * 代表文件目录树中的一个文件
	 * @author zhonghuashishan
	 *
	 */
	public static class INodeFile implements INode {
		
		private String name;

		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "INodeFile{" +
					"name='" + name + '\'' +
					'}';
		}
	}
	
}

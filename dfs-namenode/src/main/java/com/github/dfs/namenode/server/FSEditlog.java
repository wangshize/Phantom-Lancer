package com.github.dfs.namenode.server;

import java.io.IOException;
import java.util.LinkedList;

/**
 * 负责管理edits log日志的核心组件
 * @author zhonghuashishan
 *
 */
public class FSEditlog {

	/**
	 * 当前递增到的txid的序号
	 */
	private long txidSeq = 0L;
	/**
	 * 内存双缓冲区
	 */
	private DoubleBuffer editLogBuffer = new DoubleBuffer();
	/**
	 * 当前是否在将内存缓冲刷入磁盘中
	 */
	private volatile Boolean isSyncRunning = false;
	/**
	 * 是否正在调度刷盘操作
	 */
	private volatile Boolean isSchedulingSync = false;
	/**
	 * 在同步到磁盘中的最大的一个txid
	 */
	private volatile Long syncTxid = 0L;
	/**
	 * 每个线程自己本地的txid副本
	 */
	private ThreadLocal<Long> localTxid = new ThreadLocal<Long>();
	
	// 就会导致说，对一个共享的map数据结构出现多线程并发的读写的问题
	// 此时对这个map的读写是不是就需要加锁了
//	private Map<Thread, Long> txidMap = new HashMap<Thread, Long>();
	
	/**
	 * 记录edits log日志
	 * @param content
	 */
	public void logEdit(String content) {
		// 这里必须得直接加锁
		synchronized(this) {
			//检查是否正在调度刷盘操作，目的是为了交换两块缓冲区
			waitSchedulingSync();

			// 获取全局唯一递增的txid，代表了edits log的序号
			txidSeq++;
			long txid = txidSeq;
			localTxid.set(txid); // 放到ThreadLocal里去，相当于就是维护了一份本地线程的副本
			
			// 构造一条edits log对象
			EditLog log = new EditLog(txid, content); 
			
			// 将edits log写入内存缓冲中，不是直接刷入磁盘文件
			try {
				editLogBuffer.write(log);
			} catch (IOException e) {
				e.printStackTrace();
			}

			//每次写完一条log之后，检查一下当前缓冲区是否已经满了
			if(!editLogBuffer.shouldSyncToDisk()) {
				return;
			}
			isSchedulingSync = true;
		}
		
		logSync();
	}

	private void waitSchedulingSync() {
		try {
			while (isSchedulingSync) {
				wait(1000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 将内存缓冲中的数据刷入磁盘文件中
	 * 在这里尝试允许某一个线程一次性将内存缓冲中的数据刷入磁盘文件中
	 * 相当于实现一个批量将内存缓冲数据刷磁盘的过程
	 */
	private void logSync() {
		// 再次尝试加锁
		synchronized(this) {
			long txid = localTxid.get(); // 获取到本地线程的副本

			// 如果说当前正好有人在刷内存缓冲到磁盘中去
			if(isSyncRunning) {
				// 那么此时这里应该有一些逻辑判断
				
				// 假如说某个线程已经把txid = 1,2,3,4,5的edits log都从syncBuffer刷入磁盘了
				// 或者说此时正在刷入磁盘中
				// 此时syncTxid = 5，代表的是正在输入磁盘的最大txid
				// 那么这个时候来一个线程，他对应的txid = 3，此时他是可以直接返回了
				// 就代表说肯定是他对应的edits log已经被别的线程在刷入磁盘了
				// 这个时候txid = 3的线程就不需要等待了
				if(txid <= syncTxid) {
					return;
				}
				//如果有其他线程在刷盘，需要等待刷完后才能进行刷盘
				while(isSyncRunning) {
					try {
						wait(2000);
					} catch (Exception e) {
						e.printStackTrace();  
					}
				}
			}
			
			// 交换两块缓冲区
			editLogBuffer.setReadyToSync();
			// 然后可以保存一下当前要同步到磁盘中去的最大的txid
			// 此时editLogBuffer中的syncBuffer这块区域，交换完以后这里可能有多条数据
			// 而且他里面的edits log的txid一定是从小到大的
			// 此时要同步的txid = 6,7,8,9,10,11,12
			// syncTxid = 12
			syncTxid = txid;
			// 设置当前正在同步到磁盘的标志位
			isSyncRunning = true;
			//唤醒卡在waitSchedulingSync的线程
			isSchedulingSync = false;
			notifyAll();
		}
		
		// 开始同步内存缓冲的数据到磁盘文件里去
		// 这个过程其实是比较慢，基本上肯定是毫秒级了，弄不好就要几十毫秒
		try {
			editLogBuffer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		synchronized(this) {
			// 同步完了磁盘之后，就会将标志位复位，再释放锁
			isSyncRunning = false;
			// 唤醒可能正在等待他同步完磁盘的线程
			notifyAll();
		}
	}
	
}

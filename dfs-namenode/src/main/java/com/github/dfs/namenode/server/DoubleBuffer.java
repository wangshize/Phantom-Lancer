package com.github.dfs.namenode.server;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
	 * 内存双缓冲
	 * @author zhonghuashishan
	 *
	 */
public class DoubleBuffer {

	private static final String editelogPath = "/Users/wangsz/SourceCode/editslog/";

	/**
	 * 单块缓冲区大小 512kb
	 */
	private Integer EDIT_LOG_BUFFER_LIMIT = 25 * 1024;

	/**
	 * 是专门用来承载线程写入edits log
	 */
	private EditLogBuffer currentBuffer = new EditLogBuffer();
	/**
	 * 专门用来将数据同步到磁盘中去的一块缓冲
	 */
	private EditLogBuffer syncBuffer = new EditLogBuffer();

	/**
	 * 当前缓冲区最大的txid
	 */
	private long maxTxid;

	/**
	 * 上一次刷盘最大的txid
	 */
	private long lastMaxTxid;

	/**
	 * 将edits log写到内存缓冲里去
	 * @param log
	 */
	public void write(EditLog log) throws IOException {
		currentBuffer.write(log);
		maxTxid = log.getTxid();
	}

	/**
	 * 判断当前缓冲区是否满了，需要刷到磁盘
	 * @return
	 */
	public boolean shouldSyncToDisk() {
		if(currentBuffer.size() >= EDIT_LOG_BUFFER_LIMIT) {
			return true;
		}
		return false;
	}

	/**
	 * 交换两块缓冲区，为了同步内存数据到磁盘做准备
	 */
	public void setReadyToSync() {
		EditLogBuffer tmp = currentBuffer;
		currentBuffer = syncBuffer;
		syncBuffer = tmp;
	}

	/**
	 * 获取sync buffer缓冲区里的最大的一个txid
	 * @return
	 */
//	public Long getSyncMaxTxid() {
//		return syncBuffer.getLast().txid;
//	}

	/**
	 * 将syncBuffer缓冲区中的数据刷入磁盘中
	 */
	public void flush() throws IOException {
		syncBuffer.flush();
		syncBuffer.clear();
	}

	/**
	 * editslog  缓冲区
	 */
	class EditLogBuffer {

		private ByteArrayOutputStream buffer;

		private long endTxid;

		public EditLogBuffer() {
			buffer = new ByteArrayOutputStream(EDIT_LOG_BUFFER_LIMIT * 2);
		}

		public void write(EditLog log) throws IOException {
			endTxid = log.getTxid();
			buffer.write(log.getContent().getBytes());
			buffer.write("\n".getBytes());
			System.out.println("写入log：" + log.toString());
			System.out.println("当前缓冲区的大小：" + size());
		}

		public Integer size() {
			return buffer.size();
		}

		public void flush() throws IOException {
			ByteBuffer byteBuffer = ByteBuffer.wrap(buffer.toByteArray());

			try(RandomAccessFile file = new RandomAccessFile(editelogPath +
					"edits-log-" + ++lastMaxTxid + "-" + endTxid + ".log", "rw");
				FileOutputStream fout = new FileOutputStream(file.getFD());
				FileChannel logFileChannel = fout.getChannel()) {

				logFileChannel.write(byteBuffer);
				//强制刷盘
				logFileChannel.force(false);
			} catch (IOException e) {
				throw e;
			}
			lastMaxTxid = endTxid;
		}

		public void clear() {
			buffer.reset();
		}
	}

}
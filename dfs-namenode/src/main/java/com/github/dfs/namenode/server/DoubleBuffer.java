package com.github.dfs.namenode.server;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

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

	private List<FlushedFileMapper> txidFileMappers = new CopyOnWriteArrayList<>();

	public DoubleBuffer() {
		initFileMappers();
	}

	private void initFileMappers() {
		File editsLogDir = new File(editelogPath);
		File[] editsLogs = editsLogDir.listFiles();
		Arrays.sort(editsLogs);
		for (File editsLog : editsLogs) {
			String fileName = editsLog.getName();
			fileName = fileName.substring(0, fileName.lastIndexOf("."));
			String[] fileNameSplited = fileName.split("-");
			long startTxid = Long.valueOf(fileNameSplited[0]);
			long endTxid = Long.valueOf(fileNameSplited[1]);
			String filePath = editsLog.getPath();
			txidFileMappers.add(new FlushedFileMapper(startTxid, endTxid, filePath));
		}
	}
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
	 * 将syncBuffer缓冲区中的数据刷入磁盘中
	 */
	public void flush() throws IOException {
		syncBuffer.flush();
		syncBuffer.clear();
	}

	/**
	 * 已落盘的txid和文件路径映射
	 * @return
	 */
	public List<FlushedFileMapper> getTxidFileMapper() {
		return txidFileMappers;
	}

	public String[] getBufferEditsLog() {
		String editslogRawData = new String(currentBuffer.getBufferData());
		String[] splitedEditslogRawData = editslogRawData.split("\n");
		return splitedEditslogRawData;
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
			long startLastMaxTxid = lastMaxTxid + 1;
			String filePath = editelogPath +
					startLastMaxTxid + "-" + endTxid + ".log";
			try(RandomAccessFile file = new RandomAccessFile(filePath, "rw");
				FileOutputStream fout = new FileOutputStream(file.getFD());
				FileChannel logFileChannel = fout.getChannel()) {
				logFileChannel.write(byteBuffer);
				//强制刷盘
				logFileChannel.force(false);
				txidFileMappers.add(new FlushedFileMapper(startLastMaxTxid, endTxid, filePath));
			} catch (IOException e) {
				throw e;
			}
			lastMaxTxid = endTxid;
		}

		public void clear() {
			buffer.reset();
		}

		public byte[] getBufferData() {
			return buffer.toByteArray();
		}
	}

}
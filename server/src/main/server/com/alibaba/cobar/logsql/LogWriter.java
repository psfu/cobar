package com.alibaba.cobar.logsql;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class LogWriter extends Common implements Runnable {
	public LogWriter() {
		f = getFile();
	}

	static final int poolSize = 45 * 1024 * 1024;
	static final int safeSize = 15 * 1024 * 1024;
	static final int fileInterval = 1 * 60 * 60 * 1000;
	static final int checkInterval = 2 * 1000;
	static final String filePath = "./logs/sqllog_";
	static final String folderPath = "./logs";

	static final SimpleDateFormat fformatLong = new SimpleDateFormat("yyyyMMdd-HHmmss");
	static final SimpleDateFormat fformat = new SimpleDateFormat("yyyyMMdd-HH");

	Long lastFile = null;
	Long lastTime = null;
	private File f;
	private FileOutputStream fos;
	private FileChannel fc;

	@Override
	public void run() {

		log(10, "---->log init....");

		ByteBuffer bb;
		bb = ByteBuffer.allocate(poolSize);

		// log(1, bb.remaining());

		fc = getChannel(f, fc);

		lastFile = System.currentTimeMillis();
		for (;;) {
			try {
				writeLog(bb);
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					write0(bb, fc);
					// fc.close();
					// fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public FileChannel getChannel(File f, FileChannel fc) {
		try {
			if (fc != null && fc.isOpen()) {
				fc.close();
			}
			if (fos != null) {
				fos.close();
			}

			fos = new FileOutputStream(f);
			this.fc = fos.getChannel();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return this.fc;
	}

	public File getFile() {

		Long now = System.currentTimeMillis();
		String nowStr = fformatLong.format(new Date(now));
		File f = new File(filePath + nowStr + ".log");
		File fileDir;

		fileDir = new File(folderPath);
		if (!fileDir.exists()) {
			fileDir.mkdirs();
		}
		log(2, "f-->" + (System.currentTimeMillis() - now));
		log(10, f.getAbsolutePath());
		return f;
	}

	public void executeR(ByteBuffer bb, FileChannel fc, SQLLogList ll) throws Exception {

		// Long t1 = System.currentTimeMillis();
		// log(10, "--->" + Thread.currentThread().getName());
		try {
			while (!ll.empty()) {
				SQLLog l = ll.pop();
				// Thread.sleep(100);
				// log(1, l);
				if (bb.remaining() < safeSize) {
					log(2, "full write....");
					write0(bb, fc);
				}
				l.write(bb);
			}

			//
			// log(2, bb.position());

			dealLog(bb, fc);

			//
			// log(2, (System.currentTimeMillis() - t1) + "ms");
		} catch (Exception e) {

			e.printStackTrace();
			throw (e);
		}
	}

	public void dealLog(ByteBuffer bb, FileChannel fc) throws IOException {
		if (this.lastTime == null) {
			this.lastTime = System.currentTimeMillis();
		}

		//
		// log(1,"checkInterval...." + System.currentTimeMillis() +"-" +
		// (System.currentTimeMillis() - t01));

		if ((System.currentTimeMillis() - lastTime) > checkInterval) {
			write0(bb, fc);
			log(2, "interval write....");
			this.lastTime = System.currentTimeMillis();
		}

		Long currFile = System.currentTimeMillis();
		if ((currFile - this.lastFile) > fileInterval) {
			f = getFile();
			fc = getChannel(f, fc);
			this.lastFile = currFile;
			log(1, "file....");
		}
	}

	public void write0(ByteBuffer bb, FileChannel fc) throws IOException {
		long t = System.currentTimeMillis();
		bb.flip();
		fc.write(bb);
		bb.clear();
		log(1, "w-->" + (System.currentTimeMillis() - t));
	}

	static final ReentrantLock lock = new ReentrantLock();

	/** Condition for waiting takes */
	static final Condition notEmpty = lock.newCondition();

	
	// @Override
	public static boolean isRunning = false;

	public void writeLog(ByteBuffer bb) throws Exception {

		lock.lock();
		try {
			notEmpty.await();
			if (isRunning) {
				log(2, "---->writeLog:isRunning");
				// return;
			}
			isRunning = true;
		} finally {
			lock.unlock();
		}

		// SQLLogger
		while (SQLLogger.dllHasMore()) {
			SQLLogList ll;

			ll = SQLLogger.popDll();

			if (ll == null) {
				return;
			}
			executeR(bb, fc, ll);

			SQLLogger.addBll(ll);

		}

		isRunning = false;

	}

	public static void notifyWrite() {
		lock.lock();
		try {
			notEmpty.signal();
		} finally {
			lock.unlock();
		}
	}

	static boolean running = false;

	public static void start() {
		if (!running) {
			LogWriter lw = new LogWriter();
			Thread t = new Thread(lw);
			t.start();
			running = true;
		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

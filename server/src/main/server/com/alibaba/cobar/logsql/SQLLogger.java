package com.alibaba.cobar.logsql;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class SQLLogger extends Common {

	public static final int bllMax = 60;
	
	//开关
	public static boolean enableLog = true;

	public static Set<SQLLogList> cll = new HashSet<SQLLogList>();
	public static Set<SQLLogList> dll = new HashSet<SQLLogList>();
	public static Set<SQLLogList> bll = new HashSet<SQLLogList>();

	private static ThreadLocal<SQLLogList> tlr = new ThreadLocal<SQLLogList>();

	static final ReentrantLock lock = new ReentrantLock();

	static {
		LogWriter.start();
		LogHelperTimer.start();
	}

	/**
	 * 接收SQLLog进行处理
	 * 
	 * @param r
	 */
	public static void addR(SQLLog r) {

		if (!enableLog) {
			return;
		}

		SQLLogList ll = tlr.get();

		if (ll == null) {
			ll = getLogList();
			tlr.set(ll);
		}

		ll.add(r);

		if (ll.full()) {
			dealLogList(ll);
			tlr.set(null);
		}

	}

	/**
	 * 给当前在记录的一个记录列表
	 * 
	 * @return
	 */
	private static SQLLogList getLogList() {

		SQLLogList ll;

		lock.lock();
		try {
			if (bll.size() == 0) {
				ll = new SQLLogList();
			} else {
				ll = bll.iterator().next();
				bll.remove(ll);
			}
			cll.add(ll);
		} finally {
			lock.unlock();
		}
		ll.lastActive = System.currentTimeMillis();
		return ll;
	}


	/**
	 * 移动当前在记录的到要处理的
	 * 
	 * @param ll
	 * @return
	 */
	private static SQLLogList dealLogList(SQLLogList ll) {
		lock.lock();
		try {
			cll.remove(ll);
			dll.add(ll);
		} finally {
			lock.unlock();
		}

		ll.ready = false;

		return ll;
	}

	/**
	 * 是否有更多要处理的
	 * 
	 * @return
	 */
	public static boolean dllHasMore() {
		return !dll.isEmpty();
	}

	/**
	 * 获取一个要处理的列表
	 * 
	 * @return
	 */
	public static SQLLogList popDll() {
		SQLLogList ll;
		lock.lock();
		try {
			if (dll.isEmpty()) {
				return null;
			}
			ll = dll.iterator().next();
			dll.remove(ll);
		} finally {
			lock.unlock();
		}
		return ll;
	}

	/**
	 * 添加一个到回收的
	 * 
	 * @param ll
	 */
	public static void addBll(SQLLogList ll) {
		lock.lock();
		try {
			if (bll.size() < bllMax) {
				bll.add(ll);
			}
		} finally {
			lock.unlock();
		}

	}

	
	/**
	 * 使用对象缓存池的接口
	 * 
	 * @return
	 */
	public static SQLLogList getSQLLogger() {
		// long tid = Thread.currentThread().getId();

		SQLLogList ll = tlr.get();

		if (ll == null) {
			ll = getLogList();
			tlr.set(ll);
		}
		return ll;
	}

	/**
	 * 使用对象缓存池的接口
	 * 
	 * @param ll
	 */
	public static void dealSQLLogger(SQLLogList ll) {
		if (ll.full() || ll.ready) {
			dealLogList(ll);
			tlr.set(null);
			LogWriter.notifyWrite();
		}
	}

	public static void main(String[] args) {

	}

}

package com.alibaba.cobar.logsql;

import java.util.Iterator;
import java.util.Set;

public class LogHelperTimer extends Common implements Runnable {

	public static int checkInterval = 20 * 1000;
	public static int dealInterval = 30 * 1000;

	public static void main(String[] args) {

	}

	static Thread lht = null;

	public static void start() {
		if (lht == null) {
			lht = new Thread(new LogHelperTimer());
			lht.start();
		}
	}

	@Override
	public void run() {
		for (;;) {
			try {
				Thread.sleep(checkInterval);
				Set<SQLLogList> cll = SQLLogger.cll;
				log(2, "cll.size():" + cll.size());
				Iterator<SQLLogList> ill = cll.iterator();
				while (ill.hasNext()) {
					SQLLogList ll = ill.next();
					if ((System.currentTimeMillis() - ll.lastActive) > dealInterval && !ll.ready) {
						ll.ready = true;
						log(2, "ll:" + ll.id + " be set as todeal!!!");
					}
				}
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}

	}

}

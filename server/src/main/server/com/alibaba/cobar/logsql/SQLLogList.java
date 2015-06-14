package com.alibaba.cobar.logsql;

import java.util.concurrent.atomic.AtomicInteger;

public class SQLLogList extends Common {

	public static int listSize = 200;
	public static int maxPosition = listSize - 1;
	public static AtomicInteger ai = new AtomicInteger();

	long id;
	int ii = -1;
	SQLLog[] ls;
	boolean ready = false;
	long lastActive;

	public SQLLogList() {

		id = ai.incrementAndGet();
		ii = -1;
		ls = new SQLLog[listSize];

		for (int i = 0; i < listSize; i++) {
			ls[i] = new SQLLog();
		}
	}

	public SQLLogList(long id) {
		this.id = id;
		ii = -1;
		ls = new SQLLog[listSize];

		for (int i = 0; i < listSize; i++) {
			ls[i] = new SQLLog();
		}
	}

	public int size() {
		return listSize;
	}

	public boolean full() {
		return (ii >= maxPosition);
	}

	public void add(SQLLog l) {
		if (ii == maxPosition) {
			throw new IndexOutOfBoundsException("" + ii);
		}
		ls[++ii] = l;

	}

	public SQLLog top() {
		return ls[ii];
	}

	/**
	 * get next
	 * 
	 * @return
	 */
	public SQLLog next() {
		if (ii == maxPosition) {
			throw new IndexOutOfBoundsException("" + ii);
		}
		return ls[++ii];
	}

	public boolean empty() {
		return (ii < 0);
	}

	public SQLLog pop() {
		return ls[ii--];
	}

	@Override
	public boolean equals(Object obj) {
		return this.id == ((SQLLogList) obj).id;
	}



	public static void main(String[] args) {
		SQLLogList ll = new SQLLogList();
		while (!ll.empty()) {
			SQLLog l = ll.pop();
			log(2, "XXXX");
		}
		int j = 0;
		while (!ll.full()) {
			SQLLog l = ll.next();
			log(2, j);
			l.startTime = j++;
		}
		while (!ll.empty()) {
			SQLLog l = ll.pop();
			log(2, l.startTime);
		}

		int k = 0;
		while (k < 20) {
			SQLLog l = ll.next();
			log(2, k);
			l.startTime = k++;
		}
		while (!ll.empty()) {
			SQLLog l = ll.pop();
			log(2, l.startTime);
		}
	}

}

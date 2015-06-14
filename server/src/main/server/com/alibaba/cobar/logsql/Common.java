package com.alibaba.cobar.logsql;

public class Common {
	
	public static void log(int level, Object o) {
		if (level > 0) {
			System.out.println(o.toString());
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}

package com.alibaba.cobar.logsql;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public final class SQLLog {

	public String host;
	public String schema;
	public String statement;
	public long startTime;
	public long executeTime;
	public String dataNode;
	public int dataNodeIndex;

	public String dataSource;
	public String dataSourceSchema;
	public String info;

	static final String st1 = "-";
	static final String st2 = "->";
	static final String en = ";<--";
	static final String st0 = "\r\n-->";
	static byte[] bs0 = null;
	static byte[] bs1 = null;
	static byte[] bs2 = null;
	static byte[] be = null;

	static {
		try {
			bs0 = st0.getBytes("utf-8");
			bs1 = st1.getBytes("utf-8");
			bs2 = st2.getBytes("utf-8");
			be = en.getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public void write(ByteBuffer bb) {

		bb.put(bs0);
		write0(bb, startTime + "");
		bb.put(bs1);
		write0(bb, executeTime + "");
		bb.put(bs1);
		write0(bb, host);
		bb.put(bs1);
		write0(bb, schema);
		// bb.put(bs1);
		// write0(bb, dataNode);
		bb.put(bs1);
		write0(bb, dataSource);
		bb.put(bs1);
		write0(bb, dataSourceSchema);
		bb.put(bs1);
		write0(bb, info);
		bb.put(bs2);
		write0(bb, statement);
		bb.put(be);

	}

	private void write0(ByteBuffer bb, String s) {
		try {
			bb.put(s.getBytes("utf-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		// for (int i = 0; i < s.length(); i++) {
		// bb.putChar(s.charAt(i));
		// }
	}

	public static void main(String[] args) {
		SQLLog sr = new SQLLog();
		sr.host = "host";
		sr.schema = "schema";
		sr.dataNode = "dataNode";
		sr.statement = "statement";
		sr.dataNodeIndex = 1;
		sr.startTime = System.currentTimeMillis();
		sr.executeTime = 100;

		// sr.setInfo();

		// System.out.println(sr.length());

		ByteBuffer bb;
		bb = ByteBuffer.allocate(1024 * 4);
		System.out.println(bb.remaining());
		System.out.println(bb.position());
		sr.write(bb);
		System.out.println(bb.remaining());
		System.out.println(bb.position());

	}

	@Override
	public String toString() {
		return startTime + ":" + dataSource + ":" + dataSourceSchema + ":" + statement;
	}

}

package com.alibaba.cobar.parser.util;

import java.sql.SQLSyntaxErrorException;
import java.util.List;

import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableRefFactor;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReference;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableReferences;
import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertReplaceStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLUpdateStatement;
import com.alibaba.cobar.parser.recognizer.SQLParserDelegate;
import com.alibaba.cobar.parser.recognizer.mysql.syntax.MySQLParser;
import com.alibaba.cobar.server.parser.ServerParse;
/**
 * 
 * @author vv
 *
 */
public class TableUtil {

	public static void log(int l, Object o) {
		System.out.println(o.toString());
	}
	
	/**
	 * 根据SQL获取表名
	 * @param instr
	 * @param charset
	 * @param type //查询语句的类型
	 * @return
	 */
	public static String QuickParseSingleTable(String instr, String charset, int type) {
		//System.out.println("type:" + type);
		if (type != ServerParse.SELECT) {
			if (type == ServerParse.SHOW) {
				return null;
			}
			if (type == ServerParse.UPDATE || type == ServerParse.REPLACE || type == ServerParse.INSERT || type == ServerParse.DELETE) {
				return parseSqlTable(instr, charset, type);
			}
			return null;

		}
		p: for (int i = 0; i < instr.length(); ++i) {
			if ((instr.charAt(i) == ' ' || instr.charAt(i) == '`') && (instr.charAt(i + 1) == 'f' || instr.charAt(i + 1) == 'F')
					&& (instr.charAt(i + 2) == 'r' || instr.charAt(i + 2) == 'R') && (instr.charAt(i + 3) == 'o' || instr.charAt(i + 3) == 'O')
					&& (instr.charAt(i + 4) == 'm' || instr.charAt(i + 4) == 'M') && (instr.charAt(i + 5) == ' ')) {

//				log(1, i + ":" + instr.charAt(i));

				int j = i + 6;
				sub: for (;;) {
					// sub: while (k < 0) {
					if (CharTypes.isWhitespace(instr.charAt(j)) || instr.charAt(j) == '`') {
						++j;
					} else if (instr.charAt(j) == '(') {
						i = j;
						continue p;
					} else {
						break sub;
					}

				}

				int k = j;
				sub: for (;;) {
//					 log(1, "instr.charAt(j)" + instr.charAt(k));
					//if (k < instr.length() && instr.charAt(k) != ' ' && instr.charAt(k) != '\t' && instr.charAt(k) != '\r' && instr.charAt(k) != '\n' && instr.charAt(k) != ','
					if (k < instr.length() && !CharTypes.isWhitespace(instr.charAt(k)) && instr.charAt(k) != ','
							&& instr.charAt(k) != '`') {
						++k;
					} else {
						break sub;
					}
				}

				log(1, j + ":" + j + ",k:" + k);

				// 多个表的话加到数组
				return instr.substring(j, k);
			}
		}

		return null;
	}

	
	/**
	 * 通过语法树进行解析获取所使用的表名
	 * @param stmt
	 * @param charset
	 * @param type
	 * @return
	 */
	public static String parseSqlTable(String stmt, String charset, int type) {
		try {
			SQLStatement ast = SQLParserDelegate.parse(stmt, charset == null ? MySQLParser.DEFAULT_CHARSET : charset);

			if (ast instanceof DMLInsertReplaceStatement) {
				DMLInsertReplaceStatement ir = (DMLInsertReplaceStatement) ast;
				Identifier table = ir.getTable();
				return table.getIdText();
			}

			if (ast instanceof DMLUpdateStatement) {
				DMLUpdateStatement u = (DMLUpdateStatement) ast;
				TableReferences tables = u.getTableRefs();
				List<TableReference> ts = tables.getTableReferenceList();
				TableReference table = ts.get(0);
				if (table instanceof TableRefFactor) {
					TableRefFactor trf = (TableRefFactor) table;
					Identifier t = trf.getTable();
					return t.getIdText();
				}
			}

			if (ast instanceof DMLDeleteStatement) {
				DMLUpdateStatement u = (DMLUpdateStatement) ast;
				TableReferences tables = u.getTableRefs();
				List<TableReference> ts = tables.getTableReferenceList();
				TableReference table = ts.get(0);
				if (table instanceof TableRefFactor) {
					TableRefFactor trf = (TableRefFactor) table;
					Identifier t = trf.getTable();
					return t.getIdText();
				}
			}

		} catch (SQLSyntaxErrorException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static void main(String[] args) {
		String table = null;
		// table =
		// QuickParseSingleTable("select a 123 from abc123 , abc456 , ( select 123 from 789) where 1=1 and 2=2");
		// table =
		// QuickParseSingleTable("select * from dal_t_r12\r\n LIMIT 0, 100");
		// table = QuickParseSingleTable("select * from dal_t_r12");

		String sql1 = "select * from dal_t_r12\nLIMIT 0, 100";
		table = QuickParseSingleTable(sql1, null, ServerParse.SELECT);
		System.out.println("table:" + table);
		System.out.println("-----");
		
		
//		String t1 = "\n";
//		System.out.println((int) t1.charAt(0));
//
//		System.out.println((int) '\n');
//
//		System.out.println(t1.charAt(0) != '\n');
//		System.out.println(sql1.charAt(23) != '\n');
//
//		System.out.println(table);

		String sql2 = "  INSERT INTO dal_t_r19 (`id`, `user_id`, `info`) VALUES ('19001', '19001', '19001');";

		// sql1 = sql1.toLowerCase();

		String table1 = parseSqlTable(sql2, null, -1);
		System.out.println(table1);
		
		System.out.println("-----");

		String sql3 = "  update dal_t_r19 set info = '19001' where id ='19001';";
		table1 = parseSqlTable(sql3, null, -1);
		System.out.println(table1);

		// long cc =System.currentTimeMillis();
		// for (int i =0;i<100*100*10;++i) {
		// QuickParseSingleTable(sql1,null,ServerParse.SELECT);
		// }
		// System.out.println((System.currentTimeMillis()-cc)+"ms"+","+((System.currentTimeMillis()-cc)*1000/(100*100*10)+"ns"));
		//
		// for (int i =0;i<100*100*2;++i) {
		// parseSqlTable(sql2,null,-1);
		// }
		// cc =System.currentTimeMillis();
		// for (int i =0;i<100*100*10;++i) {
		// parseSqlTable(sql2,null,-1);
		// }
		// System.out.println((System.currentTimeMillis()-cc)+"ms"+","+((System.currentTimeMillis()-cc)*1000/(100*100*10)+"ns"));
		//

	}

}

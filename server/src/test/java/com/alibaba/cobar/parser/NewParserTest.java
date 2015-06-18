package com.alibaba.cobar.parser;

import java.sql.SQLSyntaxErrorException;

import com.alibaba.cobar.parser.ast.stmt.SQLStatement;
import com.alibaba.cobar.parser.recognizer.SQLParserDelegate;
import com.alibaba.cobar.parser.recognizer.mysql.syntax.MySQLParser;
import com.alibaba.cobar.parser.visitor.MySQLOutputASTVisitor;

public class NewParserTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//String sql2 = "select count(*) from em_r_train_course tc where tc.train_phase_id = '596d69e5-c988-4bb6-abcde-c775b3f7c3ec' and not exists (select uc.course_id from lm_wr_user_course uc where uc.course_id = tc.course_id and uc.user_project_id = 'b44a73a1-0aeb-4e32-a4b4-b5816f9348d0' and uc.audit_state=2 and uc.delete_flag = 1 limit 0,10;";
		String sql2 = "select count(*) from em_r_train_course tc where tc.train_phase_id = '596d69e5-c988-4bb6-abcde-c775b3f7c3ec' and not exists (select uc.course_id from lm_wr_user_course uc where uc.course_id = tc.course_id and uc.user_project_id = 'b44a73a1-0aeb-4e32-a4b4-b5816f9348d0' and uc.audit_state=2 and uc.delete_flag = 1 )  limit 0,10;";
		String charset = null;
		
		try {
			System.out.println(testParser( sql2,null));
			
			
			int cc = 50000;
			long start0 = System.currentTimeMillis();
			for (int i = 0; i < cc; ++i) {
				testParser( sql2,null);
			}
			long l0 = System.currentTimeMillis() - start0;
			System.out.println(l0 * 1000 * 1000 / cc + "ns");
			
			long start1 = System.currentTimeMillis();
			for (int i = 0; i < cc; ++i) {
				testParser( sql2,null);
			}
			long l1= System.currentTimeMillis() - start1;
			System.out.println(l1 * 1000 * 1000 / cc + "ns");
			
		} catch (SQLSyntaxErrorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static String genSQL(SQLStatement ast, String orginalSql) {
        StringBuilder s = new StringBuilder();
        ast.accept(new MySQLOutputASTVisitor(s));
        return s.toString();
    }

	private static String testParser(String sql2, String charset) throws SQLSyntaxErrorException {
		SQLStatement ast = SQLParserDelegate.parse(sql2, charset == null ? MySQLParser.DEFAULT_CHARSET : charset);
		
		return genSQL(ast,sql2);
	}

}

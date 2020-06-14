package main.java.streaming;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

class JDBCWrapper implements Serializable { 
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
		Object[] obj = new Object[]{"1","1","1","1"};
		List<Object[]> list = new ArrayList<Object[]>();
		list.add(obj);
		jdbcWrapper.doBatch("INSERT INTO result VALUES(?,?,?,?)", list);
	}

	private static JDBCWrapper jdbcInstance = null;
	private static LinkedBlockingQueue<Connection> dbConnectionPool = new LinkedBlockingQueue<Connection>();

	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static JDBCWrapper getJDBCInstance() {
		if (jdbcInstance == null) {

			synchronized (JDBCWrapper.class) {
				if (jdbcInstance == null) {
					jdbcInstance = new JDBCWrapper();
				}
			}
		}

		return jdbcInstance;
	}
 
	private JDBCWrapper() {

		for (int i = 0; i < 10; i++) {

			try {
				Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop1:3306/sparkstreaming", "spark",
						"spark2016");
				dbConnectionPool.put(conn);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}
	
	public synchronized Connection getConnection() {
		while (0 == dbConnectionPool.size()) {
			try {
				Thread.sleep(20);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
 
		return dbConnectionPool.poll();
	}

	public int[] doBatch(String sqlText, List<Object[]> paramsList) {
		Connection conn = getConnection();
		PreparedStatement preparedStatement = null;
		int[] result = null;
		try {
			conn.setAutoCommit(false);
			preparedStatement = conn.prepareStatement(sqlText);

			for (Object[] parameters : paramsList) {
				for (int i = 0; i < parameters.length; i++) {
					preparedStatement.setObject(i + 1, parameters[i]);
				}

				preparedStatement.addBatch();
			}

			result = preparedStatement.executeBatch();

			conn.commit();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (conn != null) {
				try {
					dbConnectionPool.put(conn);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		return result;
	}

	public void doQuery(String sqlText, Object[] paramsList, ExecuteCallBack callBack) {

		Connection conn = getConnection();
		PreparedStatement preparedStatement = null;
		ResultSet result = null;
		try {

			preparedStatement = conn.prepareStatement(sqlText);

			if(paramsList!=null){
				for (int i = 0; i < paramsList.length; i++) {
					preparedStatement.setObject(i + 1, paramsList[i]);
				}
			}

			result = preparedStatement.executeQuery();

			callBack.resultCallBack(result);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (conn != null) {
				try {
					dbConnectionPool.put(conn);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}
}

interface ExecuteCallBack {
	void resultCallBack(ResultSet result) throws Exception;
}
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.ArrayList;
import datastructure.aggregates;
public class output{
	public static void main(String[] args) {
		try {
			Class.forName("org.postgresql.Driver");
			Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres",
					"postgres");
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("select * from sales");
			ResultSetMetaData rsmd = resultSet.getMetaData();
			int columnsNumber = rsmd.getColumnCount();
			for (int i = 1; i <= columnsNumber; i++) {
				System.out.printf("%-30.30s", rsmd.getColumnName(i));
			}
			System.out.println();
			aggregates x = new aggregates();
			aggregates y = new aggregates();
			aggregates z = new aggregates();
			int ny = 0;
			int nj = 0;
			int ct = 0;
			while (resultSet.next()) {
				String[] keys = { resultSet.getString(1) };
				if (resultSet.getString(6).equals("NY")) {
					x.update(resultSet.getInt(7), keys);
					ny++;
				}
				if (resultSet.getString(6).equals("NJ")) {
					y.update(resultSet.getInt(7), keys);
					nj++;
				}
				if (resultSet.getString(6).equals("CT")) {
					z.update(resultSet.getInt(7), keys);
					ct++;
				}

			}
			System.out.println(ny);
			System.out.println(nj);
			System.out.println(ct);
			x.printresult();
			y.printresult();
			z.printresult();
		} catch (SQLException | ClassNotFoundException e) {
			System.out.println("Connection failure.");
			e.printStackTrace();
		}
	}
}

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
public class output{
public static void main(String[] args){
try{
Class.forName("org.postgresql.Driver");
Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
Statement statement = connection.createStatement();
System.out.println("Hello World!");
ResultSet resultSet = statement.executeQuery("select * from sales");
ResultSetMetaData rsmd = resultSet.getMetaData();
int columnsNumber = rsmd.getColumnCount();
}
catch (SQLException | ClassNotFoundException e) {
System.out.println("Connection failure.");
e.printStackTrace();
}
}
}

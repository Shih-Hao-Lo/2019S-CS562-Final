import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.ArrayList;
public class output{
public static void main(String[] args){
try{
Class.forName("org.postgresql.Driver");
Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
Statement statement = connection.createStatement();
ResultSet resultSet = statement.executeQuery("select * from sales");
ResultSetMetaData rsmd = resultSet.getMetaData();
int columnsNumber = rsmd.getColumnCount();
for(int i = 1; i <= columnsNumber; i++){
System.out.printf("%-30.30s" , rsmd.getColumnName(i));
}
System.out.println();
int cnt = 0;
while(resultSet.next() && cnt++<=30){
for(int i = 1; i <= columnsNumber; i++){
System.out.printf("%-30.30s" , resultSet.getString(i));
}
System.out.println();

}
HashMap<ArrayList<String> , Integer> minmap = new HashMap<>();
ArrayList<String> a = new ArrayList<>();
a.add("Hello");
a.add("World");
minmap.put(a,1);
for(ArrayList<String> name: minmap.keySet()) {
String key = name.toString();
int value = minmap.get(name);
System.out.println(key + " " + value);
}
}
catch (SQLException | ClassNotFoundException e) {
System.out.println("Connection failure.");
e.printStackTrace();
}
}
}

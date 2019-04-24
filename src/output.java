import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Arrays;
import datastructure.aggregates;
public class output{
public static HashSet<HashSet<String>> globalkey;
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
globalkey = new HashSet<>();
aggregates x = new aggregates();
aggregates y = new aggregates();
aggregates z = new aggregates();
while(resultSet.next()){
String[] keys = {resultSet.getString(1)};
HashSet<String> tmpset = new HashSet<>();
tmpset.addAll(Arrays.asList(keys));
globalkey.add(tmpset);
if(resultSet.getString("state").equals("NY")){
x.update(resultSet.getInt("quant") , keys);
}
if(resultSet.getString("state").equals("NJ")){
y.update(resultSet.getInt("quant") , keys);
}
if(resultSet.getString("state").equals("CT")){
z.update(resultSet.getInt("quant") , keys);
}

}
x.printresult();
System.out.println("------------------------------------------------");
y.printresult();
System.out.println("------------------------------------------------");
z.printresult();
System.out.println("------------------------------------------------");
for(HashSet<String> k: globalkey) {
if(2+x.sum.get(k)>y.sum.get(k)&&x.avg.get(k)>z.avg.get(k)) {
System.out.println(k.toString() + " should output!");
}
}
}
catch (SQLException | ClassNotFoundException e) {
System.out.println("Connection failure.");
e.printStackTrace();
}
}
}

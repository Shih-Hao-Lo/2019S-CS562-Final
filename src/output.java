import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;
import java.util.Arrays;
import datastructure.*;
public class output{
public static Set<ArrayList<String>> globalkey;
public static void main(String[] args){
try{
Class.forName("org.postgresql.Driver");
Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
ResultSet resultSet = statement.executeQuery("select * from sales");
ResultSetMetaData rsmd = resultSet.getMetaData();
int columnsNumber = rsmd.getColumnCount();
System.out.printf("%-20.20s","[cust, month]");
System.out.printf("%-20.20s","sum(x.quant)");
System.out.printf("%-20.20s","sum(y.quant)");
System.out.printf("%-20.20s","sum(z.quant)");
System.out.println();
ArrayList<String> gb = new ArrayList<>();
gb.add("cust");
gb.add("month");
aggregates2 x = new aggregates2(resultSet, gb);
process2.process(x, resultSet, gb,"x.state=\"NY\"andx.year=1995");
aggregates2 y = new aggregates2(resultSet, gb);
process2.process(y, resultSet, gb,"y.state=\"NJ\"andy.year=1995");
aggregates2 z = new aggregates2(resultSet, gb);
process2.process(z, resultSet, gb,"z.state=\"CT\"andz.year=1995");
globalkey = x.max.keySet();
for(ArrayList<String> k: globalkey) {
if(x.sum.get(k)>y.sum.get(k)&&x.avg.get(k)>z.avg.get(k)) {
System.out.printf("%-20.20s", k.toString());
System.out.printf("%-20.20s",x.sum.get(k));
System.out.printf("%-20.20s",y.sum.get(k));
System.out.printf("%-20.20s",z.sum.get(k));
System.out.println();
}
}
}
catch (SQLException | ClassNotFoundException e) {
System.out.println("Connection failure.");
e.printStackTrace();
}
}
}

package datastructure;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

public class process2 {
	public static ArrayList<String> dic;
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Class.forName("org.postgresql.Driver");
		Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
		Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
		ResultSet resultSet = statement.executeQuery("select * from sales");
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
		ArrayList<String> gb = new ArrayList<>();
		dic = new ArrayList<>();
		gb.add("cust");
		gb.add("month");
		dic.add("cust");
		dic.add("month");
		aggregates2 a = new aggregates2(resultSet , gb);
		String s1 = "x.cust=\"Sam\"andx.year=\"1995\"";
		String s2 = "x.month<=month";//andx.prod=prod";
		process(a , resultSet , gb , s2);
		a.printresult();
	}
	
	public static void process(aggregates2 in , ResultSet result ,  ArrayList<String> group , String st) throws SQLException {
		String[] expression = st.split("and");
		ArrayList<String> constexp = new ArrayList<>();
		ArrayList<String> othexp = new ArrayList<>();
		int len = 0;
		for(int x = 0 ; x < expression.length ; x++) {
			len = 0;
			while(expression[x].charAt(len) != '>' && expression[x].charAt(len) != '<' && expression[x].charAt(len) != '!' && expression[x].charAt(len) != '=') {
				len++;
			}
			String p1,p2;
			String op;
			if(expression[x].charAt(len) == '!' && expression[x].charAt(len+1) == '=') {
				p1 = expression[x].substring(0, len);
				p2 = expression[x].substring(len+2, expression[x].length());
				op = "!=";
			}
			else if(expression[x].charAt(len) == '>' && expression[x].charAt(len+1) == '=') {
				p1 = expression[x].substring(0, len);
				p2 = expression[x].substring(len+2, expression[x].length());
				op = ">=";
			}
			else if(expression[x].charAt(len) == '<' && expression[x].charAt(len+1) == '=') {
				p1 = expression[x].substring(0, len);
				p2 = expression[x].substring(len+2, expression[x].length());
				op = "<=";
			}
			else {
				p1 = expression[x].substring(0, len);
				p2 = expression[x].substring(len+1, expression[x].length()); 
				op = Character.toString(expression[x].charAt(len));
			}
			len = 0;
			while(len < p1.length() && p1.charAt(len) != '.') {
				len++;
			}
			if(len != p1.length()) p1 = p1.substring(len+1 , p1.length());
			System.out.print(p1+"|");
			System.out.print(op+"|");
			System.out.println(p2);
			if(p2.charAt(0) == '\"') {
				p2 = p2.substring(1, p2.length()-1);
				constexp.add(p1+"/"+op+"/"+p2);
			}
			else {
				othexp.add(p1+"/"+op+"/"+p2);
			}
		}

		//printatt(result);
		
		while(result.next()) {
			if(checkconst(constexp , result)) {
				//printrow(result);
				updateto(in , result , group , othexp);
			}
		}
	}
	
	public static boolean checkconst(ArrayList<String> c , ResultSet result) throws SQLException {
		boolean out = true;
		for(int x = 0 ; x < c.size() ; x++) {
			String[] cur = c.get(x).split("/");
			String p1 = cur[0];
			String op = cur[1];
			String p2 = cur[2];
			String data = result.getString(p1);
			if(isnumber(data)) {
				//System.out.println(Integer.valueOf(data));
				//System.out.println(op);
				//System.out.println(Integer.valueOf(p2));
				switch(op) {
					case ">":
						out = out && (Integer.valueOf(data) > Integer.valueOf(p2));
						break;
					case ">=":
						out = out && (Integer.valueOf(data) >= Integer.valueOf(p2));
						break;
					case "<":
						out = out && (Integer.valueOf(data) < Integer.valueOf(p2));
						break;
					case "<=":
						out = out && (Integer.valueOf(data) <= Integer.valueOf(p2));
						break;
					case "=":
						out = out && (Integer.valueOf(data) == Integer.valueOf(p2));
						break;
					case "!=":
						out = out && (Integer.valueOf(data) != Integer.valueOf(p2));
						break;
					default:
				}
			}
			else {
				switch(op) {
					case "=":
						out = out && (data.equals(p2));
						break;
					case "!=":
						out = out && (!data.equals(p2));
						break;
					default:
				}
			
			}
		}
		return out;
	}
	
	public static void updateto(aggregates2 in , ResultSet result , ArrayList<String> gb , 
			ArrayList<String> exp) throws SQLException {
		HashMap<String , ArrayList<String>> attmap = new HashMap<>();
		for(int x = 0 ; x < exp.size() ; x++) {
			String[] cur = exp.get(x).split("/");
			String p1 = cur[0];
			String op = cur[1];
			String p2 = cur[2];
			if(!attmap.containsKey(p1)) {
				ArrayList<String> tmplist = new ArrayList<>();
				tmplist.add(exp.get(x));
				attmap.put(p1 , tmplist);
			}
			else{
				ArrayList<String> tmplist = attmap.get(p1);
				tmplist.add(exp.get(x));
				attmap.replace(p1, tmplist);
			}
		}
		for(int x = 0 ; x < gb.size() ; x++) {
			if(!attmap.containsKey(gb.get(x))) {
				ArrayList<String> tmplist = new ArrayList<>();
				tmplist.add("x."+gb.get(x)+"/=/"+gb.get(x));
				attmap.put(gb.get(x), tmplist);
			}
		}
		/*for(String s: attmap.keySet()) {
			System.out.println("[ "+s+" : "+attmap.get(s).toString()+" ]");
		}*/
		ArrayList<String> curkeys = new ArrayList<>();
		for(int x = 0 ; x < gb.size() ; x++) {
			curkeys.add(result.getString(gb.get(x)));
		}
		//System.out.println(keys.toString());
		for(ArrayList<String> k: in.max.keySet()) {
			if(checkgb(k , curkeys , attmap , gb)) {
				in.update(result.getInt("quant"), k);
			}
		}
	}
	
	public static boolean checkgb(ArrayList<String> agkey , ArrayList<String> curkey , 
			HashMap<String , ArrayList<String>> cond , ArrayList<String> gb){
		boolean out = true;
		for(String s: cond.keySet()) {
			int idx = inlistidx(gb , s);
			String[] cur = cond.get(s).get(0).split("/");
			String op = cur[1];
			if(isnumber(curkey.get(idx))) {
				switch(op) {
					case ">":
						out = out && (Integer.valueOf(curkey.get(idx)) > Integer.valueOf(agkey.get(idx)));
						break;
					case ">=":
						out = out && (Integer.valueOf(curkey.get(idx)) >= Integer.valueOf(agkey.get(idx)));
						break;
					case "<":
						out = out && (Integer.valueOf(curkey.get(idx)) < Integer.valueOf(agkey.get(idx)));
						break;
					case "<=":
						out = out && (Integer.valueOf(curkey.get(idx)) <= Integer.valueOf(agkey.get(idx)));
						break;
					case "!=":
						out = out && (Integer.valueOf(curkey.get(idx)) != Integer.valueOf(agkey.get(idx)));
						break;
					case "=":
						out = out && (Integer.valueOf(curkey.get(idx)) == Integer.valueOf(agkey.get(idx)));
						break;
					default:
				}
			}
			else {
				switch(op) {
				case "!=":
					out = out && (!curkey.get(idx).equals(agkey.get(idx)));
					break;
				case "=":
					out = out && (curkey.get(idx).equals(agkey.get(idx)));
					break;
				default:
				}
			}
		}
		return out;
	}
	
	public static int inlistidx(ArrayList<String> list , String target) {
		for(int x = 0 ; x < list.size() ; x++) {
			if(list.get(x).equals(target)) return x;
		}
		return -1;
	}
	
	public static boolean isnumber(String in) {
		try {
			Integer result = Integer.valueOf(in);	
			return true;
		}
		catch(NumberFormatException e){
			return false;
		}
	}
	
	public static void printatt(ResultSet result) throws SQLException {
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            System.out.printf("%-20.20s" , result.getMetaData().getColumnName(i));
        }
        System.out.println();
	}
	public static void printrow(ResultSet result) throws SQLException {
        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            System.out.printf("%-20.20s" , result.getString(i));
        }
        System.out.println();
	}
}

package datastructure;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;

public class process {
	public static ArrayList<String> dic;
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		// TODO Auto-generated method stub
		dic = new ArrayList<>();
		Class.forName("org.postgresql.Driver");
		Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
		Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
		ResultSet resultSet = statement.executeQuery("select * from sales");
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
		ArrayList<String> gb = new ArrayList<>();
		gb.add("cust");
		//gb.add("month");
		dic.add("cust");
		//dic.add("month");
		aggregates2 a = new aggregates2(resultSet , gb);
		int cnt = 0;

		a = processbool(a , resultSet , gb , "x.cust!=cust");
		a.printresult();
	}
	
	public static aggregates2 processbool(aggregates2 in , ResultSet result , ArrayList<String> group , String suchthat) throws SQLException {
		while(result.next()){
			ArrayList<String> k = new ArrayList<>();
			for(int x = 0 ; x < group.size() ; x++) {
				k.add(result.getString(group.get(x)));
			}
			if(evaluate(result , suchthat , k , in)) {
				in.update(result.getInt("quant"), toStrarr(k));
			}
		}
		result.beforeFirst();
		return in;
	}
	
	public static boolean evaluate(ResultSet result , String st , ArrayList<String> key , aggregates2 in) throws SQLException {
		boolean out = true;
		int len = 0;
		ArrayList<String> expression = new ArrayList<>();
		ArrayList<String> operator = new ArrayList<>();
		for(int x = 0 ; x < st.length() ; x++) {
			if(st.charAt(x) == 'o' && st.charAt(x+1) == 'r') {
				expression.add(st.substring(len, x));
				operator.add("or");
				len = x+2;
				x +=2;
			}
			else if(st.charAt(x) == 'a' && st.charAt(x+1) == 'n' && st.charAt(x+2) == 'd') {
				expression.add(st.substring(len, x));
				operator.add("and");
				len = x+3;
				x+=3;
			}
		}
		expression.add(st.substring(len, st.length()));
		//System.out.println(expression.toString());
		//System.out.println(operator.toString());
		for(int x = 0 ; x < expression.size() ; x++) {
			len = 0;
			while(expression.get(x).charAt(len) != '>' && expression.get(x).charAt(len) != '<' && expression.get(x).charAt(len) != '!' && expression.get(x).charAt(len) != '=') {
				len++;
			}
			String p1,p2;
			String op;
			if(expression.get(x).charAt(len) == '!' && expression.get(x).charAt(len+1) == '=') {
				p1 = expression.get(x).substring(0, len);
				p2 = expression.get(x).substring(len+2, expression.get(x).length());
				op = "!=";
			}
			else if(expression.get(x).charAt(len) == '>' && expression.get(x).charAt(len+1) == '=') {
				p1 = expression.get(x).substring(0, len);
				p2 = expression.get(x).substring(len+2, expression.get(x).length());
				op = ">=";
			}
			else if(expression.get(x).charAt(len) == '<' && expression.get(x).charAt(len+1) == '=') {
				p1 = expression.get(x).substring(0, len);
				p2 = expression.get(x).substring(len+2, expression.get(x).length());
				op = "<=";
			}
			else {
				p1 = expression.get(x).substring(0, len);
				p2 = expression.get(x).substring(len+1, expression.get(x).length()); 
				op = Character.toString(expression.get(x).charAt(len));
			}
			//System.out.println(p1);
			//System.out.println(op);
			//System.out.println(p2);
			len = 0;
			while(len < p1.length() && p1.charAt(len) != '.') {
				len++;
			}
			if(len != p1.length()) p1 = p1.substring(len+1 , p1.length());
			//System.out.println(p1);
			boolean curbool = true;
			
			if(p2.charAt(0) != '\"') {
				if(isnumber(p2)) {
					switch(op) {
					case ">":
						curbool = result.getInt(p1) > Integer.parseInt(p2);
						break;
					case ">=":
						curbool = result.getInt(p1) > Integer.parseInt(p2);
						break;
					case "<":
						curbool = result.getInt(p1) < Integer.parseInt(p2);
						break;
					case "<=":
						curbool = result.getInt(p1) < Integer.parseInt(p2);
						break;
					case "=":
						curbool = result.getInt(p1) == Integer.parseInt(p2);
						break;
					case "!=":
						curbool = result.getInt(p1) != Integer.parseInt(p2);
						break;
					}
					//System.out.print(result.getInt(p1)+"|"+Integer.parseInt(p2)+":");
					//System.out.println(curbool);
				}
				else {
					String dat = result.getString(p2);
//					if(isnumber(dat)) {
//						switch(op) {
//						case ">":
//							curbool = result.getInt(p1) > Integer.parseInt(dat);
//							break;
//						case ">=":
//							curbool = result.getInt(p1) >= Integer.parseInt(dat);
//							break;
//						case "<":
//							curbool = result.getInt(p1) < Integer.parseInt(dat);
//							break;
//						case "<=":
//							curbool = result.getInt(p1) < Integer.parseInt(dat);
//							break;
//						case "=":
//							curbool = result.getInt(p1) == Integer.parseInt(dat);
//							break;
//						case "!=":
//							curbool = result.getInt(p1) != Integer.parseInt(dat);
//							break;
//						}
//						//System.out.print(result.getInt(p1)+"|"+Integer.parseInt(dat)+":");
//						//System.out.println(curbool);
//					}
//					else {
						switch(op) {
						case "=":
							curbool = result.getString(p1).equals(key.get(mapdic(p2)));
							//System.out.print(result.getString(p1)+"|"+key.get(mapdic(p2))+":");
							//System.out.println(curbool);
							break;
						case "!=":
							in.addtomap(result, p2, result.getString(p1) , "!=" , st);
							curbool = false;
							//System.out.print(result.getString(p1)+"|"+key.get(mapdic(p2))+":");
							//System.out.println(curbool);
							break;
						case ">":
							in.addtomap(result, p2, result.getString(p1) , ">" , st);
							curbool = false;
							break;
						case "<":
							in.addtomap(result, p2, result.getString(p1) , ">=" , st);
							curbool = false;
							break;
						case ">=":
							in.addtomap(result, p2, result.getString(p1) , "<" , st);
							curbool = false;
							break;
						case "<=":
							in.addtomap(result, p2, result.getString(p1) , "<=" , st);
							curbool = false;
							break;
						}
					}
				}
//			}
			else {
				switch(op) {
				case "=":
					curbool = result.getString(p1).equals(p2.substring(1,p2.length()-1));
					//System.out.print(result.getString(p1)+"|"+p2.substring(1,p2.length()-1)+":");
					//System.out.println(curbool);
					break;
				case "!=":
					curbool = !(result.getString(p1).equals(p2.substring(1,p2.length()-1)));
					//System.out.print(result.getString(p1)+"|"+p2.substring(1,p2.length()-1)+":");
					//System.out.println(curbool);
					break;
				}
			}
			if(x == 0) {
				out = curbool;
			}
			else {
				switch(operator.get(x-1)) {
				case "and":
					out = out && curbool;
					break;
				case "or":
					out = out || curbool;
					break;
				}
			}
		}
		return out;
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
	
	public static String[] toStrarr(ArrayList<String> in) {
		String[] out = new String[in.size()];
		for(int x = 0 ; x < in.size() ; x++) {
			out[x] = in.get(x);
		}
		return out;
	}
	
	public static int mapdic(String s) {
		for(int x = 0 ; x < dic.size() ; x++) {
			if(dic.get(x).equals(s)) return x;
		}
		return 0;
	}
}

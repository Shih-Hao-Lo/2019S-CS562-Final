import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import datastructure.*;
import transfer.esqltophi;

public class input {
    public static PrintWriter writer;

	public static void main(String[] args) throws IOException {
		writer = new PrintWriter("src/output.java", "UTF-8");
		phi phidata = esqltophi.fromfile();

		ArrayList<String> a = new ArrayList<>();//cust
		ArrayList<String> s = new ArrayList<>();//project
		a.add("ArrayList<String> gb = new ArrayList<>();");
		
		//Projection
		for(int x = 0; x < phidata.S.size(); x++) {
			  int len = 0;
			  int len1 = 0;
			  String name = "" ;
			  String name1 = "";
			  while(len < phidata.S.get(x).length() && phidata.S.get(x).charAt(len) != '(') {
				  len++;
				 
			  }

			  if(len != phidata.S.get(x).length()) {
				  name = phidata.S.get(x).substring(0, len);
				  len1 = len;
				
				  while(phidata.S.get(x).charAt(len1) != '.') {
					  len1++;
				  }
				  name1 = phidata.S.get(x).substring(len+1, len1);
				  //System.out.println(name1);//
			  }else{
				  name = phidata.S.get(x);
			  }	  
			  if(len1 != 0) {
				  s.add(name1 +"."+name);
			  }else {
				  s.add(name);
			  }
		}
		
		for(int x = 0; x < phidata.V.size() ; x++) {
			a.add("gb.add(\""+phidata.V.get(x)+"\");");
		}
	
		for(int x = 0; x < phidata.theta.size() ; x++) {
		 	  int len = 0;
			  while(phidata.theta.get(x).charAt(len) != '.') {
				  len++;
			  }
			  String name = phidata.theta.get(x).substring(0, len);
			  a.add("aggregates2 " + name + " = new aggregates2(resultSet, gb);");
			  
			  String cond = "" + phidata.theta.get(x);
			  if(phidata.W.length() != 0) {
				  cond = cond + "and" + name + "." + removestr(phidata.W);
			  }
			  a.add("process2.process("+ name +", resultSet, gb,\"" + cond + "\");");
			  
		}
		before();
		addquery("select * from sales");
		//printattribute();
		for(int x = 0 ; x < phidata.V.size() ; x++) {
			addstatment("System.out.printf(\"%-10s\", \"" + phidata.V.get(x) + "\");");
		}
		for(int x = phidata.V.size(); x < s.size(); x++) {		
				addstatment("System.out.printf(\"%-20.20s\",\"" + phidata.S.get(x) + "\");");
		}
		addstatment("System.out.println();");
		for(int x = 0; x < a.size(); x++) {
			addstatment(a.get(x));
		}
		addstatment("globalkey = "+ phidata.theta.get(0).substring(0, 1) + ".max.keySet();");
		addstatment("for(ArrayList<String> k: globalkey) {");
		addstatment(processhaving(phidata.G));
		//addstatment("System.out.printf(\"%-10.20s\", k.toString());");
		for(int x = 0 ; x < phidata.V.size() ; x++) {
			addstatment("if(isnumber(k.get("+x+"))) {");
			addstatment("System.out.printf(\"%10s\", k.get(" + x + "));");
			addstatment("}");
			addstatment("else {");
			addstatment("System.out.printf(\"%-10s\", k.get(" + x + "));");
			addstatment("}");
		}
		for(int x = 0; x < s.size(); x++) {
			boolean kadded = false;
			if(s.get(x).contains(".")) {
				addstatment("System.out.printf(\"%20.20s\"," + s.get(x) + ".get(k));");
			}
		}
		addstatment("System.out.println();");
		if(phidata.G.length() != 0) addstatment("}");
		addstatment("}");
		after();
		
//        try {
//            //runProcess("javac src/output.java");
//        	runProcess("javac -cp postgresql-42.2.2.jar src/output.java");
//            runProcess("java output");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
		
	}
	public static void before() {
		writer.println("import java.sql.Connection;");
		writer.println("import java.sql.DriverManager;");
		writer.println("import java.sql.ResultSet;");
		writer.println("import java.sql.ResultSetMetaData;");
		writer.println("import java.sql.SQLException;");
		writer.println("import java.sql.Statement;");
		writer.println("import java.util.HashMap;");
		writer.println("import java.util.ArrayList;");
		writer.println("import java.util.Set;");
		writer.println("import java.util.Arrays;");
		writer.println("import datastructure.*;");
        writer.println("public class output{");
        writer.println("public static Set<ArrayList<String>> globalkey;");
        writer.println("public static void main(String[] args){");
        writer.println("try{");
        writer.println("Class.forName(\"org.postgresql.Driver\");");
        writer.println("Connection connection = DriverManager.getConnection(\"jdbc:postgresql://localhost:5432/postgres\", \"postgres\", \"postgres\");");
        writer.println("Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);");
	}
    public static void after(){
    	writer.println("}");
    	writer.println("catch (SQLException | ClassNotFoundException e) {");
    	writer.println("System.out.println(\"Connection failure.\");");
    	writer.println("e.printStackTrace();");
    	writer.println("}");
    	writer.println("}");
    	writer.println("public static boolean isnumber(String in) {");
    	writer.println("try {");
    	writer.println("Integer result = Integer.parseInt(in);");
    	writer.println("return true;");
    	writer.println("}");
    	writer.println("catch(NumberFormatException e){");
    	writer.println("return false;");
    	writer.println("}");
    	writer.println("}");
        writer.println("}");
        writer.close();
    }
    
    public static void addstatment(String statment) {
    	writer.println(statment);
    }
    
    public static void addquery(String query) {
    	writer.println("ResultSet resultSet = statement.executeQuery(\"" + query + "\");");
    	writer.println("ResultSetMetaData rsmd = resultSet.getMetaData();");
    	writer.println("int columnsNumber = rsmd.getColumnCount();");
    }
    
    public static void printattribute() {
    	writer.println("for(int i = 1; i <= columnsNumber; i++){");
        writer.println("System.out.printf(\"%-30.30s\" , rsmd.getColumnName(i));");
        writer.println("}");
        writer.println("System.out.println();");
    }
    
    public static String processhaving(String G) {
    	if(G.length() == 0) return "";
    	String out = "if(";
    	String in = removestr(G);
    	String[] expression = split2(in);
    	String[] comprator = splitcompare(in);
    	for(int x = 0 ; x < expression.length ; x++) {
    		int len = 0;
    		char cond = ' ';
    		while(expression[x].charAt(len) != '>' && expression[x].charAt(len) != '<' && expression[x].charAt(len) != '=') {
    			len++;
    		}
    		cond = expression[x].charAt(len);
    		String first = expression[x].substring(0, len);
    		String second = expression[x].substring(len+1, expression[x].length());
    		//System.out.println(first);
    		//System.out.println(second);
    		//System.out.println(cond);
    		//process first argument
    		String hold = "";
    		if(first.charAt(0) != 's' && first.charAt(0) != 'c' && first.charAt(0) != 'm' && first.charAt(0) != 'a') {
    			hold = "";
    			len = 0;
    			while(first.charAt(len) != '+' && first.charAt(len) != '-' && first.charAt(len) != '*' && first.charAt(len) != '/') {
    				
    				len++;
    			}
    			hold = first.substring(0, len+1);
    			first = first.substring(len+1, first.length());
    		}
    		//System.out.println(hold);
    		//System.out.println(first);
    		out += hold;
    		len = 0;
    		while(first.charAt(len) != '(') {
    			len++;
    		}
    		String tag = first.substring(0, len);
    		first = first.substring(len+1, first.length());
    		len = 0;
    		while(first.charAt(len) != '.') {
    			len++;
    		}
    		String name = first.substring(0, len);
    		//System.out.println(name);
    		out = out + name + "." + tag + ".get(k)" + cond;
    		//process second argument
    		hold = "";
    		if(second.charAt(0) != 's' && second.charAt(0) != 'c' && second.charAt(0) != 'm' && second.charAt(0) != 'a') {
    			hold = "";
    			len = 0;
    			while(second.charAt(len) != '+' && second.charAt(len) != '-' && second.charAt(len) != '*' && second.charAt(len) != '/') {
    				
    				len++;
    			}
    			hold = second.substring(0, len+1);
    			second = second.substring(len+1, second.length());
    		}
    		//System.out.println(hold);
    		//System.out.println(second);
    		out += hold;
    		len = 0;
    		while(second.charAt(len) != '(') {
    			len++;
    		}
    		tag = second.substring(0, len);
    		second = second.substring(len+1, second.length());
    		len = 0;
    		while(second.charAt(len) != '.') {
    			len++;
    		}
    		name = second.substring(0, len);
    		//System.out.println(name);
    		out = out + name + "." + tag+".get(k)"; 
    		if(x < comprator.length) {
    			out += comprator[x];
    		}
    	}
    	out = out + ") {";
    	return out;
    }
    
    //remove space
	public static String removestr(String in) {
		String out = "";
		for(int x = 0 ; x < in.length() ; x++) {
			if(in.charAt(x) == ' ') continue;
			out += in.charAt(x);
		}
		return out;
	}
	
	//split according to and, or
	public static String[] split2(String in) {
		ArrayList<String> out = new ArrayList<>();
		int seg = 0;
		for(int x = 0 ; x < in.length() ; x++) {
			if(in.charAt(x) == 'o' && in.charAt(x+1) == 'r') {
				out.add(in.substring(seg, x));
				x += 2;
				seg = x;
			}
			if(in.charAt(x) == 'a' && in.charAt(x+1) == 'n' && in.charAt(x+2) == 'd') {
				out.add(in.substring(seg, x));
				x+=3;
				seg = x;
			}
		}
		out.add(in.substring(seg, in.length()));
		String[] outarr = new String[out.size()];
		for(int x = 0 ; x < out.size() ; x++) {
			outarr[x] = out.get(x);
		}
		return outarr;
	}
	
	public static String[] splitcompare(String in){
		ArrayList<String> out = new ArrayList<>();
		for(int x = 0 ; x < in.length() ; x++) {
			if(in.charAt(x) == 'o' && in.charAt(x+1) == 'r') {
				out.add("||");
				x += 2;
			}
			if(in.charAt(x) == 'a' && in.charAt(x+1) == 'n' && in.charAt(x+2) == 'd') {
				out.add("&&");
				x+=3;
			}
		}
		String[] outarr = new String[out.size()];
		for(int x = 0 ; x < out.size() ; x++) {
			outarr[x] = out.get(x);
		}
		return outarr;
	}
	
    private static void runProcess(String command) throws Exception {
        Process pro = Runtime.getRuntime().exec(command);
        printLines(command + " stdout:", pro.getInputStream());
        printLines(command + " stderr:", pro.getErrorStream());
        pro.waitFor();
        System.out.println(command + " exitValue() " + pro.exitValue());
    }
    
    private static void printLines(String name, InputStream ins) throws Exception {
        String line = null;
        BufferedReader in = new BufferedReader(new InputStreamReader(ins));
        while ((line = in.readLine()) != null) {
          System.out.println(name + " " + line);
        }
      }
}

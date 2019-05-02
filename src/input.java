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

import datastructure.phi;
import transfer.esqltophi;

public class input {
    public static PrintWriter writer;

	public static void main(String[] args) throws IOException {
		writer = new PrintWriter("src/output.java", "UTF-8");
		
		phi phidata = esqltophi.fromfile();
		//phidata.printphi();
		//System.out.println(processhaving(phidata.G));
		ArrayList<String> st = new ArrayList<>();
		ArrayList<String> exp = new ArrayList<>();
		ArrayList<String> res = new ArrayList<>();
		exp.add("String[] keys = {resultSet.getString(1)};");//Grouping Attribute Extention
		exp.add("HashSet<String> tmpset = new HashSet<>();");
		exp.add("tmpset.addAll(Arrays.asList(keys));");
		exp.add("globalkey.add(tmpset);");
		for(int x = 0 ; x < phidata.n ; x++) {
			  int len = 0;
			  while(phidata.theta.get(x).charAt(len) != '.') {
				  len++;
			  }
			  String name = phidata.theta.get(x).substring(0, len);
			  st.add("aggregates " + name + " = new aggregates();");
			  String Label = "";
			  while(phidata.theta.get(x).charAt(len) != '=') {
				  len++;
				  Label += phidata.theta.get(x).charAt(len);
			  }
			  int len2 = 0;
			  Object[] Farr = phidata.F.toArray();
			  while(Farr[0].toString().charAt(len2) != '.') {
				  len2++;
			  }
			  String Label2 = Farr[0].toString().substring(len2+1, Farr[0].toString().length()-1);
			  //System.out.println(Label2);
			  //System.out.println(Label.substring(0, Label.length()-1));
			  Label = Label.substring(0, Label.length()-1);
			  String condition = phidata.theta.get(x).substring(len+1, phidata.theta.get(x).length());
			  if(condition.charAt(0) != '\"') condition = '\"' + condition;
			  if(condition.charAt(condition.length()-1) != '\"') condition = condition + '\"';
			  String[] expression  = {"if(resultSet.getString(\""+ Label +"\").equals(" + condition +")){" , name + ".update(resultSet.getInt(\""+ Label2 +"\") , keys);" , "}"};
			  exp.addAll(Arrays.asList(expression));
			  String print =  name+".printresult();";
			  res.add(print);
			  res.add("System.out.println(\"------------------------------------------------\");");
		}
		
		before();
		//addstatment("System.out.println(\"Hello World!\");");
		addquery("select * from sales " + phidata.W);
		printattribute();
		addstatment("globalkey = new HashSet<>();");
		//String[] data = {"for(int i = 1; i <= columnsNumber; i++){" , "System.out.printf(\"%-30.30s\" , resultSet.getString(i));" , "}" , "System.out.println();"};
		for(int x = 0 ; x < st.size() ; x++) {
			addstatment(st.get(x));
		}
		addwhile("resultSet.next()" , "" , exp);
		for(int x = 0 ; x < res.size() ; x++) {
			addstatment(res.get(x));
		}
		addstatment("for(HashSet<String> k: globalkey) {");
		addstatment(processhaving(phidata.G));
		addstatment("System.out.println(k.toString() + \" should output!\");");//project part
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
		writer.println("import java.util.HashSet;");
		writer.println("import java.util.Arrays;");
		writer.println("import datastructure.*;");
        writer.println("public class output{");
        writer.println("public static HashSet<HashSet<String>> globalkey;");
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
    
    public static void addwhile(String loopcounter , String counterchange , ArrayList<String> exec) {
    	writer.println("while("+loopcounter+") {");
    	for(int x = 0 ; x < exec.size() ; x++) {
    		writer.println(exec.get(x));
    	}
    	writer.println(counterchange);
    	writer.println("}");
    }
    
    public static void addfor(String startcondition , String endcondition , String counterchange , String[] exec) {
    	writer.println("for(" + startcondition +" ; " + endcondition + " ; " + counterchange + "){");
    	for(int x = 0 ; x < exec.length ; x++) {
    		writer.println(exec[x]);
    	}
    	writer.println("}");
    }
    
    public static void addhashmap(String name) {
    	writer.println("HashMap<ArrayList<String> , Integer> " + name + " = new HashMap<>();");
    }
    
    public static void existedmap(String name , String key , String value , String action) {
    	switch(action) {
    		case "put":
    			writer.println(name + ".put(" + key + "," + value + ");");
    		break;
    		case "rem":
    			writer.println(name + ".remove(" + key + ");");
    		break;
    		case "rep":
    			writer.println(name + ".replace(" + key + "," + value + ");");
    		break;
    		default:
    	}
    }
    
    public static void traviseHashMap(String name , String[] exec) {
    	writer.println("for(ArrayList<String> name: " + name + ".keySet()) {");
    	writer.println("String key = name.toString();");
    	writer.println("int value = " + name + ".get(name);"); 
    	for(int x = 0 ; x < exec.length ; x++) {
    		writer.println(exec[x]);
    	}
        writer.println("}");
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
    
	public static String removestr(String in) {
		String out = "";
		for(int x = 0 ; x < in.length() ; x++) {
			if(in.charAt(x) == ' ') continue;
			out += in.charAt(x);
		}
		return out;
	}
	
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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

public class input {
    public static PrintWriter writer;

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		writer = new PrintWriter("src/output.java", "UTF-8");
		
		before();
		//addstatment("System.out.println(\"Hello World!\");");
		addquery("select * from sales");
		printattribute();
		addstatment("int cnt = 0;");
		String[] data = {"for(int i = 1; i <= columnsNumber; i++){" , "System.out.printf(\"%-30.30s\" , resultSet.getString(i));" , "}" , "System.out.println();"};
		addwhile("resultSet.next() && cnt++<=30" , "" , data);
		addhashmap("minmap");
		addstatment("ArrayList<String> a = new ArrayList<>();");
		addstatment("a.add(\"Hello\");");
		addstatment("a.add(\"World\");");
		existedmap("minmap" , "a" , "1" , "put");
		String[] data2 = {"System.out.println(key + \" \" + value);"};
		traviseHashMap("minmap", data2);
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
        writer.println("public class output{");
        writer.println("public static void main(String[] args){");
        writer.println("try{");
        writer.println("Class.forName(\"org.postgresql.Driver\");");
        writer.println("Connection connection = DriverManager.getConnection(\"jdbc:postgresql://localhost:5432/postgres\", \"postgres\", \"postgres\");");
        writer.println("Statement statement = connection.createStatement();");
	}
    public static void after(){
    	writer.println("}");
    	writer.println("catch (SQLException | ClassNotFoundException e) {");
    	//writer.println("catch (SQLException e) {");
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
    
    public static void addwhile(String loopcounter , String counterchange , String[] exec) {
    	writer.println("while("+loopcounter+"){");
    	for(int x = 0 ; x < exec.length ; x++) {
    		writer.println(exec[x]);
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

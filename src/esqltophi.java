import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import datastructure.phi;

public class esqltophi {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			fromfile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static phi fromfile() throws IOException {
		  File file = new File("C:\\Users\\howar\\Desktop\\eclipse-workspace\\CS562-project\\src\\esql.txt"); 
		  
		  BufferedReader br = new BufferedReader(new FileReader(file)); 
		  phi output = new phi();
		  String st; 
		  int len = 0;
		  String[] tmp = new String[2];
		  ArrayList<String> data = new ArrayList<>();
		  while ((st = br.readLine()) != null) {
		    //System.out.println(st);
			  len = 0;
			  while(st.charAt(len) != ' ') {
				  len++;
			  }
			  tmp[0] = st.substring(0, len);
			  tmp[1] = st.substring(len+1, st.length());
			  //System.out.println("t0:"+tmp[0]);
			  //System.out.println("t1:"+tmp[1]);
			  switch(tmp[0].toLowerCase()) {
			  case "select":
				  output.addtophi("s", tmp[1]);
				  System.out.println("select0:"+tmp[0]);
				  System.out.println("select1:"+tmp[1]);
				  break;
			  case "group":
				  output.addtophi("v", tmp[1].substring(3, tmp[1].length()));
				  System.out.println("group0:"+tmp[0]);
				  System.out.println("group1:"+tmp[1].substring(3, tmp[1].length()));
				  break;
			  case"such":
				  output.addtophi("t", tmp[1].substring(5, tmp[1].length()-1));
				  System.out.println("sucht0:"+tmp[0]);
				  System.out.println("sucht1:"+tmp[1].substring(5, tmp[1].length()));
				  break;
			  case"having":
				  output.addtophi("g", tmp[1]);
				  System.out.println("having0:"+tmp[0]);
				  System.out.println("having1:"+tmp[1]);
				  break;
			  default:
				  System.out.println("default0:"+tmp[0]);
				  System.out.println("default1:"+tmp[1]);
			  }
			  String[] aggregate = tmp[1].split(" ");
			  for(int x = 0 ; x < aggregate.length ; x++) {
				  
				  if(aggregate[x].length() >= 3) {
					  if(aggregate[x].substring(0, 3).equals("sum") || 
							  aggregate[x].substring(0, 3).equals("max") ||
							  aggregate[x].substring(0, 3).equals("min") ||
							  aggregate[x].substring(0, 3).equals("avg")) {
						  output.addtophi("f", removestr(aggregate[x]));
					  }
				  }
				  if(aggregate[x].length() >= 5) {
					  if(aggregate[x].substring(0, 5).equals("count")) {
						  output.addtophi("f", removestr(aggregate[x]));
					  }
				  }
			  }
		  } 
		  System.out.println("\nphi data:"); 
		  output.printphi();
		  br.close();
		  return output;
	}
	
	public static String removestr(String in) {
		String out = "";
		for(int x = 0 ; x < in.length() ; x++) {
			if(in.charAt(x) == ',' || in.charAt(x) == ' ' || in.charAt(x) == ';') continue;
			out += in.charAt(x);
		}
		return out;
	}

}

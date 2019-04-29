package datastructure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class phi {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public ArrayList<String> S; //Select clause
	public int n; //Number of grouping variables
	public HashSet<String> F; //aggregate
	public ArrayList<String> theta; //such that clause
	public String G; //having clause
	public ArrayList<String> V; //List of grouping attributes
	
	public phi() {
		S = new ArrayList<>();
		n = 0;
		F = new HashSet<>();
		theta = new ArrayList<>();
		G = "";
		V = new ArrayList<>();
	}
	
	public void addtophi(String action , String data) {
		switch(action.toLowerCase()) {
		case "s":
			S.addAll(Arrays.asList(removespace(data).split(",")));
			//System.out.println(S.toString());
			break;
		case "t":
			theta.addAll(Arrays.asList(removespace(data).split(",")));
			//System.out.println(theta.toString());
			break;
		case "g":
			G = data;
			break;
		case "v":
			int len = 0;
			while(data.charAt(len) != ':') len++;
			V.addAll(Arrays.asList(removespace(data.substring(0, len)).split(",")));
			//System.out.println(V);
			String[] tmp = removespace(data.substring(len , data.length())).split(",");
			n = tmp.length;
			//System.out.println(n);
			break;
		case "f":
			F.addAll(Arrays.asList(data));
			break;
		default:
		}
	}
	
	public void printphi() {
		System.out.println("S:"+this.S);
		System.out.println("n:"+this.n);
		System.out.println("F:"+this.F);
		System.out.println("theta:"+this.theta);
		System.out.println("G:"+this.G);
		System.out.println("V:"+this.V);
	}
	
	public String removespace(String in) {
		String out = "";
		for(int x = 0 ; x < in.length() ; x++) {
			if(in.charAt(x) == ' ') continue;
			out += in.charAt(x);
		}
		return out;
	}

}

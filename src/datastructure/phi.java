package datastructure;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class phi {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String tmp = "state=\"NJ\"";
		for(int x = 0 ; x < tmp.length() ; x++) {
			if(tmp.charAt(x) == '\"') {
				String tmp1 = tmp.substring(0, x);
				String tmp2 = tmp.substring(x,tmp.length());
				tmp1 += "\\";
				tmp = tmp1 + tmp2;
				x++;
			}
		}
		System.out.println(tmp);
	}
	
	public ArrayList<String> S; //Select clause
	public int n; //Number of grouping variables
	public HashSet<String> F; //aggregate
	public ArrayList<String> theta; //such that clause
	public String G; //having clause
	public ArrayList<String> V; //List of grouping attributes
	public String W;//where clause
	
	public phi() {
		this.S = new ArrayList<>();
		this.n = 0;
		this.F = new HashSet<>();
		this.theta = new ArrayList<>();
		this.G = "";
		this.V = new ArrayList<>();
		this.W = "";
	}
	
	public void addtophi(String action , String data) {
		switch(action.toLowerCase()) {
		case "s":
			this.S.addAll(Arrays.asList(removespace(data).split(",")));
			//System.out.println(S.toString());
			break;
		case "t":
			String tmps = removespace(data);
			for(int x = 0 ; x < tmps.length() ; x++) {
				if(tmps.charAt(x) == '\"') {
					String tmp1 = tmps.substring(0, x);
					String tmp2 = tmps.substring(x,tmps.length());
					tmp1 += "\\";
					tmps = tmp1 + tmp2;
					x++;
				}
			}
			this.theta.addAll(Arrays.asList(tmps.split(",")));
			//System.out.println(theta.toString());
			break;
		case "g":
			this.G = data;
			break;
		case "v":
			int len = 0;
			while(data.charAt(len) != ':') len++;
			this.V.addAll(Arrays.asList(removespace(data.substring(0, len)).split(",")));
			//System.out.println(V);
			String[] tmp = removespace(data.substring(len , data.length())).split(",");
			this.n = tmp.length;
			//System.out.println(n);
			break;
		case "f":
			this.F.addAll(Arrays.asList(data));
			break;
		case "w":
			this.W = data;
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
		System.out.println("W:"+this.W);
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

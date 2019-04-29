package datastructure;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class aggregates2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public ArrayList<String> g_attri;
	public HashMap<ArrayList<String> , Integer> max;
	public HashMap<ArrayList<String> , Integer> min;
	public HashMap<ArrayList<String> , Integer> sum;
	public HashMap<ArrayList<String> , Float> avg;
	public HashMap<ArrayList<String> , Integer> count;
	
	public aggregates2(ResultSet r , ArrayList<String> group) throws SQLException {
		this.g_attri = new ArrayList<>();
		this.max = new HashMap<>();
		this.min = new HashMap<>();
		this.sum = new HashMap<>();
		this.avg = new HashMap<>();
		this.count = new HashMap<>();
		for(int x = 0 ; x < group.size() ; x++) {
			g_attri.add(group.get(x));
		}
		do {
			ArrayList<String> tmp = new ArrayList<>();
			for(int x = 0 ; x < group.size() ; x++) {
				tmp.add(r.getString(group.get(x)));
			}
			this.max.put(tmp, null);
			this.min.put(tmp, null);
			this.sum.put(tmp, null);
			this.avg.put(tmp, null);
			this.count.put(tmp, null);
		}while(r.next());
		r.first();
	}
	
	public void update(int data , String[] keys) {
		ArrayList<String> key = new ArrayList<>();
		key.addAll(Arrays.asList(keys));
		//max
		if(this.max.get(key) == null) {
			this.max.replace(key, data);
		}
		else {
			int curmax = this.max.get(key);
			if(data > curmax) {
				this.max.replace(key, data);
			}
		}
		//min
		if(this.min.get(key) == null) {
			this.min.replace(key, data);
		}
		else {
			int curmin = this.min.get(key);
			if(data < curmin) {
				this.min.replace(key, data);
			}
		}
		//sum
		if(this.sum.get(key) == null) {
			this.sum.replace(key, data);
		}
		else {
			int cursum = this.sum.get(key);
			this.sum.replace(key, cursum+data);
		}
		//count
		if(this.count.get(key) == null) {
			this.count.replace(key, 1);
		}
		else {
			int curcount = this.count.get(key);
			this.count.replace(key, curcount+1);
		}
		//avg
		if(this.avg.get(key) == null) {
			this.avg.put(key, (float) data);
		}
		else {
			float curavg = this.sum.get(key);
			int curcnt = this.count.get(key);
			this.avg.replace(key, curavg/curcnt);
		}
	}
	public void printresult() {
		System.out.println("in printresult");
		for(ArrayList<String> k: this.avg.keySet()) {
			System.out.println("\nkey" + k.toString());
			System.out.print("max:" + this.max.get(k));
			System.out.print("|min:" + this.min.get(k));
			System.out.print("|avg:" + this.avg.get(k));
			System.out.print("|sum:" + this.sum.get(k));
			System.out.print("|count:" + this.count.get(k) + "\n");
		}
	}
	
	public int agpos(String target) {
		for(int x = 0 ; x < g_attri.size() ; x++) {
			if(g_attri.get(x).equals(target))return x;
		}
		return -1;
	}
	
	public void addtomap(ResultSet result , String key , String value) throws SQLException {
		//this.printresult();
		int pos = agpos(key);
		int data = result.getInt("quant");
		for(ArrayList<String> k : this.max.keySet()) {
			if(!removespace(k.get(pos)).equals(value)) {
				if(this.max.get(k) == null) {
					this.max.replace(k, data);
				}
				else {
					int curmax = this.max.get(k);
					if(data > curmax) {
						this.max.replace(k, data);
					}
				}
				//min
				if(this.min.get(k) == null) {
					this.min.replace(k, data);
				}
				else {
					int curmin = this.min.get(k);
					if(data < curmin) {
						this.min.replace(k, data);
					}
				}
				//sum
				if(this.sum.get(k) == null) {
					this.sum.replace(k, data);
				}
				else {
					int cursum = this.sum.get(k);
					this.sum.replace(k, cursum+data);
				}
				//count
				if(this.count.get(k) == null) {
					this.count.replace(k, 1);
				}
				else {
					int curcount = this.count.get(k);
					this.count.replace(k, curcount+1);
				}
				//avg
				if(this.avg.get(k) == null) {
					this.avg.put(k, (float) data);
				}
				else {
					float curavg = this.sum.get(k);
					int curcnt = this.count.get(k);
					this.avg.replace(k, curavg/curcnt);
				}
			}
		}
	}
	
	public String[] tostrarr(ArrayList<String> in) {
		String[] out = new String[in.size()];
		for(int x = 0 ; x < in.size() ; x++) {
			out[x] = in.get(x);
		}
		return out;
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

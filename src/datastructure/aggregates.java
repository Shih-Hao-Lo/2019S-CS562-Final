package datastructure;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class aggregates {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public HashSet<String> g_attributes;
	public HashMap<HashSet<String> , Integer> max;
	public HashMap<HashSet<String> , Integer> min;
	public HashMap<HashSet<String> , Integer> sum;
	public HashMap<HashSet<String> , Float> avg;
	public HashMap<HashSet<String> , Integer> count;
	
	public aggregates() {
		this.g_attributes = new HashSet<>();
		this.max = new HashMap<>();
		this.min = new HashMap<>();
		this.sum = new HashMap<>();
		this.avg = new HashMap<>();
		this.count = new HashMap<>();
	}
	
	public void update(int data , String[] keys) {
		HashSet<String> key = new HashSet<>();
		key.addAll(Arrays.asList(keys));
		//max
		if(!this.max.containsKey(key)) {
			this.max.put(key, data);
		}
		else {
			int curmax = this.max.get(key);
			if(data > curmax) {
				this.max.replace(key, data);
			}
		}
		//min
		if(!this.min.containsKey(key)) {
			this.min.put(key, data);
		}
		else {
			int curmin = this.min.get(key);
			if(data < curmin) {
				this.min.replace(key, data);
			}
		}
		//sum
		if(!this.sum.containsKey(key)) {
			this.sum.put(key, data);
		}
		else {
			int cursum = this.sum.get(key);
			this.sum.replace(key, cursum+data);
		}
		//count
		if(!this.count.containsKey(key)) {
			this.count.put(key, 1);
		}
		else {
			int curcount = this.count.get(key);
			this.count.replace(key, curcount+1);
		}
		//avg
		if(!this.avg.containsKey(key)) {
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
		for(HashSet<String> k: this.avg.keySet()) {
			System.out.println("\nkey" + k.toString());
			System.out.print("max:" + this.max.get(k));
			System.out.print("|min:" + this.min.get(k));
			System.out.print("|avg:" + this.avg.get(k));
			System.out.print("|sum:" + this.sum.get(k));
			System.out.print("|count:" + this.count.get(k) + "\n");
		}
	}
}

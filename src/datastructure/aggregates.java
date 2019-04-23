package datastructure;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class aggregates {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	public static HashSet<String> g_attributes;
	public static HashMap<HashSet<String> , Integer> max;
	public static HashMap<HashSet<String> , Integer> min;
	public static HashMap<HashSet<String> , Integer> sum;
	public static HashMap<HashSet<String> , Float> avg;
	public static HashMap<HashSet<String> , Integer> count;
	
	public aggregates() {
		g_attributes = new HashSet<>();
		max = new HashMap<>();
		min = new HashMap<>();
		sum = new HashMap<>();
		avg = new HashMap<>();
		count = new HashMap<>();
	}
	
	public void update(int data , String[] keys) {
		HashSet<String> key = new HashSet<>();
		key.addAll(Arrays.asList(keys));
		//max
		if(!max.containsKey(key)) {
			max.put(key, data);
		}
		else {
			int curmax = max.get(key);
			if(data > curmax) {
				max.replace(key, data);
			}
		}
		//min
		if(!min.containsKey(key)) {
			min.put(key, data);
		}
		else {
			int curmin = min.get(key);
			if(data < curmin) {
				min.replace(key, data);
			}
		}
		//sum
		if(!sum.containsKey(key)) {
			sum.put(key, data);
		}
		else {
			int cursum = sum.get(key);
			sum.replace(key, cursum+data);
		}
		//count
		if(!count.containsKey(key)) {
			count.put(key, 1);
		}
		else {
			int curcount = count.get(key);
			count.replace(key, curcount+1);
		}
		//avg
		if(!avg.containsKey(key)) {
			avg.put(key, (float) data);
		}
		else {
			float curavg = sum.get(key);
			int curcnt = count.get(key);
			avg.replace(key, (curavg+data)/curcnt);
		}
	}
	public void printresult() {
		System.out.println("\n\nin printresult");
		for(HashSet<String> k: avg.keySet()) {
			System.out.println("\nkey" + k.toString());
			System.out.print("max:" + max.get(k));
			System.out.print("|min:" + min.get(k));
			System.out.print("|avg:" + avg.get(k));
			System.out.print("|sum:" + sum.get(k));
			System.out.print("|count:" + count.get(k));
		}
	}
}

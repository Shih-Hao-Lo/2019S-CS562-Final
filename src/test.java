import java.util.HashSet;

import datastructure.aggregates;
public class test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String[] s = {"cust"};
		aggregates ag = new aggregates();
		String[] a = {"a"};
		String[] b = {"b"};
		String[] c = {"c"};

		ag.update(2, a);
		ag.update(7, b);
		ag.update(8, c);
		ag.update(4, a);
		ag.update(6, a);
		ag.update(9, b);
		ag.update(11, a);
		ag.update(22, c);
		ag.update(1, a);
		ag.update(0, c);
		for(HashSet<String> k: ag.avg.keySet()) {
			System.out.println("key"+k.toString());
			System.out.println("max"+ag.max.get(k));
			System.out.println("min"+ag.min.get(k));
			System.out.println("avg"+ag.avg.get(k));
			System.out.println("sum"+ag.sum.get(k));
			System.out.println("count"+ag.count.get(k));
		}
	}

}

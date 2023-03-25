package util;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Sort {

	public Sort() {}

	public List<Integer> sortedIndex(double weight[])  {
		List<Integer> list = new ArrayList<Integer>(weight.length);
		for (int i = 0; i < weight.length; i++) {
			list.add(i);
		}
		list.sort(new myComparator<Integer>(weight));
		return list;
	}
}

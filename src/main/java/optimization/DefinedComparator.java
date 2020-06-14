package main.java.optimization;

import java.io.Serializable;
import java.util.Comparator;


public class DefinedComparator  implements Comparator<Integer>,Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Integer o1, Integer o2) {
		return o2-o1;
	}

}

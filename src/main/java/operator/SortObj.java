package main.java.operator;

import java.io.Serializable;
import java.util.Comparator;

public class SortObj implements Comparator<Integer>,Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Integer v1, Integer v2) {
		 
		return v2 - v1;
	}

}

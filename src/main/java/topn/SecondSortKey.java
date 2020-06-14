package main.java.topn;

import java.io.Serializable;

/**
 * 1、自定义的二次排序的类 必须是可序列化的。 进行网络的传输
 * 2、必须实现Comparable接口
 * @author root
 *
 */
public class SecondSortKey implements Serializable,Comparable<SecondSortKey> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer first;
	private Integer second;
	public Integer getFirst() {
		return first;
	}
	public void setFirst(Integer first) {
		this.first = first;
	}
	public Integer getSecond() {
		return second;
	}
	public void setSecond(Integer second) {
		this.second = second;
	}
	public SecondSortKey(Integer first, Integer second) {
		super();
		this.first = first;
		this.second = second;
	}
	@Override
	public int compareTo(SecondSortKey that) {
		if(getFirst() - that.getFirst() == 0 ){
			return getSecond() - that.getSecond();
		}else{
			return getFirst() - that.getFirst();
		}
	}
	@Override
	public String toString() {
		return "First:" + getFirst() + "\tSecond:" + getSecond();
	}
}

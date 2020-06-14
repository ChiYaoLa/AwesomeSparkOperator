package main.java.puv;

public class SortObj {
	private String key;
	private Integer value;
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public Integer getValue() {
		return value;
	}
	public void setValue(Integer value) {
		this.value = value;
	}
	public SortObj(String key, Integer value) {
		super();
		this.key = key;
		this.value = value;
	}
	@Override
	public String toString() {
		return key+"==="+value;
	}	
}

public class StringAndInt implements Comparable<StringAndInt> {
	private String tag;
	private int count;
	
	public StringAndInt(String tag, int count){
	
		this.count = count;
		this.tag = tag;
	
	}
	
	
	
	@Override
	public int compareTo(StringAndInt o) {
		if (this.count < o.count)
			return 1;
		else if (this.count > o.count)
			return -1;		
		return 0;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	

}

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringAndInt2 implements Comparable<StringAndInt2>, Writable {
	private Text tag;
	private int count;
	
	public StringAndInt2(Text tag, int count){
	
		this.count = count;
		this.tag = tag;
	
	}
	
	public StringAndInt2(){
        this.count = 0;
        this.tag = new Text();  
    }
    
	
	@Override
	public int compareTo(StringAndInt2 o) {
		if (this.count < o.count)
			return 1;
		else if (this.count > o.count)
			return -1;		
		return 0;
	}

	public Text getTag() {
		return tag;
	}

	public void setTag(Text tag) {
		this.tag = tag;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

    @Override
    public void readFields(DataInput arg0) throws IOException {
        this.tag.readFields(arg0);
        this.count = arg0.readInt();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        this.tag.write(arg0);
        arg0.writeInt(this.count);
    }
}

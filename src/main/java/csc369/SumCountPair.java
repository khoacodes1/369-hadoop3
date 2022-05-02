package csc369;

import java.io.*;
import org.apache.hadoop.io.*;

public class SumCountPair implements Writable {
    private int sum;
    private int count;

    public SumCountPair(){
    }
    
    public int getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }
    
    public SumCountPair(int sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readInt();
        count = in.readInt();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
        out.writeInt(count);
    }
}

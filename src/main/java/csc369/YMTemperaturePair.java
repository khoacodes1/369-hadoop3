package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;

public class YMTemperaturePair
    implements Writable, WritableComparable<YMTemperaturePair> {
    
    private final Text yearMonth = new Text();
    private final IntWritable temperature = new IntWritable();
    
    public YMTemperaturePair() {
    }
    
    public YMTemperaturePair(String yearMonth, int temperature) {
        this.yearMonth.set(yearMonth);
        this.temperature.set(temperature);
    }
    
    @Override
    public void write(DataOutput out) throws IOException{
        yearMonth.write(out);
        temperature.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        yearMonth.readFields(in);
        temperature.readFields(in);
    }
    
    @Override
    public int compareTo(YMTemperaturePair pair) {
        if (yearMonth.compareTo(pair.getYearMonth()) == 0) {
            return temperature.compareTo(pair.temperature);
        }
        return yearMonth.compareTo(pair.getYearMonth());
    }
    
    public Text getYearMonth() {
        return yearMonth;
    }
    
    public IntWritable getTemperature() {
        return temperature;
    }
    
}

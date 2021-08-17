package HaddopMethods.FirstWordMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FirstWordMapOutput implements Writable {
    public int count_w1;
    public int count_w1w2;

    //hadoop empty builder
    public FirstWordMapOutput() {}

    public FirstWordMapOutput(int count_w1, int count_w1w2) {
        this.count_w1 = count_w1;
        this.count_w1w2 = count_w1w2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count_w1);
        dataOutput.writeInt(count_w1w2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count_w1 = dataInput.readInt();
        count_w1w2 =dataInput.readInt();
    }

    @Override
    public String toString() {
        return count_w1 + " " + count_w1w2;
    }
}

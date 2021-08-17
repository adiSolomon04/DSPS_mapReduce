package HaddopMethods.calculateN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class calculateNMapData implements WritableComparable<calculateNMapData> {
    public String w1;
    public String w2;

    //hadoop empty builder
    public calculateNMapData(){}

    public calculateNMapData(String w1, String w2) {
        this.w1 = w1;
        this.w2 = w2;
    }

    @Override
    public int compareTo(calculateNMapData object) {
        if (this.w1.compareTo(object.w1) == 0)
                return this.w2.compareTo(object.w2);
        else
            return this.w1.compareTo(object.w1);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(w1);
        dataOutput.writeUTF(w2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        w1 = dataInput.readUTF();
        w2 = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return w1 + " " + w2 ;
    }
}

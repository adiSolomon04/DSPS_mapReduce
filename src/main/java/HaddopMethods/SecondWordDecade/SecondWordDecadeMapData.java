package HaddopMethods.SecondWordDecade;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondWordDecadeMapData implements WritableComparable<SecondWordDecadeMapData> {
    public String w1;
    public String w2;
    public String Decade;

    //hadoop empty builder
    public SecondWordDecadeMapData(){}

    public SecondWordDecadeMapData(String w1, String w2, String decade) {
        this.w1 = w1;
        this.w2 = w2;
        this.Decade = decade;
    }

    @Override
    public int compareTo(SecondWordDecadeMapData object) {
        if(this.w2.equals(object.w2)) {
                if (this.w1.equals(object.w1)) {
                    return this.Decade.compareTo(object.Decade);
                } else
                    return this.w1.compareTo(object.w1);
            }
        else
            return this.w2.compareTo(object.w2);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(w1);
        dataOutput.writeUTF(w2);
        dataOutput.writeUTF(Decade);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        w1 = dataInput.readUTF();
        w2 = dataInput.readUTF();
        Decade = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return w1 + " " + w2 + " " +Decade;
    }
}

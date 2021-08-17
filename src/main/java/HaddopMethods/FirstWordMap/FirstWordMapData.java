package HaddopMethods.FirstWordMap;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FirstWordMapData implements WritableComparable<FirstWordMapData> {
    public String w1;
    public String w2;
    public String Decade;

    //hadoop empty builder
    public FirstWordMapData(){}

    public FirstWordMapData(String w1, String w2, String decade) {
        this.w1 = w1;
        this.w2 = w2;
        this.Decade = decade;
    }

    @Override
    public int compareTo(FirstWordMapData object) {
        if (this.w1.compareTo(object.w1) == 0)
            if(this.w2.compareTo(object.w2) ==0)
                return this.Decade.compareTo(object.Decade); //todo: ask adler if it ok to assume smallest // yes because its in stop words
            else
                return this.w2.compareTo(object.w2);
        else
            return this.w1.compareTo(object.w1);
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

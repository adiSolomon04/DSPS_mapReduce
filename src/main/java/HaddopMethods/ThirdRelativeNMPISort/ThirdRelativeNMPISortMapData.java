package HaddopMethods.ThirdRelativeNMPISort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ThirdRelativeNMPISortMapData implements WritableComparable<ThirdRelativeNMPISortMapData> {
    public String w1;
    public String w2;
    public String Decade;
    public Double relativeNMPI;

    //hadoop empty builder
    public ThirdRelativeNMPISortMapData(){}

    public ThirdRelativeNMPISortMapData(String w1, String w2, String decade, Double relativeNMPI) {
        this.w1 = w1;
        this.w2 = w2;
        this.Decade = decade;
        this.relativeNMPI = relativeNMPI;
    }

    @Override
    public int compareTo(ThirdRelativeNMPISortMapData object) {
        if(this.Decade.equals(object.Decade)) {
               return this.relativeNMPI.compareTo(object.relativeNMPI);
            }
        else
            return this.Decade.compareTo(object.Decade);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(w1);
        dataOutput.writeUTF(w2);
        dataOutput.writeUTF(Decade);
        dataOutput.writeUTF(String.valueOf(relativeNMPI));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        w1 = dataInput.readUTF();
        w2 = dataInput.readUTF();
        Decade = dataInput.readUTF();
        relativeNMPI = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return Decade + " " + w1 + " " + w2;
    }
}

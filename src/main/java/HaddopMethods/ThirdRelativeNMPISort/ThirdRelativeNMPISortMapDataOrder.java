package HaddopMethods.ThirdRelativeNMPISort;


import org.apache.hadoop.io.WritableComparable;

        import java.io.DataInput;
        import java.io.DataOutput;
        import java.io.IOException;

public class ThirdRelativeNMPISortMapDataOrder implements WritableComparable<ThirdRelativeNMPISortMapDataOrder> {
    public String w1;
    public String w2;
    public String Decade;
    public Double npmi;

    //hadoop empty builder
    public ThirdRelativeNMPISortMapDataOrder(){}

    public ThirdRelativeNMPISortMapDataOrder(String w1, String w2, String decade, Double npmi) {
        this.w1 = w1;
        this.w2 = w2;
        this.Decade = decade;
        this.npmi = npmi;
    }

    @Override
    public int compareTo(ThirdRelativeNMPISortMapDataOrder object) {
        if(this.Decade.equals(object.Decade)) {
            if (this.w1.equals("*")||object.w1.equals("*")) {
                return this.w1.compareTo(object.w1);
            } else{
                if(this.npmi.equals(object.npmi)) {
                    if (this.w1.equals(object.w1))
                        return this.w2.compareTo(object.w2);
                    else
                        return this.w1.compareTo(object.w1);
                }
                else
                    return this.npmi.compareTo(object.npmi);
            }
        }
        else
            return this.Decade.compareTo(object.Decade);
    }

    public int compareTo2(ThirdRelativeNMPISortMapDataOrder other) {
        if(this.Decade.equals(other.Decade)) {
            if(this.npmi.equals(other.npmi)) {
                if (this.w1.equals(other.w1))
                    return this.w2.compareTo(other.w2);
                else
                    return this.w1.compareTo(other.w1);
            }
            else
                return Double.compare(this.npmi,other.npmi);
        }
        else
            return this.Decade.compareTo(other.Decade);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(w1);
        dataOutput.writeUTF(w2);
        dataOutput.writeUTF(Decade);
        dataOutput.writeUTF(String.valueOf(npmi));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        w1 = dataInput.readUTF();
        w2 = dataInput.readUTF();
        Decade = dataInput.readUTF();
        npmi = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return Decade + "\t" + w1 + "\t" + w2;
    }
}

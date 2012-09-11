import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextDuple
    implements WritableComparable<TextDuple> {

    private Text first;
    private Text second;

    public TextDuple() {
        set(new Text(), new Text());
    }

    public TextDuple(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public TextDuple(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override public boolean equals(Object o) {
        if (o instanceof TextDuple) {
            TextDuple td = (TextDuple) o;
            return first.equals(td.first) && second.equals(td.second);
        }

        return false;
    }

    @Override public String toString() {
        return first + "\t" + second;
    }

    @Override public int compareTo(TextDuple td) {
        int cmp = first.compareTo(td.first);
        if (cmp != 0) {
            return cmp;
        }

        return second.compareTo(td.second);
    }
}

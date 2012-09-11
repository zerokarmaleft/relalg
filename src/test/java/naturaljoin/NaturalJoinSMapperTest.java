import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.*;

public class NaturalJoinSMapperTest {

    @Test public void processValidRecord()
        throws IOException, InterruptedException {

        Text key = new Text("A");
        Text value = new Text("apple");

        new MapDriver<Text, Text, Text, TextDuple>()
            .withMapper(new NaturalJoinSMapper())
            .withInput(key, value)
            .withOutput(key, new TextDuple(new Text("S"), value))
            .runTest();
    }
}


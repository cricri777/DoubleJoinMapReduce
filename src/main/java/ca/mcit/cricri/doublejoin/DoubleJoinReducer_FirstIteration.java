package ca.mcit.cricri.doublejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import static ca.mcit.cricri.doublejoin.model.DataUtiliy.*;

@Slf4j
public class DoubleJoinReducer_FirstIteration extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // Split input in two list each of which representing one relation (left or right)
        List<String> leftRelation = new ArrayList<>();
        List<String> rightRelation = new ArrayList<>();
        for (Text value : values) {

            String record = value.toString();
            String[] fields = record.split(SEPARATOR);
            String relationName = fields[0];


            switch (relationName) {
                case FILE_NAME_R:
                    leftRelation.add(record);
                    break;
                case FILE_NAME_S:
                    rightRelation.add(record);
                    break;
                default:
                    log.info("Reducer relationName [{}], should be [{}] or [{}]", relationName, FILE_NAME_R, FILE_NAME_S);
            }

        }

        if (rightRelation.isEmpty() || leftRelation.isEmpty()) return;

        // "Put it together" using cartesian product
        for (String recordA : leftRelation) {
            for (String recordB : rightRelation) {
                String recordOut = recordA + SEPARATOR + recordB;
                // Commit
                context.write(new Text(recordB.split(SEPARATOR)[2]), new Text(recordOut));
            }
        }
    }
}
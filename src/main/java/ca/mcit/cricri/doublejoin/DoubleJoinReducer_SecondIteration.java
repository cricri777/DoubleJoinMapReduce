package ca.mcit.cricri.doublejoin;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static ca.mcit.cricri.doublejoin.model.DataUtiliy.FILE_NAME_T;
import static ca.mcit.cricri.doublejoin.model.DataUtiliy.SEPARATOR;

@Slf4j
public class DoubleJoinReducer_SecondIteration extends Reducer<Text, Text, Text, Text> {

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
                case FILE_NAME_T:
                    rightRelation.add(record);
                    break;
                case "part-r-00000":
                    leftRelation.add(record.replace("part-r-00000" + SEPARATOR, ""));
                    break;
                default:
                    log.info("Reducer relationName [{}], should be [{}] or [part-r-00000]", relationName, FILE_NAME_T);
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

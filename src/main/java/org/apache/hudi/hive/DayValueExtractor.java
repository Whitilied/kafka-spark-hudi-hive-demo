package org.apache.hudi.hive;

import org.apache.hudi.hive.PartitionValueExtractor;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Collections;
import java.util.List;

public class DayValueExtractor implements PartitionValueExtractor {

    private transient DateTimeFormatter dtfOut;

    public DayValueExtractor() {
        this.dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd");
    }

    private DateTimeFormatter getDtfOut() {
        if (dtfOut == null) {
            dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd");
        }
        return dtfOut;
    }

    @Override
    public List<String> extractPartitionValuesInPath(String partitionPath) {
        // partition path is expected to be in this format yyyy/mm/dd
        String[] splits = partitionPath.split("-");
        if (splits.length != 3) {
            throw new IllegalArgumentException(
                    "Partition path " + partitionPath + " is not in the form yyyy-mm-dd ");
        }
        // Get the partition part and remove the / as well at the end
        int year = Integer.parseInt(splits[0].contains("=") ? splits[0].split("=")[1] : splits[0]);
        int mm = Integer.parseInt(splits[1].contains("=") ? splits[1].split("=")[1] : splits[1]);
        int dd = Integer.parseInt(splits[2].contains("=") ? splits[2].split("=")[1] : splits[2]);
        DateTime dateTime = new DateTime(year, mm, dd, 0, 0);
        return Collections.singletonList(getDtfOut().print(dateTime));
    }

}

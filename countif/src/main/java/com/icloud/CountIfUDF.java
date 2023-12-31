package com.icloud;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.BooleanWritable;

@Description(
        name = "countif",
        value = "_FUNC_(condition) - Returns the count of rows where the condition is true",
        extended = "Example:\n" +
                   "  SELECT countif(column1 > 10) FROM table1;"
)
public class CountIfUDF extends UDAF {
    public static class CountIfEvaluator implements UDAFEvaluator {

        private int count;

        public CountIfEvaluator() {
            super();
            init();
        }

        public void init() {
            count = 0;
        }

        public boolean iterate(BooleanWritable condition) {
            if (condition != null && condition.get()) {
                count++;
            }
            return true;
        }

        public int terminatePartial() {
            return count;
        }

        public boolean merge(int other) {
            count += other;
            return true;
        }

        public int terminate() {
            return count;
        }
    }
}
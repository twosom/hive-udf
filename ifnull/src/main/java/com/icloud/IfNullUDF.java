package com.icloud;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;

@Description(
        name = "ifnull",
        value = "_FUNC_(expr1, expr2) - If expr1 is not null, returns expr1; otherwise, returns expr2",
        extended = "Example:\n" +
                   "  SELECT ifnull(column1, column2) FROM table1;"
)
public class IfNullUDF extends GenericUDF {

    private transient ObjectInspector[] argumentOIs;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function ifnull takes exactly two arguments");
        }
        this.argumentOIs = arguments;
        return ObjectInspectorUtils.getStandardObjectInspector(
                arguments[0],
                ObjectInspectorCopyOption.JAVA
        );
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null) {
            return ObjectInspectorUtils.copyToStandardObject(
                    arguments[1].get(),
                    argumentOIs[1],
                    ObjectInspectorCopyOption.JAVA
            );
        } else {
            return ObjectInspectorUtils.copyToStandardObject(
                    arguments[0].get(),
                    argumentOIs[0],
                    ObjectInspectorCopyOption.JAVA
            );
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("ifnull", children);
    }
}
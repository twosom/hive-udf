package com.icloud;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;


@Description(
        name = "strpos",
        value = "_FUNC_(str, substr) - Returns the starting position of the first occurrence of substr in str",
        extended = "Example:\n" +
                   "  SELECT strpos('Hello, World!', 'World') FROM table1;"
)
public class StrPosUDF extends GenericUDF {
    private transient TypeInfo[] typeInfos;
    private transient Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("The function strpos takes exactly two arguments");
        }

        typeInfos = new TypeInfo[arguments.length];
        converters = new Converter[arguments.length];

        for (int i = 0; i < arguments.length; i++) {
            TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[i]);

            if (typeInfo.getCategory() != PRIMITIVE
                || !typeInfo.getTypeName().equalsIgnoreCase("string")) {
                throw new UDFArgumentTypeException(i + 1,
                        "The " + GenericUDFUtils.getOrdinal(i + 1) + " argument must be a string");
            }

            typeInfos[i] = typeInfo;
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }

        String str = converters[0].convert(arguments[0].get()).toString();
        String substr = converters[1].convert(arguments[1].get()).toString();

        int index = str.indexOf(substr);

        return index == -1 ? 0 : index + 1;

    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("strpos", children);
    }

}
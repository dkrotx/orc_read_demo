package ru.mail.go.orc.io.spark;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class OrcRecordSchema {
    public static class Field {
        String name;
        DataType sql_type;
        TypeDescription orc_type;
        boolean nullable;

        private Field(String name, DataType sql_type, TypeDescription orc_type, boolean nullable) {
            this.name = name;
            this.sql_type = sql_type;
            this.orc_type = orc_type;
            this.nullable = nullable;
        }

        static private DataType getSQLDataTypeFor(TypeDescription orc_type) {
            TypeDescription.Category cat = orc_type.getCategory();

            switch(cat) {
                case INT: return DataTypes.IntegerType;
                case LONG: return DataTypes.LongType;
                case STRING: return DataTypes.StringType;
                default:
                    throw new IllegalArgumentException("Unknown type " + cat.getName());
            }
        }

        static public Field MakeField(String name, TypeDescription orc_type, boolean nullable) {
            return new Field(name, getSQLDataTypeFor(orc_type), orc_type, nullable);
        }

        static public Field MakeField(String name, TypeDescription orc_type) {
            return MakeField(name, orc_type, true);
        }
    }

    public static Object OrcWritable2Java(Writable val, TypeDescription.Category cat) {
        switch(cat) {
            case INT:
                return ((IntWritable)val).get();

            case STRING:
            case CHAR:
            case VARCHAR:
                return val.toString();

            case LONG:
                return ((LongWritable)val).get();

            default:
                throw new IllegalArgumentException("Unknown type " + cat.getName());
        }
    }

    private Field[] fields;

    public OrcRecordSchema(Field[] fields) {
        this.fields = fields;
    }

    public final TypeDescription GetORCSchema() {
        TypeDescription schema = TypeDescription.createStruct();
        for (Field desc: fields) {
            schema.addField(desc.name, desc.orc_type);
        }

        return schema;
    }

    public final StructType GetSQLSchema() {
        ArrayList<StructField> sql_fields = new ArrayList<>();
        for (Field desc: fields) {
            sql_fields.add(DataTypes.createStructField(desc.name, desc.sql_type, desc.nullable));
        }

        return DataTypes.createStructType(sql_fields);
    }

    public static Row OrcStruct2SQLRow(OrcStruct st) {
        int i = 0;
        Object[] cols = new Object[st.getNumFields()];
        List<TypeDescription> childs = st.getSchema().getChildren();

        for (TypeDescription fld_descr: childs) {
            cols[i] = OrcWritable2Java(st.getFieldValue(i), fld_descr.getCategory());
            i++;
        }

        return RowFactory.create(cols);
    }
}

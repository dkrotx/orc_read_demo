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

public class RecordSchema {
    class field {
        String name;
        DataType sql_type;
        TypeDescription orc_type;
        boolean nullable;

        field(String name, DataType sql_type, TypeDescription orc_type, boolean nullable) {
            this.name = name;
            this.sql_type = sql_type;
            this.orc_type = orc_type;
            this.nullable = nullable;
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

    ArrayList<field> fields;

    public RecordSchema() {
        fields = new ArrayList<>();
        fields.add(new field("id", DataTypes.IntegerType, TypeDescription.createInt(), true));
        fields.add(new field("name", DataTypes.StringType, TypeDescription.createString(), true));
        fields.add(new field("phone", DataTypes.LongType, TypeDescription.createLong(), true));
    }

    public final TypeDescription GetORCSchema() {
        TypeDescription schema = TypeDescription.createStruct();
        for (field desc: fields) {
            schema.addField(desc.name, desc.orc_type);
        }

        return schema;
    }

    public final StructType GetSQLSchema() {
        ArrayList<StructField> sql_fields = new ArrayList<>();
        for (field desc: fields) {
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

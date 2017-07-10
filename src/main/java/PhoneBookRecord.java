import org.apache.orc.TypeDescription;
import ru.mail.go.orc.io.spark.OrcRecordSchema;


class PhoneBookRecord {
    static OrcRecordSchema MakeOrcRecordSchema() {
        return new OrcRecordSchema(new OrcRecordSchema.Field[] {
                OrcRecordSchema.Field.MakeField("id", TypeDescription.createInt()),
                OrcRecordSchema.Field.MakeField("name", TypeDescription.createString()),
                OrcRecordSchema.Field.MakeField("phone", TypeDescription.createLong()),
        });
    }
}

package ru.mail.go.orc.io.mapreduce;

import java.io.*;
import java.util.Base64;


class SerializationUtils {
    static String SerializeBase64(Object o) throws IOException {
        ByteArrayOutputStream mem = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(mem);
        out.writeObject(o);
        out.close();

        return Base64.getEncoder().encodeToString(mem.toByteArray());
    }

    static Object DeserializeFromBase64(String s) throws IOException, ClassNotFoundException {
        byte[] data = Base64.getDecoder().decode(s);
        ByteArrayInputStream mem = new ByteArrayInputStream(data);
        ObjectInputStream in = new ObjectInputStream(mem);

        Object res = in.readObject();
        in.close();

        return res;
    }
}

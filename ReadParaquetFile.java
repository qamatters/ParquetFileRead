package com;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.Type;

public class ReadParaquetFile {

    public static void printGroup(Group g) {
        int fieldCount = g.getType().getFieldCount();
        for (int field = 0; field < fieldCount; field++) {
            int valueCount = g.getFieldRepetitionCount(field);
            Type fieldType = g.getType().getType(field);
            String fieldName = fieldType.getName();
            for (int index = 0; index < valueCount; index++) {
                if (fieldType.isPrimitive()) {
                    System.out.println(fieldName + " " + g.getValueToString(field, index));
                }
            }
        }
        System.out.println("");
    }
}

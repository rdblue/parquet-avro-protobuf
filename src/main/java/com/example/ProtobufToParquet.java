/*
 * Copyright 2015 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example;

import com.example.Example.ExampleMessage;
import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class ProtobufToParquet {
  public static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

  public static String letter(int ordinal) {
    int start = (ordinal % ALPHABET.length());
    return ALPHABET.substring(start, start + 1);
  }

  public static void writeProtoFile(String path) throws IOException {
    File file = new File(path);
    file.deleteOnExit();

    FileOutputStream out = new FileOutputStream(file);
    try {
      for (int i = 0; i < 1000; i += 1) {
        ExampleMessage message = ExampleMessage.newBuilder()
            .setId(i)
            .addAllStrings(Arrays.asList(
                letter(i), letter(i + 1), letter(i + 2)))
            .build();
        message.writeDelimitedTo(out);
      }
    } finally {
      out.close();
    }
  }

  public static void writeProtobufToParquetAvro(String protoFile,
                                                String parquetFile)
      throws IOException {
    ProtobufData model = ProtobufData.get();

    Schema schema = model.getSchema(ExampleMessage.class);
    System.err.println("Using Avro schema: " + schema.toString(true));

    // use the 3-level structure instead of the 2-level
    // 2-level is the default for forward-compatibility until 2.x
    Configuration conf = new Configuration();
    conf.setBoolean("parquet.avro.write-old-list-structure", false);

    ParquetWriter<ExampleMessage> writer = AvroParquetWriter
        .<ExampleMessage>builder(new Path(parquetFile))
        .withConf(conf)       // conf set to use 3-level lists
        .withDataModel(model) // use the protobuf data model
        .withSchema(schema)   // Avro schema for the protobuf data
        .build();

    FileInputStream protoStream = new FileInputStream(new File(protoFile));
    try {
      ExampleMessage m;
      while ((m = ExampleMessage.parseDelimitedFrom(protoStream)) != null) {
        writer.write(m);
      }
    } finally {
      protoStream.close();
    }

    writer.close();
  }

  public static void main(String[] argv) throws IOException {
    String protoFile = "/tmp/example.proto";
    String parquetFile = "/tmp/example.parquet";
    new File(parquetFile).delete();
    writeProtoFile(protoFile);
    writeProtobufToParquetAvro(protoFile, parquetFile);
  }
}

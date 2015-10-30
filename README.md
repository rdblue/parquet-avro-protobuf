## Converting Protobuf to Parquet via Avro

### Why?

This example shows how to convert a Protobuf file to a Parquet file using
Parquet's Avro object model and Avro's support for protobuf objects. Parquet
has a module to work directly with Protobuf objects, but this isn't always a
good option when writing data for other readers, like Hive.

The reason is that Parquet and Protobuf use the same schema definitions. Both
support required, optional, and repeated data fields and use repeated to encode
arrays.  The mapping from Protobuf to Parquet is always 1-to-1.

Other object models, like Avro, allow arrays to be null or to contain null
elements and have an annotation, [LIST][list-annotation-docs], for encoding
these more complicated structures in Parquet's schema format using extra hidden
layers. More object models use this structure than bare repeated fields, so it
is desirable to use it when converting.

The easiest way to use the complex LIST stucture for protobuf data is to write
using parquet-avro and use Avro's support for Protobuf objects, avro-protobuf.

[list-annotation-docs]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md

### Code

Conversion is done in the [`writeProtobufToParquetAvro`method][write-proto-method].
The first step is to get a handle to Avro's Protobuf object model using
`ProtobufData.get()`.

```Java
ProtobufData model = ProtobufData.get();
```

The Protobuf object model is used to convert the Protobuf data class,
`ExampleMessage`, into an Avro schema.

```Java
Schema schema = model.getSchema(ExampleMessage.class);
```

Then, the Protobuf object model is passed to the builder when creating a
`ParquetWriter`.

```Java
ParquetWriter<ExampleMessage> parquetWriter = AvroParquetWriter
    .<ExampleMessage>builder(new Path(parquetFile))
    .withDataModel(model) // use the protobuf data model
    .withSchema(schema)   // Avro schema for the protobuf data
    .build();
```

Once the parquet-avro writer is configured to use Avro's protobuf support, it
is able to write protobuf messages to the outgoing Parquet file.

```Java
ExampleMessage m;
while ((m = ExampleMessage.parseDelimitedFrom(protoStream)) != null) {
  parquetWriter.write(m);
}
```

[write-proto-method]: https://github.com/rdblue/parquet-avro-protobuf/blob/master/src/main/java/com/example/ProtobufToParquet.java#L59

### Result

After running the example, you will end up with `example.parquet` in temp.
Using `parquet-tools` to view the schema shows the correct 3-level list
representation.

```
message com.example.Example$.ExampleMessage {
  required int64 id;
  required group strings (LIST) {
    repeated group list {
      required binary element (UTF8);
    }
  }
}
```

The original protobuf schema did not include the LIST annotation or the
additional levels needed for compatibility.

```
message ExampleMessage {
  required int64 id = 1;
  repeated string strings = 2;
}
```

The data looks like this when converted to JSON:

```
{"id": 0, "strings": ["a", "b", "c"]}
{"id": 1, "strings": ["b", "c", "d"]}
{"id": 2, "strings": ["c", "d", "e"]}
{"id": 3, "strings": ["d", "e", "f"]}
{"id": 4, "strings": ["e", "f", "g"]}
```

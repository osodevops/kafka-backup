import java.nio.ByteBuffer;
import java.time.Duration;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import java.nio.file.*;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.header.internals.RecordHeaders;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Independent released SDK oracle. No enterprise wire-format code is used. */
public class RegistryOracle {
  private static String decoded(Object value) {
    if(value instanceof com.google.protobuf.Message message) {return "{\"id\":\""+message.getField(message.getDescriptorForType().findFieldByName("id"))+"\"}";}
    if(value instanceof byte[] bytes) return HexFormat.of().formatHex(bytes);
    if(value instanceof ByteBuffer bytes) {byte[] raw=new byte[bytes.remaining()];bytes.get(raw);return HexFormat.of().formatHex(raw);}return value.toString();
  }
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void main(String[] args) throws Exception {
    String action=args[0], provider=args[1], url=args[2], topic=args[3];
    Map<String,Object> cfg=new HashMap<>();
    String framing=System.getenv().getOrDefault("REGISTRY_ORACLE_FRAMING","prefix4");
    boolean confluentSerde=provider.equals("confluent") || framing.equals("compatible");
    String serde;
    String dataType=action.equals("deserialize") ? args[6] : args[4];
    if(confluentSerde) {
      cfg.put("schema.registry.url",url); cfg.put("auto.register.schemas",true);
      if(framing.equals("headers")) {
        cfg.put("value.schema.id.serializer","io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer");
        cfg.put("value.schema.id.deserializer","io.confluent.kafka.serializers.schema.id.DualSchemaIdDeserializer");
      }
      serde="io.confluent.kafka.serializers.KafkaAvro";
    } else {
      cfg.put("apicurio.registry.url",url);
      cfg.put("apicurio.registry.auto-register",true);
      cfg.put("apicurio.registry.headers.enabled",framing.equals("headers"));
      cfg.put("apicurio.registry.headers.value."+System.getenv().getOrDefault("REGISTRY_ORACLE_ID_KIND","globalId")+".name","oracle.value.registry.id");
      cfg.put("apicurio.registry.artifact-resolver-strategy","io.apicurio.registry.serde.strategy.TopicIdStrategy");
      cfg.put("apicurio.registry.use-id",System.getenv().getOrDefault("REGISTRY_ORACLE_ID_KIND","globalId"));
      String idHandler="io.apicurio.registry.serde.Default4ByteIdHandler";
      try {Class.forName(idHandler);} catch(ClassNotFoundException oldSdk) {idHandler="io.apicurio.registry.serde.Legacy4ByteIdHandler";}
      if(!framing.equals("prefix4")) {
        idHandler="io.apicurio.registry.serde.Legacy8ByteIdHandler";
        try {Class.forName(idHandler);} catch(ClassNotFoundException oldSdk) {idHandler="io.apicurio.registry.serde.DefaultIdHandler";}
      }
      cfg.put("apicurio.registry.id-handler",idHandler);
      serde="io.apicurio.registry.serde.avro.AvroKafka";
    }
    if(dataType.equals("json")) {
      serde=confluentSerde ? "io.confluent.kafka.serializers.json.KafkaJsonSchema" : "io.apicurio.registry.serde.jsonschema.JsonSchemaKafka";
      cfg.put("apicurio.registry.artifact.schema.location","oracle-json-schema.json");
      cfg.put("json.value.type",com.fasterxml.jackson.databind.JsonNode.class);
    }
    if(dataType.equals("protobuf")) {
      serde=confluentSerde ? "io.confluent.kafka.serializers.protobuf.KafkaProtobuf" : "io.apicurio.registry.serde.protobuf.ProtobufKafka";
    }
    if(action.equals("serialize") || action.equals("produce")) {
      Serializer serializer=(Serializer)Class.forName(serde+"Serializer").getDeclaredConstructor().newInstance();
      serializer.configure(cfg,false);
      Object data;
      if(args[4].equals("bytes")) {data=ByteBuffer.wrap(new byte[]{0,1,2,127,-1});}
      else if(dataType.equals("json")) {
        ObjectMapper mapper=new ObjectMapper();com.fasterxml.jackson.databind.JsonNode value=mapper.readTree("{\"id\":\"oracle-value\"}");
        data=confluentSerde ? Class.forName("io.confluent.kafka.schemaregistry.json.JsonSchemaUtils").getMethod("envelope",com.fasterxml.jackson.databind.JsonNode.class,com.fasterxml.jackson.databind.JsonNode.class).invoke(null,mapper.readTree(RegistryOracle.class.getResourceAsStream("/oracle-json-schema.json")),value) : value;
      } else if(dataType.equals("protobuf")) {
        com.google.protobuf.DescriptorProtos.DescriptorProto message=com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder().setName("EventMessage").addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder().setName("id").setNumber(1).setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)).build();
        // Apicurio's native serializer looks up the descriptor's unqualified
        // name among top-level messages and cannot produce nested messages.
        // CF and ccompat exercise nested indexes; native AP uses top-level
        // index 1, still ensuring that the [0] shortcut cannot hide bugs.
        boolean nested=Boolean.parseBoolean(System.getenv().getOrDefault("REGISTRY_ORACLE_NESTED_PROTOBUF","false"));
        com.google.protobuf.DescriptorProtos.DescriptorProto rootMessage=nested ? com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder().setName("Outer").addNestedType(message).build() : message;
        com.google.protobuf.Descriptors.FileDescriptor file=com.google.protobuf.Descriptors.FileDescriptor.buildFrom(com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder().setName("oracle.proto").setPackage("oracle").setSyntax("proto3").addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder().setName("Unused")).addMessageType(rootMessage).build(),new com.google.protobuf.Descriptors.FileDescriptor[0]);
        com.google.protobuf.Descriptors.Descriptor selected=nested ? file.getMessageTypes().get(1).getNestedTypes().get(0) : file.getMessageTypes().get(1);
        data=com.google.protobuf.DynamicMessage.newBuilder(selected).setField(selected.findFieldByName("id"),"oracle-value").build();
      }
      else {Schema schema=new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"optional\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"created\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"choice\",\"type\":[\"int\",\"string\"]},{\"name\":\"payload\",\"type\":\"bytes\"}]}");GenericData.Record r=new GenericData.Record(schema);r.put("id","oracle-value");r.put("optional","present");r.put("created",1700000000123L);r.put("choice",42);r.put("payload",ByteBuffer.wrap(new byte[]{0,1,2,127,-1}));data=r;}
      RecordHeaders headers=new RecordHeaders();byte[] value=serializer.serialize(topic,headers,data);
      boolean serializedKeys=Boolean.parseBoolean(System.getenv().getOrDefault("REGISTRY_ORACLE_SERIALIZED_KEYS","false"));
      byte[] key=new byte[]{9,8};
      if(serializedKeys) {
        Map<String,Object> keyCfg=new HashMap<>(cfg);keyCfg.put("apicurio.registry.headers.enabled",false);
        if(provider.equals("apicurio")) {
          String keyIdHandler="io.apicurio.registry.serde.Default4ByteIdHandler";
          try {Class.forName(keyIdHandler);} catch(ClassNotFoundException oldSdk) {keyIdHandler="io.apicurio.registry.serde.Legacy4ByteIdHandler";}
          keyCfg.put("apicurio.registry.id-handler",keyIdHandler);keyCfg.put("apicurio.registry.use-id","globalId");
        }
        Serializer keySerializer=(Serializer)Class.forName(provider.equals("confluent") ? "io.confluent.kafka.serializers.KafkaAvroSerializer" : "io.apicurio.registry.serde.avro.AvroKafkaSerializer").getDeclaredConstructor().newInstance();
        keySerializer.configure(keyCfg,true);key=keySerializer.serialize(topic,new RecordHeaders(),99L);keySerializer.close();
      }
      Map<String,Object> result=new LinkedHashMap<>();result.put("hex",HexFormat.of().formatHex(value));result.put("expected",dataType.equals("json")?"{\"id\":\"oracle-value\"}":decoded(data));
      List<Map<String,String>> hs=new ArrayList<>();headers.forEach(h->hs.add(Map.of("key",h.key(),"hex",HexFormat.of().formatHex(h.value()))));result.put("headers",hs);
      Files.writeString(Path.of(args[5]),new ObjectMapper().writeValueAsString(result));serializer.close();
      if(action.equals("produce")) {
        Map<String,Object> props=new HashMap<>();props.put("bootstrap.servers",args[6]);props.put("compression.type",System.getenv().getOrDefault("REGISTRY_COMPRESSION","none"));props.put("max.block.ms",15000);props.put("delivery.timeout.ms",20000);props.put("request.timeout.ms",15000);props.put("key.serializer",ByteArraySerializer.class);props.put("value.serializer",ByteArraySerializer.class);
        try(KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(props)) {
          for(int i=0;i<4;i++) {RecordHeaders hs2=new RecordHeaders();headers.forEach(h->hs2.add(h.key(),h.value()));hs2.add("oracle",new byte[]{1});hs2.add("oracle",new byte[]{2});hs2.add("nullable",null);
            producer.send(new ProducerRecord<>(topic,null,System.currentTimeMillis(),serializedKeys && i==3?null:key,i==3?null:value,hs2)).get();}
        }
      }
    } else {
      Deserializer deserializer=(Deserializer)Class.forName(serde+"Deserializer").getDeclaredConstructor().newInstance();
      deserializer.configure(cfg,false);
      if(action.equals("consume")) {
        boolean serializedKeys=Boolean.parseBoolean(System.getenv().getOrDefault("REGISTRY_ORACLE_SERIALIZED_KEYS","false"));
        Deserializer keyDeserializer=null;
        if(serializedKeys) {
          Map<String,Object> keyCfg=new HashMap<>(cfg);keyCfg.put("apicurio.registry.headers.enabled",false);keyCfg.put("apicurio.registry.use-id","globalId");
          if(provider.equals("apicurio")) {
            String keyIdHandler="io.apicurio.registry.serde.Default4ByteIdHandler";
            try {Class.forName(keyIdHandler);} catch(ClassNotFoundException oldSdk) {keyIdHandler="io.apicurio.registry.serde.Legacy4ByteIdHandler";}
            keyCfg.put("apicurio.registry.id-handler",keyIdHandler);
          }
          keyDeserializer=(Deserializer)Class.forName(provider.equals("confluent") ? "io.confluent.kafka.serializers.KafkaAvroDeserializer" : "io.apicurio.registry.serde.avro.AvroKafkaDeserializer").getDeclaredConstructor().newInstance();
          keyDeserializer.configure(keyCfg,true);
        }
        Map<String,Object> props=new HashMap<>();props.put("bootstrap.servers",args[6]);props.put("key.deserializer",ByteArrayDeserializer.class);props.put("value.deserializer",ByteArrayDeserializer.class);props.put("group.id","oracle-"+UUID.randomUUID());props.put("auto.offset.reset","earliest");
        int records=0,tombstones=0;List<String> values=new ArrayList<>();long deadline=System.currentTimeMillis()+30000;
        try(KafkaConsumer<byte[],byte[]> consumer=new KafkaConsumer<>(props)) {
          consumer.subscribe(List.of(topic));while(records<4 && System.currentTimeMillis()<deadline) {
            for(ConsumerRecord<byte[],byte[]> record:consumer.poll(Duration.ofMillis(500))) {
              if(serializedKeys) {
                if(record.value()==null) {if(record.key()!=null) throw new AssertionError("Null key changed");}
                else if(!Long.valueOf(99).equals(keyDeserializer.deserialize(topic,record.headers(),record.key()))) throw new AssertionError("Serialized key changed");
              } else if(!Arrays.equals(record.key(),new byte[]{9,8})) throw new AssertionError("Key changed");
              List<Integer> oracleHeaders=new ArrayList<>();record.headers().headers("oracle").forEach(h->oracleHeaders.add((int)h.value()[0]));
              if(!oracleHeaders.equals(List.of(1,2)) || record.headers().lastHeader("nullable").value()!=null) throw new AssertionError("Unrelated headers changed: "+oracleHeaders+"/"+Arrays.toString(record.headers().lastHeader("nullable").value()));
              records++;if(record.value()==null) {tombstones++;continue;}values.add(decoded(deserializer.deserialize(topic,record.headers(),record.value())));
            }
          }
        }
        if(records!=4 || tombstones!=1) throw new AssertionError("Wrong record/tombstone counts: "+records+"/"+tombstones);
        if(keyDeserializer!=null) keyDeserializer.close();
        Files.writeString(Path.of(args[5]),new ObjectMapper().writeValueAsString(values));
      } else {
        byte[] data=HexFormat.of().parseHex(Files.readString(Path.of(args[4])).strip());
        Files.writeString(Path.of(args[5]),decoded(deserializer.deserialize(topic,new RecordHeaders(),data)));
      }
      deserializer.close();
    }
  }
}

/*
 * Copyright 2026 Taner Sener
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

package com.arthenica.kafka.connect.smt;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
public abstract class KafkaSourceId<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String FIELD_NAME_CONFIG = "field.name";
    public static final String DEFAULT_FIELD_NAME = "kafka_source_id";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            FIELD_NAME_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_FIELD_NAME,
            ConfigDef.Importance.MEDIUM,
            "Field name that will receive '<topic>:<partition>:<offset>'."
        );

    private final Map<Schema, Schema> schemaCache = new ConcurrentHashMap<>();
    private String fieldName = DEFAULT_FIELD_NAME;

    @Override
    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(FIELD_NAME_CONFIG);
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new ConfigException(FIELD_NAME_CONFIG, fieldName, "must not be blank");
        }
    }

    @Override
    public R apply(R record) {
        Object value = operatingValue(record);
        if (value == null) {
            return record;
        }

        String sourceId = sourceId(record);
        Schema schema = operatingSchema(record);
        if (schema == null) {
            return applySchemaless(record, value, sourceId);
        }
        return applyWithSchema(record, schema, value, sourceId);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaCache.clear();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private R applySchemaless(R record, Object value, String sourceId) {
        if (!(value instanceof Map<?, ?>)) {
            throw new DataException(
                "KafkaSourceId only supports schemaless Map values or schemaful Struct values; found "
                    + value.getClass().getName()
            );
        }

        Map<Object, Object> updated = new LinkedHashMap<>((Map<?, ?>) value);
        updated.put(fieldName, sourceId);
        return newRecord(record, null, updated);
    }

    private R applyWithSchema(R record, Schema schema, Object value, String sourceId) {
        if (!(value instanceof Struct)) {
            throw new DataException(
                "KafkaSourceId only supports schemaful Struct values; found " + value.getClass().getName()
            );
        }

        Field existingField = schema.field(fieldName);
        if (existingField != null && existingField.schema().type() != Schema.Type.STRING) {
            throw new DataException("Existing field '" + fieldName + "' must be a string field");
        }

        Schema updatedSchema = existingField == null
            ? schemaCache.computeIfAbsent(schema, this::addSourceIdField)
            : schema;

        Struct original = (Struct) value;
        Struct updated = new Struct(updatedSchema);
        for (Field field : schema.fields()) {
            if (!field.name().equals(fieldName)) {
                updated.put(field.name(), original.get(field));
            }
        }
        updated.put(fieldName, sourceId);
        return newRecord(record, updatedSchema, updated);
    }

    private Schema addSourceIdField(Schema schema) {
        SchemaBuilder builder = SchemaBuilder.struct();
        copySchemaBasics(schema, builder);
        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
        return builder.build();
    }

    private static void copySchemaBasics(Schema source, SchemaBuilder builder) {
        if (source.name() != null) {
            builder.name(source.name());
        }
        if (source.version() != null) {
            builder.version(source.version());
        }
        if (source.doc() != null) {
            builder.doc(source.doc());
        }
        if (source.parameters() != null) {
            for (Map.Entry<String, String> parameter : source.parameters().entrySet()) {
                builder.parameter(parameter.getKey(), parameter.getValue());
            }
        }
        if (source.isOptional()) {
            builder.optional();
        }
    }

    private static String sourceId(ConnectRecord<?> record) {
        if (!(record instanceof SinkRecord)) {
            throw new DataException("KafkaSourceId can only be applied to sink records");
        }
        SinkRecord sinkRecord = (SinkRecord) record;
        return sinkRecord.topic() + ":" + sinkRecord.kafkaPartition() + ":" + sinkRecord.kafkaOffset();
    }

    public static class Value<R extends ConnectRecord<R>> extends KafkaSourceId<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp(),
                record.headers()
            );
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends KafkaSourceId<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                updatedSchema,
                updatedValue,
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers()
            );
        }
    }
}

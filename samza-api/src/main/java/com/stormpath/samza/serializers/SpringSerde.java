package com.stormpath.samza.serializers;

import org.apache.samza.serializers.Serde;
import org.springframework.core.serializer.Deserializer;
import org.springframework.core.serializer.Serializer;
import org.springframework.util.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SpringSerde<T> implements Serde<T> {

    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    public SpringSerde(Serializer<T> serializer, Deserializer<T> deserializer) {
        Assert.notNull(serializer, "Serializer cannot be null.");
        Assert.notNull(deserializer, "Deserializer cannot be null.");
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public byte[] toBytes(T object) {
        if (object == null) {
            return null;
        }
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            serializer.serialize(object, stream);
        } catch (IOException e) {
            String msg = "Unable to serialize object [" + object + "]: " + e.getMessage();
            throw new IllegalArgumentException(msg);
        }
        return stream.toByteArray();
    }

    @Override
    public T fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        try {
            return deserializer.deserialize(stream);
        } catch (IOException e) {
            String msg = "Unable to deserialize byte array of length " + bytes.length + ": " + e.getMessage();
            throw new IllegalArgumentException(msg, e);
        }
    }
}

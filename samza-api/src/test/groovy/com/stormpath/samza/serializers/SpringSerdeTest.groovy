package com.stormpath.samza.serializers

import org.junit.Before
import org.junit.Test
import org.springframework.core.serializer.DefaultDeserializer
import org.springframework.core.serializer.DefaultSerializer
import org.springframework.core.serializer.Deserializer
import org.springframework.core.serializer.Serializer;

import static org.junit.Assert.*
import static org.easymock.EasyMock.*

class SpringSerdeTest {

    SpringSerde serde

    @Before
    void setUp() {
        serde = new SpringSerde(new DefaultSerializer(), new DefaultDeserializer())
    }

    @Test
    void testToBytesWithNullObject() {
        assertNull serde.toBytes(null)
    }

    @Test
    void testFromBytesWithNullBytes() {
        assertNull serde.fromBytes(null)
    }

    @Test
    void testFromBytesWithEmptyBytes() {
        assertNull serde.fromBytes(new byte[0])
    }

    @Test
    void testSerializationDeserialization() {
        byte[] bytes = serde.toBytes('foo')
        assertEquals 'foo', serde.fromBytes(bytes)
    }

    @Test(expected = IllegalArgumentException)
    void testToBytesWithIOException() {

        def serializer = createNiceMock(Serializer)
        serde = new SpringSerde(serializer, new DefaultDeserializer())

        expect(serializer.serialize(eq('foo'), isA(OutputStream))).andThrow(new IOException('kapow'))
        replay serializer

        try {
            serde.toBytes('foo')
        } finally {
            verify serializer
        }
    }

    @Test(expected = IllegalArgumentException)
    void testFromBytesWithIOException() {

        def deserializer = createNiceMock(Deserializer)
        serde = new SpringSerde(new DefaultSerializer(), deserializer)

        expect(deserializer.deserialize(isA(InputStream))).andThrow(new IOException('boom!'))
        replay deserializer

        try {
            serde.fromBytes('foo'.getBytes('UTF-8'))
        } finally {
            verify deserializer
        }
    }
}

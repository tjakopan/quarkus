package io.quarkus.redis.datasource.codecs;

import java.nio.charset.StandardCharsets;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;

public class Codecs {

    private Codecs() {
        // Avoid direct instantiation
    }

    @SuppressWarnings("unchecked")
    public static <T> Codec<T> getDefaultCodecFor(Class<T> clazz) {
        if (clazz.equals(Float.class) || clazz.equals(Float.TYPE)) {
            return (Codec<T>) FloatCodec.INSTANCE;
        }
        if (clazz.equals(Double.class) || clazz.equals(Double.TYPE)) {
            return (Codec<T>) DoubleCodec.INSTANCE;
        }
        if (clazz.equals(Byte.class) || clazz.equals(Byte.TYPE)) {
            return (Codec<T>) ByteCodec.INSTANCE;
        }
        if (clazz.equals(Short.class) || clazz.equals(Short.TYPE)) {
            return (Codec<T>) ShortCodec.INSTANCE;
        }
        if (clazz.equals(Integer.class) || clazz.equals(Integer.TYPE)) {
            return (Codec<T>) IntegerCodec.INSTANCE;
        }
        if (clazz.equals(Long.class) || clazz.equals(Long.TYPE)) {
            return (Codec<T>) LongCodec.INSTANCE;
        }
        if (clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE)) {
            return (Codec<T>) BooleanCodec.INSTANCE;
        }
        if (clazz.equals(String.class)) {
            return (Codec<T>) StringCodec.INSTANCE;
        }
        if (clazz.equals(byte[].class)) {
            return (Codec<T>) ByteArrayCodec.INSTANCE;
        }
        // JSON by default
        return new JsonCodec<>(clazz);
    }

    public static class JsonCodec<T> implements Codec<T> {

        private final Class<T> clazz;

        public JsonCodec(Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public byte[] encode(T item) {
            return Json.encodeToBuffer(item).getBytes();
        }

        @Override
        public T decode(byte[] payload) {
            return Json.decodeValue(Buffer.buffer(payload), clazz);
        }
    }

    public static class StringCodec implements Codec<String> {

        public static StringCodec INSTANCE = new StringCodec();

        private StringCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public byte[] encode(String item) {
            return item.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] item) {
            return new String(item, StandardCharsets.UTF_8);
        }
    }

    public static class FloatCodec implements Codec<Float> {
        public static FloatCodec INSTANCE = new FloatCodec();

        private FloatCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public byte[] encode(final Float item) {
            if (item == null) {
                return null;
            }
            return Float.toString(item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Float decode(final byte[] item) {
            if (item == null) {
                return 0.0F;
            }
            return Float.parseFloat(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class DoubleCodec implements Codec<Double> {

        public static DoubleCodec INSTANCE = new DoubleCodec();

        private DoubleCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public byte[] encode(Double item) {
            if (item == null) {
                return null;
            }
            return Double.toString(item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Double decode(byte[] item) {
            if (item == null) {
                return 0.0;
            }
            return Double.parseDouble(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class ByteCodec implements Codec<Byte> {
        public static ByteCodec INSTANCE = new ByteCodec();

        private ByteCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public byte[] encode(Byte item) {
            if (item == null) {
                return null;
            }
            return Byte.toString(item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Byte decode(byte[] item) {
            if (item == null) {
                return 0;
            }
            return Byte.parseByte(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class ShortCodec implements Codec<Short> {
        public static ShortCodec INSTANCE = new ShortCodec();

        private ShortCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public byte[] encode(Short item) {
            if (item == null) {
                return null;
            }
            return Short.toString(item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Short decode(byte[] item) {
            if (item == null) {
                return 0;
            }
            return Short.parseShort(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class IntegerCodec implements Codec<Integer> {

        public static IntegerCodec INSTANCE = new IntegerCodec();

        private IntegerCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public byte[] encode(Integer item) {
            if (item == null) {
                return null;
            }
            return Integer.toString(item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Integer decode(byte[] item) {
            if (item == null) {
                return 0;
            }
            return Integer.parseInt(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class LongCodec implements Codec<Long> {
        public static LongCodec INSTANCE = new LongCodec();

        private LongCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public byte[] encode(Long item) {
            if (item == null) {
                return null;
            }
            return Long.toString(item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Long decode(byte[] item) {
            if (item == null) {
                return 0L;
            }
            return Long.parseLong(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class BooleanCodec implements Codec<Boolean> {
        public static BooleanCodec INSTANCE = new BooleanCodec();

        private BooleanCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public byte[] encode(final Boolean item) {
            if (item == null) {
                return null;
            }
            return Boolean.toString(item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Boolean decode(final byte[] item) {
            if (item == null) {
                return false;
            }
            return Boolean.parseBoolean(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class ByteArrayCodec implements Codec<byte[]> {

        public static ByteArrayCodec INSTANCE = new ByteArrayCodec();

        private ByteArrayCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public byte[] encode(byte[] item) {
            return item;
        }

        @Override
        public byte[] decode(byte[] item) {
            return item;
        }
    }

}

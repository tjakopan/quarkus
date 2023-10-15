package io.quarkus.redis.datasource.codecs;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;

public class Codecs {

    private Codecs() {
        // Avoid direct instantiation
    }

    private static final List<Codec> CODECS = new CopyOnWriteArrayList<>(
            List.of(CharacterCodec.INSTANCE, StringCodec.INSTANCE, FloatCodec.INSTANCE, DoubleCodec.INSTANCE,
                    ByteCodec.INSTANCE, ShortCodec.INSTANCE, IntegerCodec.INSTANCE, LongCodec.INSTANCE,
                    ByteArrayCodec.INSTANCE, BooleanCodec.INSTANCE));

    public static void register(Codec codec) {
        CODECS.add(Objects.requireNonNull(codec));
    }

    public static void register(Stream<Codec> codecs) {
        codecs.forEach(Codecs::register);
    }

    public static Codec getDefaultCodecFor(Type type) {
        for (Codec codec : CODECS) {
            if (codec.canHandle(type)) {
                return codec;
            }
        }

        // JSON by default
        return new JsonCodec(type);
    }

    public static class JsonCodec implements Codec {
        private final TypeReference<?> type;
        private final Class<?> clazz;
        private final ObjectMapper mapper;

        public JsonCodec(Type clazz) {
            if (clazz instanceof Class) {
                this.clazz = (Class<?>) clazz;
                this.type = null;
            } else {
                this.type = new TypeReference<>() {
                    @Override
                    public Type getType() {
                        return clazz;
                    }
                };
                this.clazz = null;
            }
            this.mapper = DatabindCodec.mapper();
        }

        @Override
        public boolean canHandle(Type clazz) {
            throw new UnsupportedOperationException("Should not be called, the JSON codec is the fallback");
        }

        @Override
        public byte[] encode(Object item) {
            return Json.encodeToBuffer(item).getBytes();
        }

        @Override
        public Object decode(byte[] payload) {
            try {
                if (clazz != null) {
                    return Json.decodeValue(Buffer.buffer(payload), clazz);
                } else {
                    return mapper.readValue(payload, type);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class CharacterCodec implements Codec {
        public static CharacterCodec INSTANCE = new CharacterCodec();

        private CharacterCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public boolean canHandle(Type clazz) {
            return clazz.equals(Character.class) || clazz.equals(Character.TYPE);
        }

        @Override
        public byte[] encode(Object item) {
            if (item == null) {
                return null;
            }
            return Character.toString((char) item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Character decode(byte[] item) {
            if (item == null) {
                return Character.MIN_VALUE;
            }
            return new String(item, StandardCharsets.UTF_8).charAt(0);
        }
    }

    public static class StringCodec implements Codec {

        public static StringCodec INSTANCE = new StringCodec();

        private StringCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public boolean canHandle(Type clazz) {
            return clazz.equals(String.class);
        }

        @Override
        public byte[] encode(Object item) {
            return ((String) item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String decode(byte[] item) {
            return new String(item, StandardCharsets.UTF_8);
        }
    }

    public static class FloatCodec implements Codec {
        public static FloatCodec INSTANCE = new FloatCodec();

        private FloatCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public boolean canHandle(Type clazz) {
            return clazz.equals(Float.class) || clazz.equals(Float.TYPE);
        }

        @Override
        public byte[] encode(Object item) {
            if (item == null) {
                return null;
            }
            return Float.toString((float) item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Float decode(byte[] item) {
            if (item == null) {
                return 0F;
            }
            return Float.parseFloat(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class DoubleCodec implements Codec {

        public static DoubleCodec INSTANCE = new DoubleCodec();

        private DoubleCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public boolean canHandle(Type clazz) {
            return clazz.equals(Double.class) || clazz.equals(Double.TYPE);
        }

        @Override
        public byte[] encode(Object item) {
            if (item == null) {
                return null;
            }
            return Double.toString((double) item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Double decode(byte[] item) {
            if (item == null) {
                return 0.0;
            }
            return Double.parseDouble(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class ByteCodec implements Codec {
        public static ByteCodec INSTANCE = new ByteCodec();

        private ByteCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public boolean canHandle(Type clazz) {
            return clazz.equals(Byte.class) || clazz.equals(Byte.TYPE);
        }

        @Override
        public byte[] encode(Object item) {
            if (item == null) {
                return null;
            }
            return Byte.toString((byte) item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Byte decode(byte[] item) {
            if (item == null) {
                return 0;
            }
            return Byte.parseByte(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class ShortCodec implements Codec {
        public static ShortCodec INSTANCE = new ShortCodec();

        private ShortCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public boolean canHandle(Type clazz) {
            return clazz.equals(Short.class) || clazz.equals(Short.TYPE);
        }

        @Override
        public byte[] encode(Object item) {
            if (item == null) {
                return null;
            }
            return Short.toString((short) item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Short decode(byte[] item) {
            if (item == null) {
                return 0;
            }
            return Short.parseShort(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class IntegerCodec implements Codec {

        public static IntegerCodec INSTANCE = new IntegerCodec();

        private IntegerCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public boolean canHandle(Type clazz) {
            return clazz.equals(Integer.class) || clazz.equals(Integer.TYPE);
        }

        @Override
        public byte[] encode(Object item) {
            if (item == null) {
                return null;
            }
            return Integer.toString((int) item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Integer decode(byte[] item) {
            if (item == null) {
                return 0;
            }
            return Integer.parseInt(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class LongCodec implements Codec {
        public static LongCodec INSTANCE = new LongCodec();

        private LongCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public boolean canHandle(Type clazz) {
            return clazz.equals(Long.class) || clazz.equals(Long.TYPE);
        }

        @Override
        public byte[] encode(Object item) {
            if (item == null) {
                return null;
            }
            return Long.toString((long) item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Long decode(byte[] item) {
            if (item == null) {
                return 0L;
            }
            return Long.parseLong(new String(item, StandardCharsets.UTF_8));
        }
    }

    public static class ByteArrayCodec implements Codec {

        public static ByteArrayCodec INSTANCE = new ByteArrayCodec();

        private ByteArrayCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public boolean canHandle(Type clazz) {
            return clazz.equals(byte[].class);
        }

        @Override
        public byte[] encode(Object item) {
            return (byte[]) item;
        }

        @Override
        public byte[] decode(byte[] item) {
            return item;
        }
    }

    public static class BooleanCodec implements Codec {
        public static BooleanCodec INSTANCE = new BooleanCodec();

        private BooleanCodec() {
            // Avoid direct instantiation;
        }

        @Override
        public boolean canHandle(Type clazz) {
            return clazz.equals(Boolean.class) || clazz.equals(Boolean.TYPE);
        }

        @Override
        public byte[] encode(Object item) {
            if (item == null) {
                return null;
            }
            return Boolean.toString((boolean) item).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Boolean decode(byte[] item) {
            if (item == null) {
                return false;
            }
            return Boolean.parseBoolean(new String(item, StandardCharsets.UTF_8));
        }
    }
}

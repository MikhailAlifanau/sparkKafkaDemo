package com.example.sparkkafkademo;

import com.example.UserDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Decoder;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@NoArgsConstructor
public class UserDtoDecoder implements Deserializer<UserDTO> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public UserDTO deserialize(String s, byte[] bytes) {
        return null;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public UserDTO deserialize(String topic, Headers headers, byte[] data) {
        try {
            return mapper.readValue(data, UserDTO.class);
        } catch (Exception e){
            System.out.println("error deserialiing;");
        }
        return null;
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }

}

package br.com.alura.ecommerce;

import org.apache.kafka.common.serialization.Serializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonSerializer<T> implements Serializer<T> {

  private final Gson gson = new GsonBuilder().create();

  @Override
  public byte[] serialize(final String s, final T object) {
    return this.gson.toJson(object).getBytes();
  }

}

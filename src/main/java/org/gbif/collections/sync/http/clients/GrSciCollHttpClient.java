package org.gbif.collections.sync.http.clients;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.Institution;
import org.gbif.api.model.collections.Person;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.vocabulary.Country;
import org.gbif.collections.sync.SyncConfig;
import org.gbif.collections.sync.http.BasicAuthInterceptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import okhttp3.OkHttpClient;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.*;

import static org.gbif.collections.sync.http.SyncCall.syncCall;

/** A lightweight GRSciColl client. */
public class GrSciCollHttpClient {

  private static GrSciCollHttpClient instance;
  private final API api;

  private GrSciCollHttpClient(String grSciCollWsUrl, String user, String password) {
    Objects.requireNonNull(grSciCollWsUrl);

    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(Country.class, new IsoDeserializer());
    mapper.registerModule(module);

    OkHttpClient.Builder okHttpClientBuilder = new OkHttpClient.Builder();

    if (user != null && password != null) {
      okHttpClientBuilder.addInterceptor(new BasicAuthInterceptor(user, password)).build();
    }

    Retrofit retrofit =
        new Retrofit.Builder()
            .client(okHttpClientBuilder.build())
            .baseUrl(grSciCollWsUrl)
            .addConverterFactory(JacksonConverterFactory.create(mapper))
            .build();
    api = retrofit.create(API.class);
  }

  public static GrSciCollHttpClient getInstance(SyncConfig syncConfig) {
    if (instance == null) {
      instance =
          new GrSciCollHttpClient(
              syncConfig.getRegistryWsUrl(),
              syncConfig.getRegistryWsUser(),
              syncConfig.getRegistryWsPassword());
    }

    return instance;
  }

  /** Returns all institutions in GrSciCol. */
  public List<Institution> getInstitutions() {
    List<Institution> result = new ArrayList<>();

    boolean endRecords = false;
    int offset = 0;
    while (!endRecords) {
      PagingResponse<Institution> response = syncCall(api.listInstitutions(1000, offset));
      endRecords = response.isEndOfRecords();
      offset += response.getLimit();
      result.addAll(response.getResults());
    }

    return result;
  }

  public UUID createInstitution(Institution institution) {
    return syncCall(api.createInstitution(institution));
  }

  public void updateInstitution(Institution institution) {
    syncCall(api.updateInstitution(institution.getKey(), institution));
  }

  /** Returns all institutions in GrSciCol. */
  public List<Collection> getCollections() {
    List<Collection> result = new ArrayList<>();

    boolean endRecords = false;
    int offset = 0;
    while (!endRecords) {
      PagingResponse<Collection> response = syncCall(api.listCollections(1000, offset));
      endRecords = response.isEndOfRecords();
      offset += response.getLimit();
      result.addAll(response.getResults());
    }

    return result;
  }

  public UUID createCollection(Collection collection) {
    return syncCall(api.createCollection(collection));
  }

  public void updateCollection(Collection collection) {
    syncCall(api.updateCollection(collection.getKey(), collection));
  }

  /** Returns all persons in GrSciCol. */
  public List<Person> getPersons() {
    List<Person> result = new ArrayList<>();

    boolean endRecords = false;
    int offset = 0;
    while (!endRecords) {
      PagingResponse<Person> response = syncCall(api.listPersons(1000, offset));
      endRecords = response.isEndOfRecords();
      offset += response.getLimit();
      result.addAll(response.getResults());
    }

    return result;
  }

  public UUID createPerson(Person person) {
    return syncCall(api.createPerson(person));
  }

  public void updatePerson(Person person) {
    syncCall(api.updatePerson(person.getKey(), person));
  }

  public void addIdentifierToPerson(UUID personKey, Identifier identifier) {
    syncCall(api.addIdentifierToPerson(personKey, identifier));
  }

  public void addPersonToInstitution(UUID personKey, UUID institutionKey) {
    syncCall(api.addPersonToInstitution(institutionKey, personKey));
  }

  public void removePersonFromInstitution(UUID personKey, UUID institutionKey) {
    syncCall(api.removePersonFromInstitution(institutionKey, personKey));
  }

  public void addPersonToCollection(UUID personKey, UUID collectionKey) {
    syncCall(api.addPersonToCollection(collectionKey, personKey));
  }

  public void removePersonFromCollection(UUID personKey, UUID collectionKey) {
    syncCall(api.removePersonFromCollection(collectionKey, personKey));
  }

  private interface API {
    @GET("institution")
    Call<PagingResponse<Institution>> listInstitutions(
        @Query("limit") int limit, @Query("offset") int offset);

    @POST("institution")
    Call<UUID> createInstitution(@Body Institution institution);

    @PUT("institution/{key}")
    Call<Void> updateInstitution(@Path("key") UUID key, @Body Institution institution);

    @GET("collection")
    Call<PagingResponse<Collection>> listCollections(
        @Query("limit") int limit, @Query("offset") int offset);

    @POST("collection")
    Call<UUID> createCollection(@Body Collection collection);

    @PUT("collection/{key}")
    Call<Void> updateCollection(@Path("key") UUID key, @Body Collection collection);

    @GET("person")
    Call<PagingResponse<Person>> listPersons(
        @Query("limit") int limit, @Query("offset") int offset);

    @POST("person")
    Call<UUID> createPerson(@Body Person person);

    @PUT("person/{key}")
    Call<Void> updatePerson(@Path("key") UUID key, @Body Person person);

    @POST("person/{key}/identifier")
    Call<Void> addIdentifierToPerson(@Path("key") UUID personKey, @Body Identifier identifier);

    @POST("institution/{institutionKey}/contact")
    Call<Void> addPersonToInstitution(
        @Path("institutionKey") UUID institutionKey, @Body UUID personKey);

    @DELETE("institution/{institutionKey}/contact/{personKey}")
    Call<Void> removePersonFromInstitution(
        @Path("institutionKey") UUID institutionKey, @Path("personKey") UUID personKey);

    @POST("collection/{collectionKey}/contact")
    Call<Void> addPersonToCollection(
        @Path("collectionKey") UUID collectionKey, @Body UUID personKey);

    @DELETE("collection/{collectionKey}/contact/{personKey}")
    Call<Void> removePersonFromCollection(
        @Path("collectionKey") UUID collectionKey, @Path("personKey") UUID personKey);
  }

  /** Adapter necessary for retrofit due to versioning. */
  private static class IsoDeserializer extends JsonDeserializer<Country> {
    @Override
    public Country deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      try {
        if (jp != null && jp.getTextLength() > 0) {
          return Country.fromIsoCode(jp.getText());
        } else {
          return Country.UNKNOWN; // none provided
        }
      } catch (Exception e) {
        throw new IOException(
            "Unable to deserialize country from provided value (not an ISO 2 character?): "
                + jp.getText());
      }
    }
  }
}

package org.gbif.collections.sync.clients.http;

import org.gbif.api.model.collections.Collection;
import org.gbif.api.model.collections.*;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.registry.Identifier;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.collections.MasterSourceType;
import org.gbif.collections.sync.config.SyncConfig.RegistryConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.DateDeserializers.DateDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import okhttp3.OkHttpClient;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.*;

import static org.gbif.collections.sync.clients.http.SyncCall.syncCall;

/** A lightweight GRSciColl client. */
public class GrSciCollHttpClient {

  private static final ConcurrentMap<RegistryConfig, GrSciCollHttpClient> clientsMap =
      new ConcurrentHashMap<>();
  private final API api;

  private GrSciCollHttpClient(String grSciCollWsUrl, String user, String password) {
    Objects.requireNonNull(grSciCollWsUrl);

    ObjectMapper mapper =
        new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    SimpleModule module = new SimpleModule();
    module.addDeserializer(Country.class, new CountryIsoDeserializer());
    module.addDeserializer(Date.class, new CustomDateDeserializer());
    mapper.registerModule(module);

    OkHttpClient.Builder okHttpClientBuilder =
        new OkHttpClient.Builder()
            .cache(null)
            .connectTimeout(Duration.ofMinutes(2))
            .readTimeout(Duration.ofMinutes(2));

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

  public static GrSciCollHttpClient getInstance(RegistryConfig registryConfig) {
    GrSciCollHttpClient client = clientsMap.get(registryConfig);
    if (client != null) {
      return client;
    } else {
      GrSciCollHttpClient newClient =
          new GrSciCollHttpClient(
              registryConfig.getWsUrl(),
              registryConfig.getWsUser(),
              registryConfig.getWsPassword());
      clientsMap.put(registryConfig, newClient);
      return newClient;
    }
  }

  /** Returns all institutions in GrSciColl. */
  public List<Institution> getInstitutions() {
    List<Institution> result = new ArrayList<>();

    boolean endRecords = false;
    int offset = 0;
    while (!endRecords) {
      PagingResponse<Institution> response = syncCall(api.listInstitutions(null, 1000, offset));
      endRecords = response.isEndOfRecords();
      offset += response.getLimit();
      result.addAll(response.getResults());
    }

    return result;
  }

  /** Returns all IH institutions in GrSciColl. */
  public List<Institution> getIhInstitutions() {
    List<Institution> result = new ArrayList<>();

    boolean endRecords = false;
    int offset = 0;
    while (!endRecords) {
      PagingResponse<Institution> response =
          syncCall(api.listInstitutions(MasterSourceType.IH, 1000, offset));
      endRecords = response.isEndOfRecords();
      offset += response.getLimit();
      result.addAll(response.getResults());
    }

    return result;
  }

  public Institution getInstitution(UUID key) {
    return syncCall(api.getInstitution(key));
  }

  public UUID createInstitution(Institution institution) {
    return syncCall(api.createInstitution(institution));
  }

  public void updateInstitution(Institution institution) {
    syncCall(api.updateInstitution(institution.getKey(), institution));
  }

  public void addIdentifierToInstitution(UUID institutionKey, Identifier identifier) {
    syncCall(api.addIdentifierToInstitution(institutionKey, identifier));
  }

  public void addMachineTagToInstitution(UUID institutionKey, MachineTag machineTag) {
    syncCall(api.addMachineTagToInstitution(institutionKey, machineTag));
  }

  /** Returns all IH institutions in GrSciCol. */
  public List<Collection> getIhCollections() {
    List<Collection> result = new ArrayList<>();

    boolean endRecords = false;
    int offset = 0;
    while (!endRecords) {
      PagingResponse<Collection> response =
          syncCall(api.listCollections(MasterSourceType.IH, 1000, offset));
      endRecords = response.isEndOfRecords();
      offset += response.getLimit();
      result.addAll(response.getResults());
    }

    return result;
  }

  /** Returns all institutions in GrSciCol. */
  public List<Collection> getCollections() {
    List<Collection> result = new ArrayList<>();

    boolean endRecords = false;
    int offset = 0;
    while (!endRecords) {
      PagingResponse<Collection> response = syncCall(api.listCollections(null, 1000, offset));
      endRecords = response.isEndOfRecords();
      offset += response.getLimit();
      result.addAll(response.getResults());
    }

    return result;
  }

  public Collection getCollection(UUID key) {
    return syncCall(api.getCollection(key));
  }

  public UUID createCollection(Collection collection) {
    return syncCall(api.createCollection(collection));
  }

  public void updateCollection(Collection collection) {
    syncCall(api.updateCollection(collection.getKey(), collection));
  }

  public void addIdentifierToCollection(UUID collectionKey, Identifier identifier) {
    syncCall(api.addIdentifierToCollection(collectionKey, identifier));
  }

  public void addMachineTagToCollection(UUID collectionKey, MachineTag machineTag) {
    syncCall(api.addMachineTagToCollection(collectionKey, machineTag));
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

  public void deletePerson(UUID personKey) {
    syncCall(api.deletePerson(personKey));
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

  public int addContactToInstitution(UUID institutionKey, Contact contact) {
    return syncCall(api.addContactToInstitution(institutionKey, contact));
  }

  public void updateContactInInstitution(UUID institutionKey, Contact contact) {
    syncCall(api.updateContactInInstitution(institutionKey, contact.getKey(), contact));
  }

  public void removeContactFromInstitution(UUID institutionKey, int contactKey) {
    syncCall(api.removeContactFromInstitution(institutionKey, contactKey));
  }

  public int addContactToCollection(UUID institutionKey, Contact contact) {
    return syncCall(api.addContactToCollection(institutionKey, contact));
  }

  public void updateContactInCollection(UUID institutionKey, Contact contact) {
    syncCall(api.updateContactInCollection(institutionKey, contact.getKey(), contact));
  }

  public void removeContactFromCollection(UUID institutionKey, int contactKey) {
    syncCall(api.removeContactFromCollection(institutionKey, contactKey));
  }

  public Person getPerson(UUID key) {
    return syncCall(api.getPerson(key));
  }

  public void addMasterSourceMetadataToInstitution(
      UUID institutionKey, MasterSourceMetadata metadata) {
    syncCall(api.addMasterSourceMetadataToInstitution(institutionKey, metadata));
  }

  public void addMasterSourceMetadataToCollection(
      UUID collectionKey, MasterSourceMetadata metadata) {
    syncCall(api.addMasterSourceMetadataToCollection(collectionKey, metadata));
  }

  private interface API {
    @GET("institution")
    Call<PagingResponse<Institution>> listInstitutions(
        @Query("masterSourceType") MasterSourceType masterSourceType,
        @Query("limit") int limit,
        @Query("offset") int offset);

    @GET("institution/{key}")
    Call<Institution> getInstitution(@Path("key") UUID key);

    @POST("institution")
    Call<UUID> createInstitution(@Body Institution institution);

    @PUT("institution/{key}")
    Call<Void> updateInstitution(@Path("key") UUID key, @Body Institution institution);

    @POST("institution/{key}/identifier")
    Call<Void> addIdentifierToInstitution(
        @Path("key") UUID institutionKey, @Body Identifier identifier);

    @POST("institution/{key}/machineTag")
    Call<Void> addMachineTagToInstitution(
        @Path("key") UUID institutionKey, @Body MachineTag machineTag);

    @GET("collection")
    Call<PagingResponse<Collection>> listCollections(
        @Query("masterSourceType") MasterSourceType masterSourceType,
        @Query("limit") int limit,
        @Query("offset") int offset);

    @GET("collection/{key}")
    Call<Collection> getCollection(@Path("key") UUID key);

    @POST("collection")
    Call<UUID> createCollection(@Body Collection collection);

    @PUT("collection/{key}")
    Call<Void> updateCollection(@Path("key") UUID key, @Body Collection collection);

    @POST("collection/{key}/identifier")
    Call<Void> addIdentifierToCollection(
        @Path("key") UUID collectionKey, @Body Identifier identifier);

    @POST("collection/{key}/machineTag")
    Call<Void> addMachineTagToCollection(
        @Path("key") UUID collectionKey, @Body MachineTag machineTag);

    @GET("person")
    Call<PagingResponse<Person>> listPersons(
        @Query("limit") int limit, @Query("offset") int offset);

    @GET("person/{key}")
    Call<Person> getPerson(@Path("key") UUID key);

    @POST("person")
    Call<UUID> createPerson(@Body Person person);

    @PUT("person/{key}")
    Call<Void> updatePerson(@Path("key") UUID key, @Body Person person);

    @DELETE("person/{key}")
    Call<Void> deletePerson(@Path("key") UUID key);

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

    @POST("institution/{institutionKey}/contactPerson")
    Call<Integer> addContactToInstitution(
        @Path("institutionKey") UUID institutionKey, @Body Contact contact);

    @PUT("institution/{institutionKey}/contactPerson/{contactKey}")
    Call<Void> updateContactInInstitution(
        @Path("institutionKey") UUID institutionKey,
        @Path("contactKey") int contactKey,
        @Body Contact contact);

    @DELETE("institution/{institutionKey}/contactPerson/{contactKey}")
    Call<Void> removeContactFromInstitution(
        @Path("institutionKey") UUID institutionKey, @Path("contactKey") int contactKey);

    @POST("collection/{collectionKey}/contactPerson")
    Call<Integer> addContactToCollection(
        @Path("collectionKey") UUID collectionKey, @Body Contact contact);

    @PUT("collection/{collectionKey}/contactPerson/{contactKey}")
    Call<Void> updateContactInCollection(
        @Path("collectionKey") UUID collectionKey,
        @Path("contactKey") int contactKey,
        @Body Contact contact);

    @DELETE("collection/{collectionKey}/contactPerson/{contactKey}")
    Call<Void> removeContactFromCollection(
        @Path("collectionKey") UUID collectionKey, @Path("contactKey") int contactKey);

    @POST("institution/{institutionKey}/masterSourceMetadata")
    Call<Void> addMasterSourceMetadataToInstitution(
        @Path("institutionKey") UUID institutionKey,
        @Body MasterSourceMetadata masterSourceMetadata);

    @POST("collection/{collectionKey}/masterSourceMetadata")
    Call<Void> addMasterSourceMetadataToCollection(
        @Path("collectionKey") UUID collectionKey, @Body MasterSourceMetadata masterSourceMetadata);
  }

  /** Adapter necessary for retrofit due to versioning. */
  private static class CountryIsoDeserializer extends JsonDeserializer<Country> {
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

  private static class CustomDateDeserializer extends DateDeserializer {

    @Override
    public Date deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      try {
        return super.deserialize(p, ctxt);
      } catch (Exception ex) {
        return null;
      }
    }
  }
}

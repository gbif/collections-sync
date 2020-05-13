package org.gbif.collections.sync.http.clients;

import org.gbif.collections.sync.http.SyncCall;
import org.gbif.collections.sync.ih.model.IHInstitution;
import org.gbif.collections.sync.ih.model.IHMetadata;
import org.gbif.collections.sync.ih.model.IHStaff;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import okhttp3.OkHttpClient;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.GET;
import retrofit2.http.Query;

/** Lightweight IndexHerbariorum client. */
public class IHHttpClient {

  private static IHHttpClient instance;
  private final API api;

  private IHHttpClient(String ihWsUrl) {
    Objects.requireNonNull(ihWsUrl);

    ObjectMapper mapper =
        new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    OkHttpClient.Builder okHttpClientBuilder =
        new OkHttpClient.Builder()
            .cache(null)
            .connectTimeout(Duration.ofMinutes(2))
            .readTimeout(Duration.ofMinutes(2));

    Retrofit retrofit =
        new Retrofit.Builder()
            .client(okHttpClientBuilder.build())
            .baseUrl(ihWsUrl)
            .addConverterFactory(JacksonConverterFactory.create(mapper))
            .build();
    api = retrofit.create(API.class);
  }

  public static IHHttpClient getInstance(String ihWsUrl) {
    if (instance == null) {
      instance = new IHHttpClient(ihWsUrl);
    }

    return instance;
  }

  public List<IHInstitution> getInstitutions() {
    return SyncCall.syncCall(api.listInstitutions()).getData();
  }

  public List<IHStaff> getStaffByInstitution(String institutionCode) {
    return SyncCall.syncCall(api.listStaffByCode(institutionCode)).getData();
  }

  public List<IHStaff> getStaff() {
    return SyncCall.syncCall(api.listStaff()).getData();
  }

  public List<String> getCountries() {
    return SyncCall.syncCall(api.listCountries()).getData();
  }

  private interface API {
    @GET("institutions")
    Call<InstitutionWrapper> listInstitutions();

    @GET("staff/search")
    Call<StaffWrapper> listStaffByCode(@Query("code") String institutionCode);

    @GET("staff")
    Call<StaffWrapper> listStaff();

    @GET("countries")
    Call<CountryWrapper> listCountries();
  }

  @Data
  private static class InstitutionWrapper {
    private IHMetadata meta;
    private List<IHInstitution> data;
  }

  @Data
  private static class StaffWrapper {
    private IHMetadata meta;
    private List<IHStaff> data = new ArrayList<>();
  }

  @Data
  private static class CountryWrapper {
    private IHMetadata meta;
    private List<String> data = new ArrayList<>();
  }
}

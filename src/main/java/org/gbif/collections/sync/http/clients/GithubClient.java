package org.gbif.collections.sync.http.clients;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.gbif.collections.sync.config.SyncConfig.NotificationConfig;
import org.gbif.collections.sync.http.BasicAuthInterceptor;
import org.gbif.collections.sync.notification.Issue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import okhttp3.OkHttpClient;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.PATCH;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;

import static org.gbif.collections.sync.http.SyncCall.syncCall;
import static org.gbif.collections.sync.notification.IHIssueFactory.IH_SYNC_LABEL;

/** Lightweight client for the Github API. */
public class GithubClient {

  private static GithubClient instance;
  private final API api;
  private final Set<String> assignees;

  private GithubClient(String githubWsUrl, String user, String password, Set<String> assignees) {
    Objects.requireNonNull(githubWsUrl);
    Objects.requireNonNull(user);
    Objects.requireNonNull(password);

    ObjectMapper mapper =
        new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    OkHttpClient okHttpClient =
        new OkHttpClient.Builder()
            .cache(null)
            .addInterceptor(new BasicAuthInterceptor(user, password))
            .build();

    Retrofit retrofit =
        new Retrofit.Builder()
            .client(okHttpClient)
            .baseUrl(githubWsUrl)
            .addConverterFactory(JacksonConverterFactory.create(mapper))
            .build();
    api = retrofit.create(API.class);
    this.assignees = assignees;
  }

  public static GithubClient getInstance(NotificationConfig notificationConfig) {
    if (instance == null) {
      Objects.requireNonNull(notificationConfig);
      instance =
          new GithubClient(
              notificationConfig.getGithubWsUrl(),
              notificationConfig.getGithubUser(),
              notificationConfig.getGithubPassword(),
              notificationConfig.getGhIssuesAssignees());
    }

    return instance;
  }

  public void createIssue(Issue issue) {
    if (assignees != null && !assignees.isEmpty()) {
      // we use the assignees from the config if they were set
      issue.setAssignees(assignees);
    }

    syncCall(api.createIssue(issue));
  }

  public Optional<Issue> findIssueWithSameTitle(String title) {
    int page = 1;
    int perPage = 100;
    String state = "open";

    // first call
    List<IssueResult> issues =
        syncCall(api.listIssues(Collections.singletonList(IH_SYNC_LABEL), state, page, perPage));

    // paginate over issues till we find a match
    while (!issues.isEmpty()) {
      Optional<IssueResult> match =
          issues.stream().filter(i -> title.equalsIgnoreCase(i.getTitle())).findFirst();
      if (match.isPresent()) {
        return match.map(
            ir ->
                Issue.builder()
                    .number(ir.getNumber())
                    .title(ir.getTitle())
                    .labels(
                        ir.getLabels().stream()
                            .map(IssueResult.Label::getName)
                            .collect(Collectors.toSet()))
                    .assignees(
                        ir.getAssignees().stream()
                            .map(IssueResult.Assignee::getLogin)
                            .collect(Collectors.toSet()))
                    .build());
      }

      issues =
          syncCall(
              api.listIssues(Collections.singletonList(IH_SYNC_LABEL), state, page++, perPage));
    }

    return Optional.empty();
  }

  public void updateIssue(Issue issue) {
    syncCall(api.updateIssue(issue.getNumber(), issue));
  }

  private interface API {
    @POST("issues")
    Call<Void> createIssue(@Body Issue issue);

    @GET("issues")
    Call<List<IssueResult>> listIssues(
        @Query("labels") List<String> labels,
        @Query("state") String state,
        @Query("page") int page,
        @Query("per_page") int perPage);

    @PATCH("issues/{id}")
    Call<Void> updateIssue(@Path("id") long id, @Body Issue issue);
  }

  @Data
  private static class IssueResult {
    private long number;
    private String title;
    private List<Label> labels;
    private List<Assignee> assignees;

    @Data
    private static class Label {
      private String name;
    }

    @Data
    private static class Assignee {
      private String login;
    }
  }
}

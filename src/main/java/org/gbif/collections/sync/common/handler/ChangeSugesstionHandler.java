package org.gbif.collections.sync.common.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.gbif.api.model.collections.suggestions.CollectionChangeSuggestion;
import org.gbif.collections.sync.SyncResult.FailedAction;
import org.gbif.collections.sync.clients.http.GrSciCollHttpClient;
import org.gbif.collections.sync.clients.proxy.CallExecutor;

public class ChangeSugesstionHandler {

  protected final CallExecutor callExecutor;
  protected GrSciCollHttpClient grSciCollHttpClient;

  private ChangeSugesstionHandler(CallExecutor callExecutor, GrSciCollHttpClient grSciCollHttpClient) {
    this.callExecutor = callExecutor;
    this.grSciCollHttpClient = grSciCollHttpClient;
  }

  public static ChangeSugesstionHandler create(
      CallExecutor callExecutor, GrSciCollHttpClient grSciCollHttpClient) {
    return new ChangeSugesstionHandler(callExecutor, grSciCollHttpClient);
  }

  public List<CollectionChangeSuggestion> getCall(String ihIdentifier) {
    return callExecutor.executeAndReturnOrAddFail(
        () -> grSciCollHttpClient.getChangeSuggestionsByIhIdentifier(ihIdentifier),
        exceptionHandler(ihIdentifier, "Failed to get change suggestion by ihIdentifier"),
        new ArrayList<>());
  }

  public int createCollectionChangeSuggestion(CollectionChangeSuggestion changeSuggestion) {
    return callExecutor.executeAndReturnOrAddFail(
        () -> grSciCollHttpClient.createCollectionChangeSuggestion(changeSuggestion),
        exceptionHandler(changeSuggestion, "Failed to create change suggestion"),
        1);
  }

  private Function<Throwable, FailedAction> exceptionHandler(Object obj, String msg) {
    return e -> new FailedAction(obj, msg + ": " + e.getMessage());
  }

}

package org.gbif.collections.sync.common.handler;

import java.util.List;
import org.gbif.api.model.collections.suggestions.CollectionChangeSuggestion;
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
    return grSciCollHttpClient.getChangeSuggestionsByIhIdentifier(ihIdentifier);
  }

  public int createCollectionChangeSuggestion(CollectionChangeSuggestion changeSuggestion) {
    return grSciCollHttpClient.createCollectionChangeSuggestion(changeSuggestion);
  }

}

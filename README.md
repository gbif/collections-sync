[![Build Status](https://builds.gbif.org/job/collections-sync/job/master/badge/icon)](https://builds.gbif.org/job/collections-sync/job/master/)

# Registry Collections Synchronisation

Provides synchronisation utilities for key repositories such as Index Herbariorum.

## How to build the project
Just run the following Maven command:
```
mvn clean package
```

## Code style
The code formatting follows the [Google Java format](https://github.com/google/google-java-format).

## IH Sync

Sync between IH and the GrSciColl collections of the GBIF registry. IH is the master source of data and overwrites the equivalent fields in the GBIF registry. The equivalent entites of IH in the GBIF registry have a IH_IRN identifier with the corresponding IH IRN. For each IH institution we try to find an institution or collection in GBIF that has a IH_IRN identifier with that IRN. We can find the following cases:
* Matches with 1 collection
    - Update collection and associated staff
* Matches with 1 institution
    - Update institution and associated staff
    - Create new collection with the same staff and link it to the institution
* Matches with 1 collection and 1 institution (and the collection belongs to the institution)
    - Update institution and associated staff
    - Update collection and associated staff
* There are no matches
    - Create new institution
    - Create new collection and link it to the institution
    - Sync the staff to both entities
* Conflict
    - More than 1 institution or 1 collection matched
    - 1 institution and 1 collection matched but the collection doesn't belong to the institution


### How to run a IH sync
To run a IH sync just use this command:

```
java -jar collections-sync.jar -c gbif-configuration/collections-sync/uat/config.yaml
```

The configuration file has to provide all the [config fields](https://github.com/gbif/collections-sync/blob/master/src/main/java/org/gbif/collections/sync/SyncConfig.java).

Some [fields](https://github.com/gbif/collections-sync/blob/aa3ea0168f46f13bf31a522ccff5f3366a7dc122/src/main/java/org/gbif/collections/sync/CliSyncApp.java#L41) can be overriden in the command line by using the following options:

* --config or -c to specify the config file path
* --dryRun or -dr to specify the dryRun option. This just runs the sync process but doesn't update anything in the registry.
* --sendNotifications or -n to specify if we want to create Github issues when we find conflicts or invalid data during the sync.
* --githubAssignees or -ga to specify the github assignees for the issues created

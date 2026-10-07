# CHANGELOG

## Emoji Cheatsheet
- :pencil2: doc updates
- :bug: when fixing a bug
- :rocket: when making general improvements
- :white_check_mark: when adding tests
- :arrow_up: when upgrading dependencies
- :tada: when adding new features

## Version History

### Pending

### v5.16.0

- :tada: Add an optional Outgoing flow (`event:create`) that creates a CalTopo Map in a Team Account for every CoreEvent, titled after the Event callsign with a Marker carrying its remarks & location
- :tada: Add `TeamId` (the Team the Maps are created in - distinct from the Service Account ID), `MapMode`, `MapSharing`, `MapLayers` & `MarkerColor` Outgoing environment variables
- :rocket: The created Map ID is filed under the `caltopo` external ID of the CoreEvent (CloudTAK 13.106+) which is what prevents a redelivered message creating a second Map, and the Map URL is added to the Event Links unless the Map is `PRIVATE`
- :rocket: Add `CalTopo.createMap` & `CalTopo.signature` for signed form encoded `POST` requests
- :rocket: Non-2xx CalTopo responses include the response body in the error so a 403 says why
- :white_check_mark: Add unit tests for Map creation and the Outgoing flow

### v5.15.1

- :bug: Fix TS build error

### v5.15.0

- :tada: Add optional `SinceDelta` to the Team Account source to request only locations updated within the last N seconds via the `since` parameter
- :rocket: Move CalTopo API client, signing, and response schemas into `lib/caltopo.ts`
- :white_check_mark: Add unit tests for the API client, feature transforms, and `since` handling

### v5.14.1

- :bug: Request Shared Locations with a world `bbox`, CalTopo returns an empty `200` response for a `null` bbox resulting in `Unexpected end of JSON input`

### v5.14.0

- :bug: Include the `json` payload in the Team Account request signature, CalTopo rejected the Shared Locations request with an empty body resulting in `Unexpected end of JSON input`
- :rocket: Throw a readable error when CalTopo returns an empty or non-2xx response

### v5.13.0

- :rocket: Team Account mode now surfaces only the live Shared Locations of devices reporting to the team, via the signed `/api/v1/geodata/locations` endpoint, as `a-f-G-U-C` points with stale times from the CalTopo TTL
- :rocket: Remove the Team Account map import and its optional `MapId`, map objects are ingested with Single Map mode

### v5.12.0

- :tada: Add Team Account support, choosing between a single Map ID and a signed Team Account request via a `Source` union in the layer environment (Team mode takes an Account ID, Credential ID & Credential Secret)
- :rocket: Import every Collaborative Map on a Team Account when no Map ID is given, using the map title as the feature path
- :rocket: Keep accepting the legacy top level `ShareId` environment for existing layers

### v5.11.0

- :rocket: Add Capabilities document
- :arrow_up: Update GH Actions

### v5.10.0

- :arrow_up: Update Core Dependencies

### v5.9.0

- :arrow_up: Update Core Dependencies

### v5.8.0

- :arrow_up: Update Core Dependencies

### v5.7.0

- :arrow_up: Update Core Dependencies

### v5.6.0

- :arrow_up: Update Core Dependencies

### v5.5.0

- :arrow_up: Update Core Dependencies

### v5.4.0

- :arrow_up: Update Core Deps

### v5.3.0

- :arrow_up: Update Core Deps

### v5.2.0

- :arrow_up: Update Core Deps

### v5.1.0

- :rocket: Reduce log verbosity without DEBUG: true set

### v5.0.0

- :tada: Update to `CloudTAK@v6`

### v4.2.1

- :bug: Remove webhook type

### v4.2.0

- :tada: Add Capbilities API

### v4.1.0

- :rocket: Add support for CalTopo Folders

### v4.0.0

- :rocket: One CalTopo Map per Layer (simplifies end-user UI)
- :rocket: Strongly type input features

### v3.8.0

- :arrow_up: Update Core Deps

### v3.7.0

- :rocket: Use 0-1 opacity values

### v3.6.0

- :rocket: Update to use new typed environment helper

### v3.5.0

- :rocket: Parse and submit marker colour

### v3.4.0

- :rocket: Parse and submit marker colour

### v3.3.0

- :rocket: Update to support point type

### v3.2.1

- :bug: Fix opacity parsing

### v3.2.0

- :rocket: Support for styling properties

### v3.1.3

- :arrow_up: Use latest base

### v3.1.2

- :arrow_up: Use latest base

### v3.1.1

- :bug: Use instance method for schema

### v3.1.0

- :rocket: Update to latest ETL Base & use new default fns

### v3.0.0

- :rocket: Update to latest token strategy

### v2.3.1

- :bug: Use coordsEach to ensure all coords are max 3 elements

### v2.3.0

- :rocket: Set callsign and remarks to better defaults

### v2.2.1

- :bug: Filter without undefines in feature array

### v2.2.0

- :bug: Fix SARTopo non-geometry features

### v2.1.2

- :bug: Fix Point Generation

### v2.1.1

- :bug: Fix path in which features are extracted

### v2.1.0

- :rocket: Improved display in ETL
- :tada: Add Human Readable Name Field

### v2.0.1

- :rocket: Fix Tagging

### v1.0.0

- :rocket: Initial Approach


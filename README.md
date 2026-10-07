<h1 align='center'>ETL-CalTopo</h1>

<p align='center'>Bring CALTopo Maps into the TAK System</p>

## Modes

The Layer supports two independent data flows, the Outgoing flow is off unless configured.

- **Incoming** (Schedule) - Ingests the objects of a single shared CalTopo Map, or the live Shared Locations of a CalTopo Team Account
- **Outgoing** (`event:create`) - Creates a new CalTopo Map in a Team Account every time a CoreEvent is created and shared with the Layer Connection

## Outgoing - CoreEvent to CalTopo Map

Enable the Outgoing flow of the Layer and subscribe it to `event:create`. The Service Account requires at minimum `Update`
permissions on the Team (see Team Access Token below) and the Layer requires the `event:read` & `event:update` permissions.
Each created Map is titled after the CoreEvent callsign and contains a single Marker at the Event location whose description
carries the Event remarks and human readable location.

The Map ID is filed under the `caltopo` external ID of the CoreEvent, so an Event that already has one (ie: a redelivered
message) never gets a second Map. Unless the Map is `PRIVATE`, its URL is also added to the Links of the CoreEvent.

| Variable           | Default               | Description                                                                                 |
| ------------------ | --------------------- | ------------------------------------------------------------------------------------------- |
| `AccountId`        |                       | CalTopo Team Account ID the Maps are created in (`https://caltopo.com/group/{AccountId}/admin/details`) |
| `CredentialId`     |                       | Service Account Credential ID                                                               |
| `CredentialSecret` |                       | Service Account Credential Secret                                                           |
| `MapMode`          | `sar`                 | `sar` (Search & Rescue) or `cal` (Recreational)                                             |
| `MapSharing`       | `SECRET`              | `PRIVATE` (creator only), `SECRET` (secret URL), `URL` (public URL) or `PUBLIC`             |
| `MapLayers`        | `[{ "layer": "mbt" }]` | Active base layers - ie: `mbt` (MapBuilder Topo), `mbh` (MapBuilder Hybrid), `imagery`     |
| `MarkerColor`      | `FF0000`              | Hex colour of the Marker placed at the Event location                                       |
| `DEBUG`            | `false`               | Print results in logs                                                                       |


## Team Access Token

The `Team Account` mode requires a Service Account Credential ID and Secret from CalTopo.
You must be an admin of the CalTopo Team to create an access token.

1. Login to [CalTopo](https://caltopo.com) and click your username in the top bar

    <p align='center'><img src='docs/team-token-1.png' alt='Click your username'/></p>

2. In the popup, under Team Membership, click `Administer` for the team account you wish to configure

    <p align='center'><img src='docs/team-token-2.png' alt='Click Administer'/></p>

3. On the Team page, click the `Details` tab

    <p align='center'><img src='docs/team-token-3.png' alt='Click the Details tab'/></p>

4. Under Service Accounts, click `Create Service Account` and create a new Service Account with at minimum `Read` permissions
   (`Update` permissions are required for the Outgoing flow to create Maps)

    <p align='center'><img src='docs/team-token-4.png' alt='Create a Service Account'/></p>

## Development

DFPC provided Lambda ETLs are currently all written in [NodeJS](https://nodejs.org/en) through the use of a AWS Lambda optimized
Docker container. Documentation for the Dockerfile can be found in the [AWS Help Center](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html)

```sh
npm install
```

Add a .env file in the root directory that gives the ETL script the necessary variables to communicate with a local ETL server.
When the ETL is deployed the `ETL_API` and `ETL_LAYER` variables will be provided by the Lambda Environment

```json
{
    "ETL_API": "http://localhost:5001",
    "ETL_LAYER": "19"
}
```

To run the task, ensure the local [CloudTAK](https://github.com/dfpc-coe/CloudTAK/) server is running and then run with typescript runtime
or build to JS and run natively with node

```
ts-node task.ts
```

```
npm run build
cp .env dist/
node dist/task.js
```

### Deployment

Deployment into the CloudTAK environment for configuration is done via automatic releases to the DFPC AWS environment.

Github actions will build and push docker releases on every version tag which can then be automatically configured via the 
CloudTAK API.

Non-DFPC users will need to setup their own docker => ECS build system via something like Github Actions or AWS Codebuild.


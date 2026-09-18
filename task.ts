import { Type, TSchema, Static } from '@sinclair/typebox';
import { Feature } from '@tak-ps/node-cot';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import { fetch } from '@tak-ps/etl';
import { coordEach } from '@turf/meta';
import { createHmac } from 'node:crypto';

const MapSource = Type.Object({
    Mode: Type.Literal('Map'),
    MapId: Type.String({
        description: 'CalTopo Map or Share ID',
    }),
}, { title: 'Single Map' });

const TeamSource = Type.Object({
    Mode: Type.Literal('Team'),
    AccountId: Type.String({
        description: 'CalTopo Team Account ID',
    }),
    CredentialId: Type.String({
        description: 'Service Account Credential ID',
    }),
    CredentialSecret: Type.String({
        description: 'Service Account Credential Secret',
    }),
}, {
    title: 'Team Account',
    description: 'Surfaces the live Shared Locations of devices reporting to the Team Account',
});

const Env = Type.Object({
    Source: Type.Union([MapSource, TeamSource]),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
});

// Layers created before Team Account support store the Share ID at the top level
const LegacyEnv = Type.Object({
    ShareId: Type.String(),
    'DEBUG': Type.Boolean({ default: false })
});

const CALTOPO = 'https://caltopo.com/';
// Matches the CalTopo Shared Locations overlay default expiry
const LOCATION_TTL = 30 * 60 * 1000;

/**
 * Append the CalTopo service account signature query parameters
 * Signing string is "{method} {path}\n{expires}\n{payload}" HMAC-SHA256 with the base64 decoded secret
 */
function sign(method: string, url: URL, creds: Static<typeof TeamSource>, payload = ''): URL {
    const expires = Date.now() + 120 * 1000;
    const data = `${method} ${url.pathname}\n${expires}\n${payload}`;

    const signature = createHmac('sha256', Buffer.from(creds.CredentialSecret, 'base64'))
        .update(data)
        .digest('base64');

    url.searchParams.set('id', creds.CredentialId);
    url.searchParams.set('expires', String(expires));
    url.searchParams.set('signature', signature);

    return url;
}

/**
 * CalTopo responds to a rejected signature or unknown ID with an empty body, often with a 200 status
 * Surface that as a readable error instead of a JSON parse failure
 */
function assertBody(res: { ok: boolean, status: number, headers: Headers }): void {
    if (!res.ok) {
        throw new Error(`CalTopo responded with HTTP ${res.status}`);
    } else if (res.headers.get('content-length') === '0') {
        throw new Error('CalTopo returned an empty response - check the ID & Credentials and that the credential has access');
    }
}

const LocationOutput = Type.Object({
    title: Type.Optional(Type.String()),
    device: Type.Optional(Type.String()),
    sharedWith: Type.Optional(Type.String()),
    type: Type.Optional(Type.String()),
    updated: Type.Optional(Type.Number()),
    ttl: Type.Optional(Type.Number()),
    stroke: Type.Optional(Type.String()),
    'aircraft:heading': Type.Optional(Type.Number()),
});

const Output = Type.Object({
    title: Type.String(),
    description: Type.Optional(Type.String()),
    class: Type.String(),
    creator: Type.String(),
    updated: Type.Number(),

    'marker-symbol': Type.Optional(Type.Union([Type.String(), Type.Null()])),
    'marker-rotation': Type.Optional(Type.Union([Type.String(), Type.Null()])),
    'marker-color': Type.Optional(Type.Union([Type.String(), Type.Null()])),
    'marker-size': Type.Optional(Type.Union([Type.String(), Type.Null()])),

    stroke: Type.Optional(Type.Union([Type.String(), Type.Null()])),
    'stroke-opacity': Type.Optional(Type.Union([Type.Number(), Type.Null()])),
    'stroke-width': Type.Optional(Type.Union([Type.Number(), Type.Null()])),
    pattern: Type.Optional(Type.Union([Type.String(), Type.Null()])),

    fill: Type.Optional(Type.Union([Type.String(), Type.Null()])),
    'fill-opacity': Type.Optional(Type.Union([Type.Number(), Type.Null()])),

    folderId: Type.Optional(Type.Union([Type.String(), Type.Null()])),
    visible: Type.Optional(Type.Boolean()),
    labelVisible: Type.Optional(Type.Boolean()),
});

export default class Task extends ETL {
    static name = 'etl-caltopo';
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Env;
            } else {
                return Output;
            }
        } else {
            return Type.Object({});
        }
    }

    async control(): Promise<void> {
        const raw = await this.env(Type.Union([Env, LegacyEnv]));

        const env: Static<typeof Env> = 'ShareId' in raw
            ? { Source: { Mode: 'Map', MapId: raw.ShareId }, DEBUG: raw.DEBUG }
            : raw;

        const features = env.Source.Mode === 'Map'
            ? await this.fetchMap(new URL(`/api/v1/map/${env.Source.MapId}/since/-500`, CALTOPO), env.DEBUG)
            : await this.fetchLocations(env.Source, env.DEBUG);

        await this.submit({
            type: 'FeatureCollection',
            features: features
        }, {
            verbose: env.DEBUG
        });
    }

    /**
     * Fetch the Shared Locations visible to a Team Account service credential
     * Each location is a track whose last coordinate is the current position, coords are [lng, lat, alt, time]
     */
    async fetchLocations(creds: Static<typeof TeamSource>, verbose: boolean): Promise<Static<typeof Feature.InputFeature>[]> {
        console.log(`ok - requesting shared locations for ${creds.AccountId}`);

        // The signed payload must match the json parameter or CalTopo rejects the request
        const payload = JSON.stringify({ bbox: null, zoom: 8 });
        const url = sign('GET', new URL('/api/v1/geodata/locations', CALTOPO), creds, payload);
        url.searchParams.set('json', payload);

        const res = await fetch(url);
        assertBody(res);
        const body = await res.typed(Type.Object({
            status: Type.String(),
            timestamp: Type.Optional(Type.Integer()),
            result: Type.Object({
                features: Type.Array(Type.Object({
                    id: Type.Union([Type.String(), Type.Integer()]),
                    properties: LocationOutput,
                    geometry: Type.Optional(Type.Any()),
                })),
            }),
        }), { verbose });

        const now = Date.now();
        const features: Static<typeof Feature.InputFeature>[] = [];

        for (const loc of body.result.features) {
            let coord: number[] | undefined;
            if (loc.geometry?.type === 'Point') {
                coord = loc.geometry.coordinates;
            } else if (loc.geometry?.type === 'LineString' && loc.geometry.coordinates.length) {
                coord = loc.geometry.coordinates[loc.geometry.coordinates.length - 1];
            }

            if (!coord) continue;

            const updated = loc.properties.updated ?? coord[3] ?? now;
            const ttl = loc.properties.ttl ?? LOCATION_TTL;
            if (updated + ttl < now) continue;

            const id = String(loc.id);
            const callsign = loc.properties.title || loc.properties.device || id;

            const feat: Static<typeof Feature.InputFeature> = {
                id: `caltopo-loc-${id}`,
                type: 'Feature',
                properties: {
                    type: 'a-f-G-U-C',
                    how: 'm-g',
                    callsign,
                    time: new Date(updated).toISOString(),
                    start: new Date(updated).toISOString(),
                    stale: new Date(updated + ttl).toISOString(),
                    metadata: loc.properties
                },
                geometry: {
                    type: 'Point',
                    coordinates: coord.slice(0, 3)
                }
            };

            if (loc.properties['aircraft:heading'] !== undefined) {
                feat.properties.course = Math.round(loc.properties['aircraft:heading']);
            }

            features.push(feat);
        }

        return features;
    }

    /**
     * Fetch a single CalTopo map and transform its objects into CloudTAK features
     */
    async fetchMap(url: URL, verbose: boolean): Promise<Static<typeof Feature.InputFeature>[]> {
        console.log(`ok - requesting ${url.pathname}`);

        const res = await fetch(url);
        assertBody(res);
        const body = await res.typed(Type.Object({
            status: Type.String(),
            timestamp: Type.Integer(),
            result: Type.Object({
                state: Type.Object({
                    type: Type.String({ const: 'FeatureCollection' }),
                    features: Type.Array(Type.Object({
                        id: Type.String(),
                        type: Type.Literal('Feature'),
                        properties: Output,
                        geometry: Type.Optional(Type.Any())
                    }))
                }),
                timestamp: Type.Integer(),
            }),
        }), { verbose });

        const folders: Map<string, Static<typeof Output>> = new Map();

        const features: Static<typeof Feature.InputFeature>[] = body.result.state.features
            .filter((feat) => {
                if (feat.properties.class === 'Folder') {
                    folders.set(feat.id, feat.properties);
                    return false;
                } else {
                    // SARTopo will send "features" like "Operational Periods" which do not have geometry
                    return !!feat.geometry;
                }
            })
            .map((calFeat) => {
                const feat: Static<typeof Feature.InputFeature> = {
                    id: calFeat.id,
                    type: 'Feature',
                    properties: {
                        metadata: calFeat.properties
                    },
                    geometry: calFeat.geometry
                };
                const metadata = feat.properties.metadata ?? {};

                feat.properties.callsign = String(calFeat.properties.title);
                feat.properties.remarks = calFeat.properties.description ? String(calFeat.properties.description) : '';

                if (metadata.fill !== undefined) feat.properties.fill = String(metadata.fill);
                if (metadata['fill-opacity'] !== undefined) feat.properties['fill-opacity'] = Number(metadata['fill-opacity']);
                if (metadata.stroke !== undefined) feat.properties.stroke = String(metadata.stroke);
                if (metadata['stroke-opacity'] !== undefined) feat.properties['stroke-opacity'] = Number(metadata['stroke-opacity']);
                if (metadata['stroke-width'] !== undefined) feat.properties['stroke-width'] = Number(metadata['stroke-width']);
                if (metadata.ico !== undefined) feat.properties.icon = String(metadata.icon);

                // CalTopo returns points with 4+ coords
                coordEach(feat.geometry, (coord) => {
                    return coord.splice(3)
                });

                feat.properties.archived = true;
                if (feat.geometry.type === 'Point') {
                    feat.properties.type = 'u-d-p';

                    if (metadata['marker-color']) {
                        feat.properties['marker-color'] = `#${metadata['marker-color']}`;
                        delete metadata['marker-color'];
                        feat.properties['marker-opacity'] = 1;
                    }
                }

                return feat;
            })
            // After all Features/Folders have been seperated, apply folder => path transform
            .map((feat) => {
                const metadata = feat.properties.metadata;
                if (metadata?.folderId && typeof metadata.folderId === 'string') {
                    const folder = folders.get(metadata.folderId);
                    if (folder) {
                        feat.path = `/${folder.title}`;
                    }
                }

                return feat;
            });

        return features;
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(await Task.init(import.meta.url), event);
}


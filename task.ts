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
    MapId: Type.Optional(Type.String({
        description: 'Limit to a single Map ID, otherwise every map on the team account is imported',
    })),
}, { title: 'Team Account' });

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

        let features: Static<typeof Feature.InputFeature>[] = [];

        if (env.Source.Mode === 'Map') {
            const url = new URL(`/api/v1/map/${env.Source.MapId}/since/-500`, CALTOPO);
            features = await this.fetchMap(url, env.DEBUG);
        } else if (env.Source.MapId) {
            const url = sign('GET', new URL(`/api/v1/map/${env.Source.MapId}/since/-500`, CALTOPO), env.Source);
            features = await this.fetchMap(url, env.DEBUG);
        } else {
            for (const map of await this.fetchTeamMaps(env.Source, env.DEBUG)) {
                const url = sign('GET', new URL(`/api/v1/map/${map.id}/since/-500`, CALTOPO), env.Source);
                features.push(...await this.fetchMap(url, env.DEBUG, `/${map.title}`));
            }
        }

        await this.submit({
            type: 'FeatureCollection',
            features: features
        }, {
            verbose: env.DEBUG
        });
    }

    /**
     * List the Collaborative Maps visible to a Team Account service credential
     */
    async fetchTeamMaps(creds: Static<typeof TeamSource>, verbose: boolean): Promise<Array<{ id: string, title: string }>> {
        console.log(`ok - requesting team ${creds.AccountId}`);

        const url = sign('GET', new URL(`/api/v1/acct/${creds.AccountId}/since/0`, CALTOPO), creds);

        const res = await fetch(url);
        const body = await res.typed(Type.Object({
            status: Type.String(),
            result: Type.Object({
                state: Type.Object({
                    features: Type.Array(Type.Object({
                        id: Type.String(),
                        properties: Type.Object({
                            class: Type.String(),
                            title: Type.Optional(Type.String()),
                        }),
                    }))
                }),
            }),
        }), { verbose });

        return body.result.state.features
            .filter((feat) => feat.properties.class === 'CollaborativeMap')
            .map((feat) => ({ id: feat.id, title: feat.properties.title || feat.id }));
    }

    /**
     * Fetch a single CalTopo map and transform its objects into CloudTAK features
     *
     * @param prefix - Path prefix applied to every feature, used to separate maps from a Team Account
     */
    async fetchMap(url: URL, verbose: boolean, prefix = ''): Promise<Static<typeof Feature.InputFeature>[]> {
        console.log(`ok - requesting ${url.pathname}`);

        const res = await fetch(url);
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
                        feat.path = `${prefix}/${folder.title}`;
                    }
                }

                if (!feat.path && prefix) feat.path = prefix;

                return feat;
            });

        return features;
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(await Task.init(import.meta.url), event);
}


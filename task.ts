import { Type, TSchema, Static } from '@sinclair/typebox';
import { Feature } from '@tak-ps/node-cot';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import { coordEach } from '@turf/meta';
import CalTopo, { Credentials, MapObject, MapFeature, LocationFeature, LOCATION_TTL } from './lib/caltopo.js';

const MapSource = Type.Object({
    Mode: Type.Literal('Map'),
    MapId: Type.String({
        description: 'CalTopo Map or Share ID',
    }),
}, { title: 'Single Map' });

const TeamSource = Type.Composite([
    Type.Object({
        Mode: Type.Literal('Team'),
        AccountId: Type.String({
            description: 'CalTopo Team Account ID',
        }),
    }),
    Credentials,
    Type.Object({
        SinceDelta: Type.Optional(Type.Integer({
            minimum: 1,
            description: 'Only request locations updated within the last N seconds, leave blank to request all locations',
        })),
    })
], {
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
                return MapObject;
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

        const caltopo = new CalTopo({ verbose: env.DEBUG });

        let features: Static<typeof Feature.InputFeature>[];
        if (env.Source.Mode === 'Map') {
            features = this.fromMap(await caltopo.map(env.Source.MapId));
        } else {
            console.log(`ok - requesting shared locations for ${env.Source.AccountId}`);

            const since = env.Source.SinceDelta
                ? Date.now() - env.Source.SinceDelta * 1000
                : undefined;

            features = this.fromLocations(await caltopo.locations(env.Source, { since }));
        }

        await this.submit({
            type: 'FeatureCollection',
            features: features
        }, {
            verbose: env.DEBUG
        });
    }

    /**
     * Transform Shared Locations into CloudTAK features at each device's current position
     */
    fromLocations(locations: Static<typeof LocationFeature>[]): Static<typeof Feature.InputFeature>[] {
        const now = Date.now();
        const features: Static<typeof Feature.InputFeature>[] = [];

        for (const loc of locations) {
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
     * Transform the objects of a single CalTopo map into CloudTAK features
     */
    fromMap(objects: Static<typeof MapFeature>[]): Static<typeof Feature.InputFeature>[] {
        const folders: Map<string, Static<typeof MapObject>> = new Map();

        return objects
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
    }
}

await local(await Task.init(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(await Task.init(import.meta.url), event);
}

import { Type, TSchema, Static } from '@sinclair/typebox';
import type Lambda from 'aws-lambda';
import { Feature } from '@tak-ps/node-cot';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType, OutgoingMessageType, OutgoingAction } from '@tak-ps/etl';
import { coordEach } from '@turf/meta';
import CalTopo, { Credentials, MapObject, MapFeature, LocationFeature, NewMap, MapMode, MapSharing, LOCATION_TTL } from './lib/caltopo.js';

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

const OutgoingEnv = Type.Composite([
    Type.Object({
        AccountId: Type.String({
            description: 'CalTopo Team Account ID the Maps are created in',
        }),
    }),
    Credentials,
    Type.Object({
        // Declared with enum rather than Type.Enum so the Layer environment UI renders a select
        MapMode: Type.Unsafe<MapMode>(Type.String({
            enum: Object.values(MapMode),
            default: MapMode.SAR,
            description: 'Mode of created Maps - sar (Search & Rescue) or cal (Recreational)'
        })),
        MapSharing: Type.Unsafe<MapSharing>(Type.String({
            enum: Object.values(MapSharing),
            default: MapSharing.SECRET,
            description: 'Default sharing of created Maps - PRIVATE (creator only), SECRET (secret URL), URL (public URL) or PUBLIC'
        })),
        MapLayers: Type.Array(Type.Object({
            layer: Type.String({ description: 'CalTopo layer ID - ie: mbt (MapBuilder Topo), mbh (MapBuilder Hybrid) or imagery' })
        }), {
            default: [{ layer: 'mbt' }],
            description: 'Active base layers of created Maps'
        }),
        MarkerColor: Type.String({
            default: 'FF0000',
            description: 'Hex colour (without #) of the Marker placed at the CoreEvent location'
        }),
        'DEBUG': Type.Boolean({
            default: false,
            description: 'Print results in logs'
        })
    })
], {
    description: 'Creates a CalTopo Map in the Team Account for every CoreEvent created - the Map ID is filed under the caltopo external ID of the Event, which requires the event:read & event:update permissions'
});

/** The CoreEvent external ID system the created Map ID is filed under */
export const EXTERNAL_SYSTEM = 'caltopo';

// Subset of CloudTAK's CoreEventResponse carried by event:<action> messages
export const CoreEvent = Type.Object({
    id: Type.String(),
    name: Type.String(),
    remarks: Type.Optional(Type.String()),
    location: Type.Optional(Type.String()),
    external_ids: Type.Optional(Type.Record(Type.String(), Type.String())),
    links: Type.Optional(Type.Array(Type.Object({
        name: Type.String(),
        url: Type.String()
    }))),
    geometry: Type.Object({
        type: Type.Literal('Point'),
        coordinates: Type.Array(Type.Number())
    })
});

export type CoreEvent = Static<typeof CoreEvent>;
export type OutgoingEnv = Static<typeof OutgoingEnv>;

export default class Task extends ETL {
    static name = 'etl-caltopo';
    static flow = [ DataFlowType.Incoming, DataFlowType.Outgoing ];
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
        } else if (type === SchemaType.Input) {
            return OutgoingEnv;
        } else {
            return Type.Object({});
        }
    }

    /**
     * event:create - a created CoreEvent becomes a new CalTopo Map in the Team Account
     * with a Marker at the Event location carrying its callsign, remarks & location
     *
     * The Map ID is filed under the caltopo external ID of the Event, so an Event that
     * already has one (ie: a redelivered message) never gets a second Map
     */
    async outgoing(event: Lambda.SQSEvent): Promise<boolean> {
        const env = await this.env(OutgoingEnv, DataFlowType.Outgoing);
        const caltopo = this.client(env.DEBUG);

        for (const message of Task.outgoingMessages(event)) {
            if (message.type !== OutgoingMessageType.Event || message.action !== OutgoingAction.Create) {
                if (env.DEBUG) console.log(`ok - skip - ${message.type}:${'action' in message ? message.action : '*'} is not an event:create`);
                continue;
            }

            // The message is a snapshot from creation time - read the Event back so a redelivery sees the Map ID
            const core = await this.coreEvent(this.type(CoreEvent, message.data).id);

            const known = core.external_ids?.[EXTERNAL_SYSTEM];
            if (known) {
                console.log(`ok - skip - event ${core.id} already has map ${known}`);
                continue;
            }

            const map = this.mapFromEvent(core, env);
            if (env.DEBUG) console.log(`ok - creating map "${map.properties.title}" for event ${core.id}`);

            const id = await caltopo.createMap(env.AccountId, env, map);
            console.log(`ok - created map ${id} for event ${core.id}`);

            await this.record(core, id, caltopo.mapUrl(id), env.MapSharing);
        }

        return true;
    }

    /** The current state of a CoreEvent - the Layer needs the event:read permission */
    async coreEvent(id: string): Promise<CoreEvent> {
        try {
            return this.type(CoreEvent, await this.fetch(`/api/core/event/${id}`));
        } catch (err) {
            throw new Error(`Failed to read CoreEvent ${id} - the Layer requires the event:read permission: ${err instanceof Error ? err.message : String(err)}`, { cause: err });
        }
    }

    /**
     * Map titled after the CoreEvent callsign with a single Marker at its location
     */
    mapFromEvent(core: CoreEvent, env: OutgoingEnv): Static<typeof NewMap> {
        const description = [
            core.remarks,
            core.location ? `Location: ${core.location}` : ''
        ].filter(Boolean).join('\n\n');

        return {
            properties: {
                title: core.name,
                mode: env.MapMode,
                mapConfig: JSON.stringify({
                    activeLayers: env.MapLayers.map((l) => [l.layer, 1])
                }),
                sharing: env.MapSharing
            },
            state: {
                type: 'FeatureCollection',
                features: [{
                    type: 'Feature',
                    geometry: {
                        type: 'Point',
                        coordinates: core.geometry.coordinates.slice(0, 2)
                    },
                    properties: {
                        title: core.name,
                        description,
                        'marker-symbol': 'point',
                        'marker-color': env.MarkerColor.replace(/^#/, ''),
                        'marker-size': '1'
                    }
                }]
            }
        };
    }

    /**
     * File the Map ID under the caltopo external ID of the Event and, when the Map is
     * reachable by URL (any sharing but PRIVATE), add that URL to the Event links.
     * PATCH replaces the links array, so the current links are re-sent with the Map appended.
     * A failure is logged rather than thrown - a retry would create a second Map
     */
    async record(core: CoreEvent, id: string, url: string, sharing: MapSharing): Promise<void> {
        const body: Record<string, unknown> = {
            external_id: { system: EXTERNAL_SYSTEM, value: id }
        };

        const links = core.links || [];
        if (sharing !== MapSharing.PRIVATE && !links.some((l) => l.url === url)) {
            body.links = [...links, { name: 'CalTopo Map', url }];
        }

        try {
            await this.fetch(`/api/core/event/${core.id}`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });
        } catch (err) {
            console.error(`not ok - failed to record map ${id} on CoreEvent ${core.id} - the Layer requires the event:update permission:`, err);
        }
    }

    async control(): Promise<void> {
        const raw = await this.env(Type.Union([Env, LegacyEnv]));

        const env: Static<typeof Env> = 'ShareId' in raw
            ? { Source: { Mode: 'Map', MapId: raw.ShareId }, DEBUG: raw.DEBUG }
            : raw;

        const caltopo = this.client(env.DEBUG);

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

    client(verbose: boolean): CalTopo {
        return new CalTopo({ verbose });
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

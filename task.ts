import { Type, TSchema, Static } from '@sinclair/typebox';
import { Feature } from '@tak-ps/node-cot';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, DataFlowType, InvocationType } from '@tak-ps/etl';
import { fetch } from '@tak-ps/etl';
import { coordEach } from '@turf/meta';

const Env = Type.Object({
    ShareId: Type.String({
        description: 'CalTopo Share ID',
    }),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print results in logs'
    })
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
        const env = await this.env(Env);

        let features: Static<typeof Feature.InputFeature>[] = [];
        console.log(`ok - requesting ${env.ShareId}`);

        const url = new URL(`/api/v1/map/${env.ShareId}/since/-500`, 'https://caltopo.com/')

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
        }), {
            verbose: env.DEBUG
        });

        const folders: Map<string, Static<typeof Output>> = new Map();

        features = body.result.state.features
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

                feat.properties.callsign = String(calFeat.properties.title);
                feat.properties.remarks = calFeat.properties.description ? String(calFeat.properties.description) : '';

                if (feat.properties.metadata.fill !== undefined) feat.properties.fill = String(feat.properties.metadata.fill);
                if (feat.properties.metadata['fill-opacity'] !== undefined) feat.properties['fill-opacity'] = Number(feat.properties.metadata['fill-opacity']);
                if (feat.properties.metadata.stroke !== undefined) feat.properties.stroke = String(feat.properties.metadata.stroke);
                if (feat.properties.metadata['stroke-opacity'] !== undefined) feat.properties['stroke-opacity'] = Number(feat.properties.metadata['stroke-opacity']);
                if (feat.properties.metadata['stroke-width'] !== undefined) feat.properties['stroke-width'] = Number(feat.properties.metadata['stroke-width']);
                if (feat.properties.metadata.ico !== undefined) feat.properties.icon = String(feat.properties.metadata.icon);

                // CalTopo returns points with 4+ coords
                coordEach(feat.geometry, (coord) => {
                    return coord.splice(3)
                });

                feat.properties.archived = true;
                if (feat.geometry.type === 'Point') {
                    feat.properties.type = 'u-d-p';

                    if (feat.properties.metadata['marker-color']) {
                        feat.properties['marker-color'] = `#${feat.properties.metadata['marker-color']}`;
                        delete feat.properties.metadata['marker-color'];
                        feat.properties['marker-opacity'] = 1;
                    }
                }

                return feat;
            });

        // After all Features/Folders have been seperated, apply folder => path transform
        features = features.map((feat) => {
            if (feat.properties.metadata.folderId && typeof feat.properties.metadata.folderId === 'string') {
                const folder = folders.get(feat.properties.metadata.folderId);
                if (folder) {
                    feat.path = `/${folder.title}`;
                }
            }

            return feat;
        });

        await this.submit({
            type: 'FeatureCollection',
            features: features
        }, {
            verbose: env.DEBUG
        });
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}


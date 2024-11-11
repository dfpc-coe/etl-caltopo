import { Type, TSchema, Static } from '@sinclair/typebox';
import type { Feature } from 'geojson';
import { Feature as COTFeature } from '@tak-ps/node-cot';
import type { Event } from '@tak-ps/etl';
import ETL, { SchemaType, handler as internal, local, env } from '@tak-ps/etl';
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

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Env;
        } else {
            return Type.Object({
                title: Type.String(),
                class: Type.String(),
                creator: Type.String()
            });
        }
    }

    async control(): Promise<void> {
        const env = await this.env(Env);

        let features: Static<typeof COTFeature.InputFeature>[] = [];
        console.log(`ok - requesting ${env.ShareId}`);

        const url = new URL(`/api/v1/map/${env.ShareId}/since/-500`, 'https://caltopo.com/')

        const res = await fetch(url);
        const body = await res.typed(Type.Object({
            status: Type.String(),
            timestamp: Type.Integer(),
            result: Type.Object({
                state: Type.Object({
                    type: Type.String({ const: 'FeatureCollection' }),
                    features: Type.Array(Type.Any())
                }),
                timestamp: Type.Integer(),
            }),
        }));

        features.push(...body.result.state.features)

        features = features
            .filter((feat) => {
                // SARTopo will send "features" like "Operational Periods" which do not have geometry
                return !!feat.geometry;
            })
            .map((feat) => {
                feat.properties = {
                    metadata: feat.properties
                };

                feat.properties.callsign = String(feat.properties.metadata.title);
                feat.properties.remarks = feat.properties.metadata.description ? String(feat.properties.metadata.description) : '';

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

        await this.submit({
            type: 'FeatureCollection',
            features: features as Feature[]
        });
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}


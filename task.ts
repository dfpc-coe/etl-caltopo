import fs from 'node:fs';
import { Type, TSchema } from '@sinclair/typebox';
import { FeatureCollection, Feature, Geometry } from 'geojson';
import ETL, { Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';
import { coordEach } from '@turf/meta';

export interface Share {
    ShareId: string;
}

export default class Task extends ETL {
    static async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Type.Object({
                'CALTOPO_SHARE_IDS': Type.Array(Type.Object({
                    Name: Type.String({
                        description: 'Human Readable name of the CalTopo Map',
                        default: ''
                    }),
                    ShareId: Type.String({
                        description: 'CalTopo Share ID'
                    }),
                })),
                'DEBUG': Type.Boolean({
                    default: false,
                    description: 'Print results in logs'
                })
            });
        } else {
            return Type.Object({
                title: Type.String(),
                class: Type.String(),
                creator: Type.String()
            });
        }
    }

    async control(): Promise<void> {
        const layer = await this.fetchLayer();

        if (!layer.environment.CALTOPO_SHARE_IDS) throw new Error('No CALTOPO_SHARE_IDS Provided');
        if (!Array.isArray(layer.environment.CALTOPO_SHARE_IDS)) throw new Error('CALTOPO_SHARE_IDS must be an array');

        const features: Feature[] = [];
        const obtains: Array<Promise<Feature[]>> = [];
        for (const share of layer.environment.CALTOPO_SHARE_IDS) {
            obtains.push((async (share: Share): Promise<Feature[]> => {
                console.log(`ok - requesting ${share.ShareId}`);

                const url = new URL(`/api/v1/map/${share.ShareId}/since/-500`, 'https://caltopo.com/')

                const res = await fetch(url);
                const body = await res.json();

                if (body.result && body.result.state && body.result.state.features) {
                    features.push(...body.result.state.features)
                }

                return features
                    .filter((feat) => {
                        // SARTopo will send "features" like "Operational Periods" which do not have geometry
                        return !!feat.geometry;
                    })
                    .map((feat) => {
                        feat.properties = {
                            metadata: feat.properties
                        };

                        feat.properties.callsign = feat.properties.metadata.title;
                        feat.properties.remarks = feat.properties.metadata.description;

                        // CalTopo returns points with 4+ coords
                        // @ts-ignore
                        coordEach(feat.geometry, (coord) => {
                            return coord.splice(3)
                        });

                        return feat;
                    });
            })(share))
        }

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        }

        for (const res of await Promise.all(obtains)) {
            if (!res || !res.length) continue;
            fc.features.push(...res);
        }

        await this.submit(fc);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}


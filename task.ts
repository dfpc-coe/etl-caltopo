import { Type, TSchema } from '@sinclair/typebox';
import { FeatureCollection, Feature } from 'geojson';
import ETL, { Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';
import { fetch } from '@tak-ps/etl';
import { coordEach } from '@turf/meta';

const Env = Type.Object({
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

        const features: Feature[] = [];
        const obtains: Array<Promise<Feature[]>> = [];
        for (const share of env.CALTOPO_SHARE_IDS) {
            obtains.push((async (share): Promise<Feature[]> => {
                console.log(`ok - requesting ${share.ShareId}`);

                const url = new URL(`/api/v1/map/${share.ShareId}/since/-500`, 'https://caltopo.com/')

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
                        feat.properties.remarks = feat.properties.metadata.description || '';

                        for (const key of ['fill', 'fill-opacity', 'stroke', 'stroke-width', 'stroke-opacity', 'icon']) {
                            if (feat.properties.metadata[key] !== undefined) {
                                feat.properties[key] = feat.properties.metadata[key];
                                delete feat.properties.metadata[key];
                            }
                        }

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


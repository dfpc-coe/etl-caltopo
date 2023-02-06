import fs from 'node:fs';
import { FeatureCollection, Feature, Geometry } from 'geojson';
import { JSONSchema6 } from 'json-schema';
import ETL, { Event, SchemaType } from '@tak-ps/etl';

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(String(fs.readFileSync(dotfile))));
} catch (err) {
    console.log('ok - no .env file loaded');
}

export interface Share {
    ShareId: string;
}

export default class Task extends ETL {
    static async schema(type: SchemaType = SchemaType.Input): Promise<JSONSchema6> {
        if (type === SchemaType.Input) {
            return {
                type: 'object',
                required: ['CALTOPO_SHARE_IDS'],
                properties: {
                    'CALTOPO_SHARE_IDS': {
                        type: 'array',
                        items: {
                            type: 'object',
                            required: ['ShareId'],
                            properties: {
                                ShareId: {
                                    type: 'string',
                                    description: 'CalTopo Share ID'
                                }
                            }
                        }
                    },
                    'DEBUG': {
                        type: 'boolean',
                        default: false,
                        description: 'Print ADSBX results in logs'
                    }
                }
            }
        } else {
            return {
                type: 'object',
                required: [],
                properties: {
                    title: {
                        type: 'string'
                    },
                    class: {
                        type: 'string'
                    },
                    creator: {
                        type: 'string'
                    }
                }
            }
        }
    }

    async control(): Promise<void> {
        const layer = await this.layer();

        if (!layer.environment.CALTOPO_SHARE_IDS) throw new Error('No CALTOPO_SHARE_IDS Provided');
        if (!Array.isArray(layer.environment.CALTOPO_SHARE_IDS)) throw new Error('CALTOPO_SHARE_IDS must be an array');

        const obtains: Array<Promise<Feature[]>> = [];
        for (const share of layer.environment.CALTOPO_SHARE_IDS) {
            obtains.push((async (share: Share): Promise<Feature[]> => {
                console.log(`ok - requesting ${share.ShareId}`);

                const url = new URL(`/api/v1/map/${share.ShareId}/since/-500`, 'https://caltopo.com/')

                const res = await fetch(url);
                const body = await res.json();

                features.push(...body.state.features)

                return features;
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

export async function handler(event: Event = {}) {
    if (event.type === 'schema:input') {
        return await Task.schema(SchemaType.Input);
    } else if (event.type === 'schema:output') {
        return await Task.schema(SchemaType.Output);
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();

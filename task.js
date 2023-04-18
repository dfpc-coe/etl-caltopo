import fs from 'node:fs';
import os from 'node:os';
import ETL from '@tak-ps/etl';
import path from 'node:path';
import EsriDump from 'esri-dump';

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(fs.readFileSync(dotfile)));
} catch (err) {
    console.log('ok - no .env file loaded');
}

export default class Task extends ETL {
    static schema() {
        return {
            type: 'object',
            required: ['ARCGIS_URL'],
            properties: {
                'ARCGIS_URL': {
                    type: 'string',
                    description: 'ArcGIS MapServer URL to pull data from'
                },
                'ARCGIS_HEADERS': {
                    type: 'array',
                    description: 'Headers to include in the request',
                    items: {
                        type: 'object',
                        required: [
                            'key',
                            'value'
                        ],
                        properties: {
                            key: {
                                type: 'string'
                            },
                            value: {
                                type: 'string'
                            }
                        }
                    }
                },
                'ARCGIS_PARAMS': {
                    type: 'array',
                    description: 'URL Params to include in the request',
                    items: {
                        type: 'object',
                        required: [
                            'key',
                            'value'
                        ],
                        properties: {
                            key: {
                                type: 'string'
                            },
                            value: {
                                type: 'string'
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
        };
    }

    async control() {
        const layer = await this.layer();

        if (!layer.environment.ARCGIS_URL) throw new Error('No ArcGIS_URL Provided');

        if (!layer.environment.ARCGIS_HEADERS) layer.environment.ARCGIS_HEADERS = [];
        if (!layer.environment.ARCGIS_PARAMS) layer.environment.ARCGIS_PARAMS = [];

        const config = {
            approach: 'iter',
            headers: {},
            params: {}
        };

        for (const header of layer.environment.ARCGIS_HEADERS) {
            if (!header.name.trim()) continue;
            config.headers[header.name] = header.value || '';
        }
        for (const param of layer.environment.ARCGIS_PARAMS) {
            if (!param.name.trim()) continue;
            config.headers[param.name] = param.value || '';
        }

        const dumper = new EsriDump(layer.environment.ARCGIS_URL, config);

        dumper.fetch();

        const fc = {
            type: 'FeatureCollection',
            features: []
        };

        await new Promise((resolve, reject) => {
            dumper.on('feature', (feature) => {
                fc.features.push(feature);
            }).on('error', (err) => {
                reject(err);
            }).on('done', () => {
                return resolve();
            });
        });

        await this.submit(fc);
    }
}

export async function handler(event = {}) {
    if (event.type === 'schema') {
        return Task.schema();
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();

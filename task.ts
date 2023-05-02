import fs from 'node:fs';
import { FeatureCollection } from 'geojson';
import ETL, {
    Event
} from '@tak-ps/etl';

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(String(fs.readFileSync(dotfile))));
} catch (err) {
    console.log('ok - no .env file loaded');
}

export default class Task extends ETL {
    static schema() {
        return {
            type: 'object',
            required: ['INREACH_MAP_SHARES'],
            properties: {
                'INREACH_MAP_SHARES': {
                    type: 'array',
                    description: 'Inreach Share IDs to pull data from',
                    items: {
                        type: 'object',
                        required: [
                            'ShareID',
                        ],
                        properties: {
                            ShareId: {
                                type: 'string'
                            },
                            Password: {
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

    async control(): Promise<void> {
        const layer = await this.layer();

        if (!layer.environment.INREACH_MAP_SHARES) throw new Error('No INREACH_MAP_SHARES Provided');

        for (const share of layer.environment.INREACH_MAP_SHARES) {

        }

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: []
        }

        await this.submit(fc);
    }
}

export async function handler(event: Event = {}) {
    if (event.type === 'schema') {
        return Task.schema();
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();

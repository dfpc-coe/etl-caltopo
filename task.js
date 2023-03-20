import fs from 'fs';
import ETL from '@tak-ps/etl';
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

        if (!layer.data.environment.ARCGIS_URL) throw new Error('No ArcGIS_URL Provided');

        const dumper = new ESRIDump(this.layer.environment.ARCGIS_URL);

        dumper.fetch();

        return new Promise((resolve, reject) => {
            dumper
                .on('feature', (feature) => {
                    console.log(feature);
                })
                .on('error', reject)
                .on('done', () => {
                    console.error('done');
                });
        });
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

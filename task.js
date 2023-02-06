import fs from 'fs';

try {
    const dotfile = new URL('.env', import.meta.url);

    fs.accessSync(dotfile);

    Object.assign(process.env, JSON.parse(fs.readFileSync(dotfile)));
    console.log('ok - .env file loaded');
} catch (err) {
    console.log('ok - no .env file loaded');
}

export default class Task {
    constructor() {
        this.token = process.env.ADSBX_TOKEN;
        this.api = 'https://adsbexchange.com/api/aircraft/v2/lat/42.0875/lon/-110.5905/dist/800/';

        this.etl = {
            api: process.env.ETL_API,
            layer: process.env.ETL_LAYER,
            token: process.env.ETL_TOKEN
        };

        if (!this.token) throw new Error('No ADSBX API Token Provided');
        if (!this.etl.api) throw new Error('No ETL API URL Provided');
        if (!this.etl.layer) throw new Error('No ETL Layer Provided');
        if (!this.etl.token) throw new Error('No ETL Token Provided');
    }

    static schema() {
        return {
            type: 'object',
            required: ['ADSBX_TOKEN'],
            properties: {
                'ADSBX_TOKEN': {
                    type: 'string',
                    description: 'API Token for ADSBExachange'
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
        const url = new URL(this.api);
        url.searchParams.append('apiKey', this.token);

        const res = await fetch(url, {
            headers: {
                'api-auth': this.token
            }
        });

        const features = [];
        for (const ac of (await res.json()).ac) {
            if (!ac.flight && !ac.r) continue;

            const id = ac.r || ac.flight;
            const callsign = ac.flight || ac.r;
            const coordinates = [ac.lon, ac.lat];
            if (!isNaN(parseInt(ac.alt_baro))) coordinates.push(ac.alt_baro);

            const feat = {
                id: id.trim(),
                type: 'Feature',
                properties: {
                    callsign: callsign.trim(),
                    squak: ac.squak,
                    emergency: ac.emergency
                },
                geometry: {
                    type: 'Point',
                    coordinates
                }
            };

            features.push(feat);
        }

        console.log(`ok - fetched ${features.length} planes`);

        const fc = {
            type: 'FeatureCollection',
            features: features
        };

        if (process.env.DEBUG) for (const feat of fc.features) console.error(JSON.stringify(feat));

        const post = await fetch(new URL(`/api/layer/${this.etl.layer}/cot`, this.etl.api), {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${this.etl.token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(fc)
        });

        if (!post.ok) {
            console.error(await post.text());
            throw new Error('Failed to post layer to ETL');
        } else {
            console.log(await post.json());
        }
    }
}

export async function handler(event={}) {
    if (event.type === 'schema') {
        return JSON.stringify(Task.schema());
    } else {
        const task = new Task();
        await task.control();
    }
}

if (import.meta.url === `file://${process.argv[1]}`) handler();

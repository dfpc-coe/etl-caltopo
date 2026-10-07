import { Type, Static } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import { createHmac } from 'node:crypto';

export const API = 'https://caltopo.com/';

// Matches the CalTopo Shared Locations overlay default expiry
export const LOCATION_TTL = 30 * 60 * 1000;

export const Credentials = Type.Object({
    CredentialId: Type.String({
        description: 'Service Account Credential ID',
    }),
    CredentialSecret: Type.String({
        description: 'Service Account Credential Secret',
    }),
});

export const Location = Type.Object({
    title: Type.Optional(Type.String()),
    device: Type.Optional(Type.String()),
    sharedWith: Type.Optional(Type.String()),
    type: Type.Optional(Type.String()),
    updated: Type.Optional(Type.Number()),
    ttl: Type.Optional(Type.Number()),
    stroke: Type.Optional(Type.String()),
    'aircraft:heading': Type.Optional(Type.Number()),
});

export const MapObject = Type.Object({
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

export const LocationFeature = Type.Object({
    id: Type.Union([Type.String(), Type.Integer()]),
    properties: Location,
    geometry: Type.Optional(Type.Any()),
});

export const MapFeature = Type.Object({
    id: Type.String(),
    type: Type.Literal('Feature'),
    properties: MapObject,
    geometry: Type.Optional(Type.Any())
});

const LocationsResponse = Type.Object({
    status: Type.String(),
    timestamp: Type.Optional(Type.Integer()),
    result: Type.Object({
        features: Type.Array(LocationFeature),
    }),
});

const MapResponse = Type.Object({
    status: Type.String(),
    timestamp: Type.Integer(),
    result: Type.Object({
        state: Type.Object({
            type: Type.String({ const: 'FeatureCollection' }),
            features: Type.Array(MapFeature)
        }),
        timestamp: Type.Integer(),
    }),
});

export default class CalTopo {
    verbose: boolean;

    constructor(opts: { verbose?: boolean } = {}) {
        this.verbose = opts.verbose ?? false;
    }

    /**
     * Append the CalTopo service account signature query parameters
     * Signing string is "{method} {path}\n{expires}\n{payload}" HMAC-SHA256 with the base64 decoded secret
     */
    static sign(method: string, url: URL, creds: Static<typeof Credentials>, payload = ''): URL {
        const expires = Date.now() + 120 * 1000;
        const data = `${method} ${url.pathname}\n${expires}\n${payload}`;

        const signature = createHmac('sha256', Buffer.from(creds.CredentialSecret, 'base64'))
            .update(data)
            .digest('base64');

        url.searchParams.set('id', creds.CredentialId);
        url.searchParams.set('expires', String(expires));
        url.searchParams.set('signature', signature);

        return url;
    }

    /**
     * CalTopo responds to requests it can't process (ie: a null bbox) with an empty body and a 200 status
     * Surface that as a readable error instead of a JSON parse failure
     */
    static assertBody(res: { ok: boolean, status: number, headers: Headers }): void {
        if (!res.ok) {
            throw new Error(`CalTopo responded with HTTP ${res.status}`);
        } else if (res.headers.get('content-length') === '0') {
            throw new Error('CalTopo returned an empty response - the request was malformed or rejected');
        }
    }

    /**
     * Fetch the Shared Locations visible to a Team Account service credential
     * Each location is a track whose last coordinate is the current position, coords are [lng, lat, alt, time]
     *
     * @param since - Only return locations updated after this ms epoch
     */
    async locations(
        creds: Static<typeof Credentials>,
        opts: { since?: number } = {}
    ): Promise<Static<typeof LocationFeature>[]> {
        // CalTopo returns an empty 200 response if bbox is null, so request the whole world
        // The signed payload must match the json parameter
        const query: Record<string, unknown> = { bbox: [-180, -90, 180, 90], zoom: 8 };
        if (opts.since !== undefined) query.since = opts.since;
        const payload = JSON.stringify(query);

        const url = CalTopo.sign('GET', new URL('/api/v1/geodata/locations', API), creds, payload);
        url.searchParams.set('json', payload);

        console.log(`ok - requesting ${url.pathname}`);

        const res = await fetch(url);
        CalTopo.assertBody(res);
        const body = await res.typed(LocationsResponse, { verbose: this.verbose });

        return body.result.features;
    }

    /**
     * Fetch the objects of a single public Map or Share ID
     */
    async map(id: string): Promise<Static<typeof MapFeature>[]> {
        const url = new URL(`/api/v1/map/${id}/since/-500`, API);

        console.log(`ok - requesting ${url.pathname}`);

        const res = await fetch(url);
        CalTopo.assertBody(res);
        const body = await res.typed(MapResponse, { verbose: this.verbose });

        return body.result.state.features;
    }
}

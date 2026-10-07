import { Type, Static } from '@sinclair/typebox';
import { fetch } from '@tak-ps/etl';
import { createHmac } from 'node:crypto';

export const API = 'https://caltopo.com/';

// Matches the CalTopo Shared Locations overlay default expiry
export const LOCATION_TTL = 30 * 60 * 1000;

export enum MapMode {
    SAR = 'sar',
    CAL = 'cal'
}

export enum MapSharing {
    PRIVATE = 'PRIVATE',
    SECRET = 'SECRET',
    URL = 'URL',
    PUBLIC = 'PUBLIC'
}

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

export const NewMapFeature = Type.Object({
    type: Type.Literal('Feature'),
    geometry: Type.Object({
        type: Type.Union([Type.Literal('Point'), Type.Literal('LineString'), Type.Literal('Polygon')]),
        coordinates: Type.Any()
    }),
    properties: Type.Record(Type.String(), Type.Unknown())
});

export const NewMap = Type.Object({
    properties: Type.Object({
        title: Type.String(),
        mode: Type.Enum(MapMode),
        mapConfig: Type.String({ description: 'JSON encoded {"activeLayers": [["mbt", 1]]}' }),
        sharing: Type.Enum(MapSharing),
        folderId: Type.Optional(Type.Union([Type.String(), Type.Null()])),
        locked: Type.Optional(Type.Boolean())
    }),
    state: Type.Object({
        type: Type.Literal('FeatureCollection'),
        features: Type.Array(NewMapFeature)
    })
});

const CreateMapResponse = Type.Object({
    status: Type.String(),
    result: Type.Object({
        id: Type.String()
    })
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
    url: URL;
    verbose: boolean;

    constructor(opts: { url?: string, verbose?: boolean } = {}) {
        this.url = new URL(opts.url ?? API);
        this.verbose = opts.verbose ?? false;
    }

    /**
     * CalTopo service account signature parameters
     * Signing string is "{method} {path}\n{expires}\n{payload}" HMAC-SHA256 with the base64 decoded secret
     */
    static signature(method: string, url: URL, creds: Static<typeof Credentials>, payload = ''): { id: string, expires: string, signature: string } {
        const expires = Date.now() + 120 * 1000;
        const data = `${method} ${url.pathname}\n${expires}\n${payload}`;

        const signature = createHmac('sha256', Buffer.from(creds.CredentialSecret, 'base64'))
            .update(data)
            .digest('base64');

        return { id: creds.CredentialId, expires: String(expires), signature };
    }

    /**
     * Append the signature query parameters used by GET requests
     */
    static sign(method: string, url: URL, creds: Static<typeof Credentials>, payload = ''): URL {
        for (const [key, value] of Object.entries(CalTopo.signature(method, url, creds, payload))) {
            url.searchParams.set(key, value);
        }

        return url;
    }

    /**
     * Browser URL of a Map
     */
    mapUrl(id: string): string {
        return new URL(`/m/${id}`, this.url).toString();
    }

    /**
     * CalTopo responds to requests it can't process (ie: a null bbox) with an empty body and a 200 status
     * Surface that as a readable error instead of a JSON parse failure
     */
    static async assertBody(res: { ok: boolean, status: number, headers: { get(name: string): string | null }, text(): Promise<string> }): Promise<void> {
        if (!res.ok) {
            const body = await res.text().catch(() => '');
            throw new Error(`CalTopo responded with HTTP ${res.status}${body ? `: ${body.trim().slice(0, 500)}` : ''}`);
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

        const url = CalTopo.sign('GET', new URL('/api/v1/geodata/locations', this.url), creds, payload);
        url.searchParams.set('json', payload);

        console.log(`ok - requesting ${url.pathname}`);

        const res = await fetch(url, { safeUrlAllow: [this.url.origin] });
        await CalTopo.assertBody(res);
        const body = await res.typed(LocationsResponse, { verbose: this.verbose });

        return body.result.features;
    }

    /**
     * Fetch the objects of a single public Map or Share ID
     */
    async map(id: string): Promise<Static<typeof MapFeature>[]> {
        const url = new URL(`/api/v1/map/${id}/since/-500`, this.url);

        console.log(`ok - requesting ${url.pathname}`);

        const res = await fetch(url, { safeUrlAllow: [this.url.origin] });
        await CalTopo.assertBody(res);
        const body = await res.typed(MapResponse, { verbose: this.verbose });

        return body.result.state.features;
    }

    /**
     * Create a Collaborative Map in a Team Account, returning the new Map ID
     * Signed POST requests carry the signature parameters and json payload as a form encoded body
     * The service account requires at least UPDATE permission
     *
     * The CalTopo UI sends the object class & owning Team as accountId in the properties, so they are set here too
     */
    async createMap(
        teamId: string,
        creds: Static<typeof Credentials>,
        map: Static<typeof NewMap>
    ): Promise<string> {
        const url = new URL(`/api/v1/acct/${teamId}/CollaborativeMap`, this.url);
        const payload = JSON.stringify({
            ...map,
            properties: {
                class: 'CollaborativeMap',
                accountId: teamId,
                folderId: null,
                locked: false,
                ...map.properties
            }
        });

        const body = new URLSearchParams(CalTopo.signature('POST', url, creds, payload));
        body.set('json', payload);

        console.log(`ok - requesting POST ${url.pathname}`);

        const res = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: body.toString(),
            safeUrlAllow: [this.url.origin]
        });
        await CalTopo.assertBody(res);
        const created = await res.typed(CreateMapResponse, { verbose: this.verbose });

        return created.result.id;
    }
}

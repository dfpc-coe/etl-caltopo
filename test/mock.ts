import http from 'node:http';
import { createHmac } from 'node:crypto';
import type { AddressInfo } from 'node:net';

export const CREDS = {
    CredentialId: 'cred-id',
    CredentialSecret: Buffer.from('test-secret').toString('base64'),
};

export type Request = {
    method: string;
    pathname: string;
    json: Record<string, unknown> | null;
};

export type Mock = {
    base: string;
    requests: Request[];
    close: () => Promise<void>;
};

function signed(method: string, pathname: string, params: URLSearchParams): boolean {
    const expected = createHmac('sha256', Buffer.from(CREDS.CredentialSecret, 'base64'))
        .update(`${method} ${pathname}\n${params.get('expires')}\n${params.get('json') ?? ''}`)
        .digest('base64');

    return params.get('id') === CREDS.CredentialId && params.get('signature') === expected;
}

/**
 * Minimal CalTopo API - signed Shared Locations & Team Map creation endpoints and a public map endpoint
 */
export async function mock(opts: {
    locations?: unknown[];
    map?: unknown[];
    mapId?: string;
} = {}): Promise<Mock> {
    const state: Mock = { base: '', requests: [], close: async () => {} };

    const server = http.createServer(async (req, res) => {
        const url = new URL(req.url || '/', 'http://localhost');
        const method = req.method || 'GET';

        // Signed POSTs carry the signature parameters and json payload as a form body
        const chunks: Buffer[] = [];
        for await (const chunk of req) chunks.push(chunk as Buffer);
        const params = method === 'POST'
            ? new URLSearchParams(Buffer.concat(chunks).toString())
            : url.searchParams;

        const raw = params.get('json');
        const json = raw ? JSON.parse(raw) : null;
        state.requests.push({ method, pathname: url.pathname, json });

        const send = (code: number, body: unknown) => {
            res.writeHead(code, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(body));
        };

        const create = url.pathname.match(/^\/api\/v1\/acct\/([^/]+)\/CollaborativeMap$/);
        if (method === 'POST' && create) {
            if (req.headers['content-type'] !== 'application/x-www-form-urlencoded') {
                return send(400, { status: 'error', message: 'expected a form body' });
            } else if (!signed('POST', url.pathname, params)) {
                return send(401, { status: 'error', message: 'bad signature' });
            } else if (create[1] === 'READONLY') {
                return send(403, { status: 'error', message: 'service account lacks UPDATE permission' });
            } else if (!json || !json.properties || !json.properties.title) {
                return send(400, { status: 'error', message: 'missing title' });
            }

            return send(200, {
                status: 'ok',
                timestamp: Date.now(),
                result: {
                    id: opts.mapId ?? 'NEWMAP',
                    type: 'Feature',
                    properties: { ...json.properties, accountId: create[1], class: 'CollaborativeMap' }
                }
            });
        }

        if (url.pathname === '/api/v1/geodata/locations') {
            if (!signed('GET', url.pathname, params)) {
                return send(401, { status: 'error', message: 'bad signature' });
            } else if (!json || !json.bbox) {
                // CalTopo answers a null bbox with an empty 200
                res.writeHead(200, { 'Content-Length': '0' });
                return res.end();
            }

            return send(200, {
                status: 'ok',
                timestamp: Date.now(),
                result: { features: opts.locations ?? [] }
            });
        }

        const map = url.pathname.match(/^\/api\/v1\/map\/([^/]+)\/since\/-500$/);
        if (map) {
            return send(200, {
                status: 'ok',
                timestamp: Date.now(),
                result: {
                    state: { type: 'FeatureCollection', features: opts.map ?? [] },
                    timestamp: Date.now()
                }
            });
        }

        send(404, { status: 'error' });
    });

    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    state.base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
    state.close = () => new Promise((resolve) => server.close(() => resolve()));

    return state;
}

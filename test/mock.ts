import http from 'node:http';
import { createHmac } from 'node:crypto';
import type { AddressInfo } from 'node:net';

export const CREDS = {
    CredentialId: 'cred-id',
    CredentialSecret: Buffer.from('test-secret').toString('base64'),
};

export type Request = {
    pathname: string;
    json: Record<string, unknown> | null;
};

export type Mock = {
    base: string;
    requests: Request[];
    close: () => Promise<void>;
};

/**
 * Minimal CalTopo API - signed Shared Locations endpoint & a public map endpoint
 */
export async function mock(opts: {
    locations?: unknown[];
    map?: unknown[];
} = {}): Promise<Mock> {
    const state: Mock = { base: '', requests: [], close: async () => {} };

    const server = http.createServer((req, res) => {
        const url = new URL(req.url || '/', 'http://localhost');
        const raw = url.searchParams.get('json');
        const json = raw ? JSON.parse(raw) : null;
        state.requests.push({ pathname: url.pathname, json });

        const send = (code: number, body: unknown) => {
            res.writeHead(code, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(body));
        };

        if (url.pathname === '/api/v1/geodata/locations') {
            const expires = url.searchParams.get('expires');
            const expected = createHmac('sha256', Buffer.from(CREDS.CredentialSecret, 'base64'))
                .update(`GET ${url.pathname}\n${expires}\n${raw ?? ''}`)
                .digest('base64');

            if (url.searchParams.get('id') !== CREDS.CredentialId || url.searchParams.get('signature') !== expected) {
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

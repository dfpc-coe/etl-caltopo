import test from 'node:test';
import assert from 'node:assert';
import { createHmac } from 'node:crypto';
import CalTopo, { API, MapMode, MapSharing } from '../lib/caltopo.js';
import { mock, CREDS } from './mock.js';

test('sign - appends id, expiry & HMAC over method, path, expiry and payload', () => {
    const before = Date.now();
    const url = CalTopo.sign('GET', new URL('/api/v1/geodata/locations', API), CREDS, '{"zoom":8}');

    assert.equal(url.searchParams.get('id'), 'cred-id');

    const expires = Number(url.searchParams.get('expires'));
    assert.ok(expires >= before + 120_000 && expires <= Date.now() + 120_000);

    const expected = createHmac('sha256', Buffer.from(CREDS.CredentialSecret, 'base64'))
        .update(`GET /api/v1/geodata/locations\n${expires}\n{"zoom":8}`)
        .digest('base64');
    assert.equal(url.searchParams.get('signature'), expected);
});

test('assertBody - surfaces HTTP errors and empty 200 bodies', () => {
    assert.throws(() => {
        CalTopo.assertBody({ ok: false, status: 401, headers: new Headers() });
    }, /HTTP 401/);

    assert.throws(() => {
        CalTopo.assertBody({ ok: true, status: 200, headers: new Headers({ 'content-length': '0' }) });
    }, /empty response/);

    CalTopo.assertBody({ ok: true, status: 200, headers: new Headers({ 'content-length': '12' }) });
});

test('locations - signed world bbox request, since only when provided', async () => {
    const api = await mock({
        locations: [{ id: 1, properties: { title: 'Unit 1' }, geometry: { type: 'Point', coordinates: [-105, 39, 0, 1] } }]
    });

    try {
        const caltopo = new CalTopo({ url: api.base });

        const features = await caltopo.locations(CREDS);
        assert.equal(features.length, 1);
        assert.equal(features[0].properties.title, 'Unit 1');

        const since = Date.now() - 90_000;
        await caltopo.locations(CREDS, { since });

        assert.deepEqual(api.requests.map((r) => r.pathname), [
            '/api/v1/geodata/locations',
            '/api/v1/geodata/locations'
        ]);
        assert.deepEqual(api.requests[0].json, { bbox: [-180, -90, 180, 90], zoom: 8 });
        assert.deepEqual(api.requests[1].json, { bbox: [-180, -90, 180, 90], zoom: 8, since });
    } finally {
        await api.close();
    }
});

test('locations - rejected signature is a readable error', async () => {
    const api = await mock();

    try {
        const caltopo = new CalTopo({ url: api.base });
        await assert.rejects(caltopo.locations({ ...CREDS, CredentialSecret: Buffer.from('wrong').toString('base64') }), /HTTP 401/);
    } finally {
        await api.close();
    }
});

test('map - fetches the public map state', async () => {
    const api = await mock({
        map: [{ id: 'a', type: 'Feature', properties: { title: 'Marker', class: 'Marker', creator: 'x', updated: 1 }, geometry: { type: 'Point', coordinates: [1, 2] } }]
    });

    try {
        const caltopo = new CalTopo({ url: api.base });
        const features = await caltopo.map('SHARE1');

        assert.equal(api.requests[0].pathname, '/api/v1/map/SHARE1/since/-500');
        assert.equal(features.length, 1);
        assert.equal(features[0].properties.title, 'Marker');
    } finally {
        await api.close();
    }
});

test('createMap - signed form POST to the Team Account returns the new Map ID', async () => {
    const api = await mock({ mapId: 'ABC123' });

    try {
        const caltopo = new CalTopo({ url: api.base });

        const id = await caltopo.createMap('TEAM1', CREDS, {
            properties: {
                title: 'Lost Hiker',
                mode: MapMode.SAR,
                mapConfig: JSON.stringify({ activeLayers: [['mbt', 1]] }),
                sharing: MapSharing.SECRET
            },
            state: {
                type: 'FeatureCollection',
                features: [{
                    type: 'Feature',
                    geometry: { type: 'Point', coordinates: [-105, 39] },
                    properties: { title: 'Lost Hiker', 'marker-color': 'FF0000' }
                }]
            }
        });

        assert.equal(id, 'ABC123');
        assert.equal(caltopo.mapUrl(id), `${api.base}/m/ABC123`);

        assert.equal(api.requests.length, 1);
        assert.equal(api.requests[0].method, 'POST');
        assert.equal(api.requests[0].pathname, '/api/v1/acct/TEAM1/CollaborativeMap');
        assert.equal((api.requests[0].json?.properties as Record<string, unknown>).title, 'Lost Hiker');
        assert.equal((api.requests[0].json?.properties as Record<string, unknown>).sharing, 'SECRET');
    } finally {
        await api.close();
    }
});

test('createMap - rejected signature is a readable error', async () => {
    const api = await mock();

    try {
        const caltopo = new CalTopo({ url: api.base });
        await assert.rejects(caltopo.createMap('TEAM1', { ...CREDS, CredentialSecret: Buffer.from('wrong').toString('base64') }, {
            properties: { title: 'x', mode: MapMode.CAL, mapConfig: '{}', sharing: MapSharing.PRIVATE },
            state: { type: 'FeatureCollection', features: [] }
        }), /HTTP 401/);
    } finally {
        await api.close();
    }
});

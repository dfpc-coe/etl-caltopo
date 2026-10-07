import test from 'node:test';
import assert from 'node:assert';
import { createHmac } from 'node:crypto';
import CalTopo, { API } from '../lib/caltopo.js';
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

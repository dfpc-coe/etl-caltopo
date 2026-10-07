import test from 'node:test';
import assert from 'node:assert';
import { SchemaType, DataFlowType } from '@tak-ps/etl';
import type { Static } from '@sinclair/typebox';
import type { Feature } from '@tak-ps/node-cot';
import CalTopo, { LOCATION_TTL } from '../lib/caltopo.js';
import { mock, CREDS } from './mock.js';

// task.ts calls Task.init() at module scope which requires an ETL environment
process.env.ETL_API = process.env.ETL_API || 'http://localhost:5001';
process.env.ETL_LAYER = process.env.ETL_LAYER || '1';
process.env.ETL_TOKEN = process.env.ETL_TOKEN || 'etl.test-token';

const { default: Task } = await import('../task.js');

async function run(base: string, environment: Record<string, unknown>) {
    const task = await Task.init();

    const layer = {
        id: 1,
        connection: 1,
        task: 'etl-caltopo-v5.14.1',
        incoming: { environment, ephemeral: {} }
    };

    // @ts-expect-error partial layer for testing
    task.fetchLayer = async () => layer;
    // @ts-expect-error private in base
    task.layer = layer;

    task.client = (verbose: boolean) => new CalTopo({ url: base, verbose });

    let submitted: Static<typeof Feature.InputFeatureCollection> | null = null;
    task.submit = async (fc: Static<typeof Feature.InputFeatureCollection>) => {
        submitted = fc;
        return true;
    };

    await task.control();

    assert.ok(submitted);
    return submitted as Static<typeof Feature.InputFeatureCollection>;
}

test('Input schema - Team Account carries credentials & optional SinceDelta', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Input, DataFlowType.Incoming);

    const [map, team] = schema.properties.Source.anyOf;
    assert.deepEqual(Object.keys(map.properties), ['Mode', 'MapId']);
    assert.deepEqual(Object.keys(team.properties), ['Mode', 'AccountId', 'CredentialId', 'CredentialSecret', 'SinceDelta']);
    assert.deepEqual(team.required, ['Mode', 'AccountId', 'CredentialId', 'CredentialSecret']);
    assert.equal(team.properties.SinceDelta.minimum, 1);
});

test('fromLocations - current position, stale filter, callsign & course', async () => {
    const task = await Task.init();
    const now = Date.now();

    const features = task.fromLocations([
        // Track: last coordinate is the current position, 4th coord is time
        { id: 1, properties: { title: 'Unit 1', 'aircraft:heading': 89.6 }, geometry: { type: 'LineString', coordinates: [[-105, 39, 0, now - 60_000], [-104, 38, 10, now - 1000]] } },
        // Point with explicit updated, falls back to device for callsign
        { id: 'dev-2', properties: { device: 'Phone', updated: now - 5000, ttl: 60_000 }, geometry: { type: 'Point', coordinates: [-103, 37, 5, now - 5000] } },
        // Expired by its own ttl
        { id: 3, properties: { title: 'Old', updated: now - 120_000, ttl: 60_000 }, geometry: { type: 'Point', coordinates: [-102, 36] } },
        // Expired by the default ttl
        { id: 4, properties: { title: 'Older' }, geometry: { type: 'Point', coordinates: [-101, 35, 0, now - LOCATION_TTL - 1] } },
        // No usable geometry
        { id: 5, properties: { title: 'Nowhere' } },
    ]);

    assert.deepEqual(features.map((f) => f.id), ['caltopo-loc-1', 'caltopo-loc-dev-2']);

    assert.equal(features[0].properties.callsign, 'Unit 1');
    assert.equal(features[0].properties.type, 'a-f-G-U-C');
    assert.equal(features[0].properties.course, 90);
    assert.deepEqual(features[0].geometry.coordinates, [-104, 38, 10]);
    assert.equal(features[0].properties.time, new Date(now - 1000).toISOString());
    assert.equal(features[0].properties.stale, new Date(now - 1000 + LOCATION_TTL).toISOString());

    assert.equal(features[1].properties.callsign, 'Phone');
    assert.equal(features[1].properties.course, undefined);
    assert.equal(features[1].properties.stale, new Date(now + 55_000).toISOString());
});

test('fromMap - folders become paths, markers styled, geometry-less objects dropped', async () => {
    const task = await Task.init();

    const features = task.fromMap([
        { id: 'folder-1', type: 'Feature', properties: { title: 'Team A', class: 'Folder', creator: 'x', updated: 1 } },
        { id: 'marker-1', type: 'Feature', properties: { title: 'Base', description: 'ICP', class: 'Marker', creator: 'x', updated: 1, 'marker-color': 'FF0000', folderId: 'folder-1' }, geometry: { type: 'Point', coordinates: [-105, 39, 0, 123] } },
        { id: 'line-1', type: 'Feature', properties: { title: 'Route', class: 'Shape', creator: 'x', updated: 1, stroke: '#00FF00', 'stroke-width': 3 }, geometry: { type: 'LineString', coordinates: [[-105, 39, 0, 1], [-104, 38, 0, 2]] } },
        { id: 'op-1', type: 'Feature', properties: { title: 'OP 1', class: 'OperationalPeriod', creator: 'x', updated: 1 } },
    ]);

    assert.deepEqual(features.map((f) => f.id), ['marker-1', 'line-1']);

    const [marker, line] = features;
    assert.equal(marker.path, '/Team A');
    assert.equal(marker.properties.type, 'u-d-p');
    assert.equal(marker.properties.callsign, 'Base');
    assert.equal(marker.properties.remarks, 'ICP');
    assert.equal(marker.properties['marker-color'], '#FF0000');
    assert.equal(marker.properties['marker-opacity'], 1);
    assert.equal(marker.properties.archived, true);
    assert.deepEqual(marker.geometry.coordinates, [-105, 39, 0]);

    assert.equal(line.path, undefined);
    assert.equal(line.properties.stroke, '#00FF00');
    assert.equal(line.properties['stroke-width'], 3);
    assert.equal(line.properties.remarks, '');
    assert.deepEqual(line.geometry.coordinates, [[-105, 39, 0], [-104, 38, 0]]);
});

test('control - Team source without SinceDelta requests all locations', async () => {
    const api = await mock({
        locations: [{ id: 1, properties: { title: 'Unit 1' }, geometry: { type: 'Point', coordinates: [-105, 39, 0, Date.now()] } }]
    });

    try {
        const submitted = await run(api.base, {
            Source: { Mode: 'Team', AccountId: 'ACCT', ...CREDS },
            DEBUG: false
        });

        assert.equal(api.requests.length, 1);
        assert.deepEqual(api.requests[0].json, { bbox: [-180, -90, 180, 90], zoom: 8 });
        assert.deepEqual(submitted.features.map((f) => f.id), ['caltopo-loc-1']);
    } finally {
        await api.close();
    }
});

test('control - SinceDelta is sent as a ms epoch N seconds in the past', async () => {
    const api = await mock();

    try {
        const before = Date.now();
        const submitted = await run(api.base, {
            Source: { Mode: 'Team', AccountId: 'ACCT', ...CREDS, SinceDelta: 90 },
            DEBUG: false
        });
        const after = Date.now();

        assert.equal(api.requests.length, 1);
        const since = api.requests[0].json?.since;
        assert.equal(typeof since, 'number');
        assert.ok(since >= before - 90_000 && since <= after - 90_000, `since ${since} outside expected window`);
        assert.equal(submitted.features.length, 0);
    } finally {
        await api.close();
    }
});

test('control - Map source & legacy ShareId env fetch the public map', async () => {
    const api = await mock({
        map: [{ id: 'm', type: 'Feature', properties: { title: 'Marker', class: 'Marker', creator: 'x', updated: 1 }, geometry: { type: 'Point', coordinates: [1, 2] } }]
    });

    try {
        await run(api.base, { Source: { Mode: 'Map', MapId: 'SHARE1' }, DEBUG: false });
        const legacy = await run(api.base, { ShareId: 'SHARE2', DEBUG: false });

        assert.deepEqual(api.requests.map((r) => r.pathname), [
            '/api/v1/map/SHARE1/since/-500',
            '/api/v1/map/SHARE2/since/-500'
        ]);
        assert.deepEqual(legacy.features.map((f) => f.id), ['m']);
        assert.equal(legacy.features[0].properties.callsign, 'Marker');
    } finally {
        await api.close();
    }
});

type Fetched = { url: string, method: string, body: unknown };

type Current = { links?: unknown[], external_ids?: Record<string, string> };

/**
 * Run the outgoing handler against a mock CalTopo & a stubbed CloudTAK
 * - current: the Event CloudTAK returns on GET, undefined makes the GET fail
 * - patch: false makes the PATCH fail
 */
async function outgoing(base: string, environment: Record<string, unknown>, records: unknown[], opts: { current?: Current, patch?: boolean } = {}) {
    const task = await Task.init();

    const layer = {
        id: 1,
        connection: 1,
        task: 'etl-caltopo-v5.16.0',
        outgoing: { environment, ephemeral: {}, subscriptions: ['event:create'] }
    };

    // @ts-expect-error partial layer for testing
    task.fetchLayer = async () => layer;
    // @ts-expect-error private in base
    task.layer = layer;

    task.client = (verbose: boolean) => new CalTopo({ url: base, verbose });

    const fetched: Fetched[] = [];
    // @ts-expect-error loosely typed CloudTAK responses
    task.fetch = async (url: string, init: { method?: string, body?: string } = {}) => {
        const method = init.method ?? 'GET';
        fetched.push({ url, method, body: init.body ? JSON.parse(init.body) : undefined });

        if (method === 'PATCH') {
            if (opts.patch === false) throw new Error('403 Forbidden');
            return {};
        }

        if (!opts.current) throw new Error('403 Forbidden');
        return { ...EVENT, links: opts.current.links ?? [], external_ids: opts.current.external_ids ?? {} };
    };

    await task.outgoing({ Records: records.map((body) => ({ body: JSON.stringify(body) })) } as never);

    return { fetched };
}

const EVENT = {
    id: 'evt-1',
    name: 'Lost Hiker',
    type: '10031000000000000000',
    remarks: 'Last seen at the trailhead',
    location: 'Bear Lake Trailhead, CO',
    links: [],
    external_ids: {},
    geometry: { type: 'Point', coordinates: [-105.6, 40.3, 2900] }
};

const CREATE = { type: 'event', action: 'create', channels: [1], data: EVENT };

const OUTGOING = { AccountId: 'SVC1', TeamId: 'TEAM1', ...CREDS, MapMode: 'sar', MapSharing: 'SECRET', MapLayers: [{ layer: 'mbt' }], MarkerColor: 'FF0000', DEBUG: false };

test('Outgoing schema - Team Account, credentials & map defaults', async () => {
    const task = await Task.init();
    const schema = await task.schema(SchemaType.Input, DataFlowType.Outgoing);

    assert.deepEqual(Object.keys(schema.properties), ['AccountId', 'TeamId', 'CredentialId', 'CredentialSecret', 'MapMode', 'MapSharing', 'MapLayers', 'MarkerColor', 'DEBUG']);
    assert.deepEqual(schema.required, ['AccountId', 'TeamId', 'CredentialId', 'CredentialSecret', 'MapMode', 'MapSharing', 'MapLayers', 'MarkerColor', 'DEBUG']);
    assert.equal(schema.properties.MapMode.default, 'sar');
    assert.deepEqual(schema.properties.MapMode.enum, ['sar', 'cal']);
    assert.equal(schema.properties.MapSharing.default, 'SECRET');
    assert.deepEqual(schema.properties.MapSharing.enum, ['PRIVATE', 'SECRET', 'URL', 'PUBLIC']);
    assert.deepEqual(schema.properties.MapLayers.default, [{ layer: 'mbt' }]);
});

test('mapFromEvent - callsign titles the map & marker, remarks and location fill the description', async () => {
    const task = await Task.init();

    const map = task.mapFromEvent(EVENT, { ...OUTGOING, MapMode: 'cal', MapSharing: 'URL', MapLayers: [{ layer: 'mbh' }, { layer: 'imagery' }], MarkerColor: '#00FF00' } as never);

    assert.deepEqual(map.properties, {
        title: 'Lost Hiker',
        mode: 'cal',
        mapConfig: JSON.stringify({ activeLayers: [['mbh', 1], ['imagery', 1]] }),
        sharing: 'URL'
    });
    assert.equal(map.state.features.length, 1);
    assert.deepEqual(map.state.features[0].geometry, { type: 'Point', coordinates: [-105.6, 40.3] });
    assert.deepEqual(map.state.features[0].properties, {
        title: 'Lost Hiker',
        description: 'Last seen at the trailhead\n\nLocation: Bear Lake Trailhead, CO',
        'marker-symbol': 'point',
        'marker-color': '00FF00',
        'marker-size': '1'
    });

    const bare = task.mapFromEvent({ ...EVENT, remarks: '', location: '' }, OUTGOING as never);
    assert.equal(bare.state.features[0].properties.description, '');
});

test('outgoing - event:create creates a Team Map, files its ID under caltopo & links a shared Map', async () => {
    const api = await mock({ mapId: 'MAP1' });

    try {
        const res = await outgoing(api.base, OUTGOING, [CREATE], {
            current: { links: [{ name: 'Existing', url: 'https://example.com' }] }
        });

        assert.equal(api.requests.length, 1);
        assert.equal(api.requests[0].method, 'POST');
        assert.equal(api.requests[0].pathname, '/api/v1/acct/TEAM1/CollaborativeMap');
        assert.equal((api.requests[0].json?.properties as Record<string, unknown>).title, 'Lost Hiker');
        assert.equal((api.requests[0].json?.properties as Record<string, unknown>).sharing, 'SECRET');

        assert.deepEqual(res.fetched.map((f) => `${f.method} ${f.url}`), ['GET /api/core/event/evt-1', 'PATCH /api/core/event/evt-1']);
        assert.deepEqual(res.fetched[1].body, {
            external_id: { system: 'caltopo', value: 'MAP1' },
            links: [
                { name: 'Existing', url: 'https://example.com' },
                { name: 'CalTopo Map', url: `${api.base}/m/MAP1` }
            ]
        });
    } finally {
        await api.close();
    }
});

test('outgoing - URL & PUBLIC Maps are linked, a PRIVATE Map only files its ID', async () => {
    const api = await mock({ mapId: 'MAP2' });

    try {
        for (const sharing of ['URL', 'PUBLIC']) {
            const res = await outgoing(api.base, { ...OUTGOING, MapSharing: sharing }, [CREATE], { current: {} });
            assert.deepEqual(res.fetched[1].body, {
                external_id: { system: 'caltopo', value: 'MAP2' },
                links: [{ name: 'CalTopo Map', url: `${api.base}/m/MAP2` }]
            });
        }

        const res = await outgoing(api.base, { ...OUTGOING, MapSharing: 'PRIVATE' }, [CREATE], { current: {} });
        assert.deepEqual(res.fetched[1].body, { external_id: { system: 'caltopo', value: 'MAP2' } });

        assert.equal(api.requests.length, 3);
    } finally {
        await api.close();
    }
});

test('outgoing - an Event that already has a caltopo external ID never gets a second Map', async () => {
    const api = await mock();

    try {
        const res = await outgoing(api.base, OUTGOING, [CREATE], { current: { external_ids: { caltopo: 'OLD' } } });

        assert.equal(api.requests.length, 0);
        assert.deepEqual(res.fetched.map((f) => `${f.method} ${f.url}`), ['GET /api/core/event/evt-1']);
    } finally {
        await api.close();
    }
});

test('outgoing - updates, deletes & other resources never create a map', async () => {
    const api = await mock();

    try {
        const res = await outgoing(api.base, OUTGOING, [
            { type: 'event', action: 'update', channels: [1], data: { ...EVENT, id: 'evt-2' } },
            { type: 'event', action: 'delete', channels: [1], data: { ...EVENT, id: 'evt-3' } },
            { type: 'board:event', action: 'create', channels: [1], data: { id: 'p', board: 'b', event: { ...EVENT, id: 'evt-4' } } },
            { type: 'feature', xml: '<event/>', geojson: { id: 'f', type: 'Feature', path: '/', properties: { callsign: 'x', type: 'a-f-G', how: 'm-g', time: '2026-01-01T00:00:00Z', start: '2026-01-01T00:00:00Z', stale: '2026-01-01T00:00:00Z', center: [0, 0] }, geometry: { type: 'Point', coordinates: [0, 0] } } }
        ], { current: {} });

        assert.equal(api.requests.length, 0);
        assert.equal(res.fetched.length, 0);
    } finally {
        await api.close();
    }
});

test('outgoing - an unreadable Event fails before a Map is created, a failed record never fails the Map', async () => {
    const api = await mock({ mapId: 'MAP3' });

    try {
        await assert.rejects(outgoing(api.base, OUTGOING, [CREATE]), /requires the event:read permission/);
        assert.equal(api.requests.length, 0);

        const res = await outgoing(api.base, OUTGOING, [CREATE], { current: {}, patch: false });
        assert.equal(api.requests.length, 1);
        assert.deepEqual(res.fetched.map((f) => f.method), ['GET', 'PATCH']);
    } finally {
        await api.close();
    }
});

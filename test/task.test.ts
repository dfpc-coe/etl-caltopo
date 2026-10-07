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

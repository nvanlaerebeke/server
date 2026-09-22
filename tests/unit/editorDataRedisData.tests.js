const {describe, test, expect, beforeAll, afterAll} = require('@jest/globals');
const path = require('path');
const {RedisMemoryServer} = require('../../DocService/node_modules/redis-memory-server');

describe('editorDataRedis complete editor-data contract', () => {
  let redisServer;
  let EditorData;
  let EditorStat;
  let stores;
  let stats;
  const ctx = {
    tenant: 'extended-contract',
    getCfg(_path, fallback) {
      return fallback;
    }
  };

  beforeAll(async () => {
    let host = process.env.TEST_REDIS_HOST;
    let port = Number(process.env.TEST_REDIS_PORT || 6379);
    if (!host) {
      redisServer = new RedisMemoryServer();
      host = await redisServer.getHost();
      port = await redisServer.getPort();
    }
    process.env.NODE_CONFIG_DIR = process.env.EO_CONFIG_DIR || path.join(__dirname, '..', '..', 'Common', 'config');
    process.env.NODE_CONFIG = JSON.stringify({
      log: {options: {replaceConsole: false}},
      services: {
        CoAuthoring: {
          redis: {host, port, prefix: `extended-contract:${process.pid}:`}
        }
      }
    });
    ({EditorData, EditorStat} = require('../../DocService/sources/editorDataRedis'));
  }, 30000);

  beforeAll(async () => {
    stores = [new EditorData(), new EditorData()];
    stats = [new EditorStat(), new EditorStat()];
    await Promise.all([...stores, ...stats].map(store => store.connect()));
  });

  afterAll(async () => {
    await Promise.all([...stores, ...stats].map(store => store.close()));
    if (redisServer) {
      await redisServer.stop();
    }
  });

  test('shares locks, object locks, messages, saved state, and force-save state across replicas', async () => {
    expect(await stores[0].lockSave(ctx, 'document', 'owner-a', 5)).toBe(true);
    expect(await stores[1].lockSave(ctx, 'document', 'owner-b', 5)).toBe(false);
    expect(await stores[1].unlockSave(ctx, 'document', 'owner-b')).toBe(0);
    expect(await stores[0].unlockSave(ctx, 'document', 'owner-a')).toBe(1);

    const [left, right] = await Promise.all([
      stores[0].addLocksNX(ctx, 'document', {a: {owner: 1}, b: {owner: 1}}),
      stores[1].addLocksNX(ctx, 'document', {b: {owner: 2}, c: {owner: 2}})
    ]);
    expect(Object.keys(await stores[0].getLocks(ctx, 'document')).sort()).toEqual(['a', 'b', 'c']);
    expect(Number(Object.hasOwn(left.lockConflict, 'b')) + Number(Object.hasOwn(right.lockConflict, 'b'))).toBe(1);

    await stores[0].addMessage(ctx, 'document', {message: 'hello'});
    expect(await stores[1].getMessages(ctx, 'document')).toEqual([{message: 'hello'}]);
    await stores[0].setSaved(ctx, 'document', 'saved');
    expect(await stores[1].getdelSaved(ctx, 'document')).toBe('saved');
    expect(await stores[0].getdelSaved(ctx, 'document')).toBeNull();

    await stores[0].setForceSave(ctx, 'document', 10, 1, 'https://example.test', {user: 'a'}, null);
    const started = await stores[1].checkAndStartForceSave(ctx, 'document');
    expect(started).toMatchObject({time: 10, index: 1, started: true});
    expect(await stores[0].checkAndStartForceSave(ctx, 'document')).toBeUndefined();
    const updated = await stores[0].checkAndSetForceSave(ctx, 'document', 10, 1, false, true, {result: true});
    expect(updated).toMatchObject({ended: true, convertInfo: {result: true}});
  });

  test('keeps one-shot saved reads and timer claims atomic across replicas', async () => {
    const docId = 'atomic-state';
    await stores[0].setSaved(ctx, docId, 'saved-once');
    const saved = await Promise.all(Array.from({length: 20}, (_, index) => stores[index % stores.length].getdelSaved(ctx, docId)));
    expect(saved.filter(value => value === 'saved-once')).toHaveLength(1);
    expect(saved.filter(value => value === null)).toHaveLength(19);

    await stores[0].addForceSaveTimerNX(ctx, docId, 100);
    await stores[1].addForceSaveTimerNX(ctx, docId, 1);
    const claims = await Promise.all([stores[0].getForceSaveTimer(101), stores[1].getForceSaveTimer(101)]);
    expect(claims.flat()).toEqual([[ctx.tenant, docId]]);
    expect(await stores[0].getForceSaveTimer(101)).toEqual([]);
  });

  test('does not remove a newer object lock when an older value is released', async () => {
    const docId = 'object-lock-generation';
    await stores[0].addLocks(ctx, docId, {object: {owner: 'old'}});
    await stores[1].addLocks(ctx, docId, {object: {owner: 'new'}});
    await stores[0].removeLocks(ctx, docId, {object: {owner: 'old'}});
    expect(await stores[1].getLocks(ctx, docId)).toEqual({object: {owner: 'new'}});
  });

  test('serializes concurrent save-lock, force-save and message operations', async () => {
    const lockDoc = 'concurrent-locks';
    const lockResults = await Promise.all(
      Array.from({length: 50}, (_, index) => stores[index % stores.length].lockSave(ctx, lockDoc, `owner-${index}`, 5))
    );
    expect(lockResults.filter(Boolean)).toHaveLength(1);
    const owner = `owner-${lockResults.findIndex(Boolean)}`;
    expect(await stores[0].unlockSave(ctx, lockDoc, owner)).toBe(1);

    const forceDoc = 'concurrent-force-save';
    await stores[0].setForceSave(ctx, forceDoc, 1, 1, 'https://example.test', {}, null);
    const forceResults = await Promise.all(
      Array.from({length: 50}, (_, index) => stores[index % stores.length].checkAndStartForceSave(ctx, forceDoc))
    );
    expect(forceResults.filter(Boolean)).toHaveLength(1);

    const messageDoc = 'concurrent-messages';
    await Promise.all(Array.from({length: 50}, (_, index) => stores[index % stores.length].addMessage(ctx, messageDoc, {index})));
    const messages = await stores[0].getMessages(ctx, messageDoc);
    expect(messages).toHaveLength(50);
    expect(new Set(messages.map(message => message.index))).toEqual(new Set(Array.from({length: 50}, (_, index) => index)));
  });

  test('isolates document data by tenant', async () => {
    const other = {...ctx, tenant: 'extended-contract-other'};
    const docId = 'tenant-isolation';
    await stores[0].addMessage(ctx, docId, {tenant: ctx.tenant});
    await stores[0].addLocks(ctx, docId, {lock: {tenant: ctx.tenant}});

    expect(await stores[1].getMessages(other, docId)).toEqual([]);
    expect(await stores[1].getLocks(other, docId)).toEqual({});
    expect(await stores[1].getMessages(ctx, docId)).toEqual([{tenant: ctx.tenant}]);
  });

  test('keeps presence distributed and does not delete it during data cleanup', async () => {
    const info = JSON.stringify({id: 'user-1', connectionId: 'connection-1', view: false});
    await stores[0].addPresence(ctx, 'presence-document', 'connection-1', info);
    expect(await stores[1].getPresence(ctx, 'presence-document', [])).toEqual([info]);

    await stores[0].addMessage(ctx, 'presence-document', {value: 1});
    await stores[0].cleanDocumentOnExit(ctx, 'presence-document');
    expect(await stores[1].getPresence(ctx, 'presence-document', [])).toEqual([info]);
    expect(await stores[1].getMessages(ctx, 'presence-document')).toEqual([]);

    await stores[1].removePresence(ctx, 'presence-document', 'connection-1');
    expect(await stores[0].updatePresence(ctx, 'presence-document', 'connection-1')).toBe(false);
    await stores[0].removePresenceDocument(ctx, 'presence-document');
  });

  test('implements unique-user, monthly-user, connection, shard and notification statistics', async () => {
    await stats[0].addPresenceUniqueUser(ctx, 'editor', 200, {anonym: true});
    await stats[1].addPresenceUniqueViewUser(ctx, 'viewer', 200, {anonym: false});
    expect((await stats[1].getPresenceUniqueUser(ctx, 100))[0]).toMatchObject({userid: 'editor', expire: new Date(200000)});
    expect((await stats[0].getPresenceUniqueViewUser(ctx, 100))[0].userid).toBe('viewer');
    expect(await stats[0].getPresenceUniqueUser(ctx, 200)).toEqual([]);

    const period = Date.UTC(2026, 0, 1);
    await stats[0].addPresenceUniqueUsersOfMonth(ctx, 'editor', period, {firstOpenDate: 'edit'});
    await stats[1].addPresenceUniqueViewUsersOfMonth(ctx, 'viewer', period, {firstOpenDate: 'view'});
    expect(await stats[0].getPresenceUniqueUsersOfMonth(ctx)).toEqual({
      '2026-01-01T00:00:00.000Z': {editor: {firstOpenDate: 'edit'}}
    });
    expect(await stats[1].getPresenceUniqueViewUsersOfMonth(ctx)).toEqual({
      '2026-01-01T00:00:00.000Z': {viewer: {firstOpenDate: 'view'}}
    });

    const now = Date.now();
    await stats[0].setEditorConnections(ctx, 1, 2, 3, now, [{val: 1000}]);
    expect(await stats[1].getEditorConnections(ctx)).toEqual([{time: now, edit: 1, liveview: 2, view: 3}]);
    await stats[0].setEditorConnectionsCountByShard(ctx, 'one', 2);
    await stats[1].incrEditorConnectionsCountByShard(ctx, 'one', 3);
    expect(await stats[0].getEditorConnectionsCount(ctx, [])).toBe(5);
    expect(await stats[0].lockNotification(ctx, 'notification', 5)).toBe(true);
    expect(await stats[1].lockNotification(ctx, 'notification', 5)).toBe(false);
  });
});

const {describe, test, expect, beforeAll, afterAll, jest} = require('@jest/globals');
const path = require('path');
const {RedisMemoryServer} = require('../../DocService/node_modules/redis-memory-server');

describe('editorDataRedis statistics contract', () => {
  let redisServer;
  let EditorStat;
  let stats;
  const context = {tenant: 'statistics-contract'};

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
          redis: {host, port, prefix: `statistics-contract:${process.pid}:`}
        }
      }
    });
    ({EditorStat} = require('../../DocService/sources/editorDataRedis'));
    stats = [new EditorStat(), new EditorStat()];
    await Promise.all(stats.map(stat => stat.connect()));
  }, 30000);

  afterAll(async () => {
    if (stats) {
      await Promise.all(stats.map(stat => stat.close()));
    }
    if (redisServer) {
      await redisServer.stop();
    }
  });

  test('exposes the complete EditorStat interface with matching signatures', () => {
    const memory = require('../../DocService/sources/editorDataMemory');
    const publicMethods = instance => {
      const methods = new Set();
      let prototype = Object.getPrototypeOf(instance);
      while (prototype && prototype !== Object.prototype) {
        for (const name of Object.getOwnPropertyNames(prototype)) {
          if (name !== 'constructor' && !name.startsWith('_') && typeof prototype[name] === 'function') {
            methods.add(name);
          }
        }
        prototype = Object.getPrototypeOf(prototype);
      }
      return [...methods].sort();
    };

    const redisMethods = publicMethods(stats[0]);
    const memoryMethods = publicMethods(new memory.EditorStat());
    expect(redisMethods).toEqual(memoryMethods);
    for (const method of memoryMethods) {
      expect(stats[0][method].length).toBe(new memory.EditorStat()[method].length);
    }
  });

  test('shares unique-user and monthly-user state across replicas', async () => {
    await stats[0].addPresenceUniqueUser(context, 'editor', 200, {anonym: true});
    await stats[1].addPresenceUniqueViewUser(context, 'viewer', 200, {anonym: false});
    expect(await stats[1].getPresenceUniqueUser(context, 100)).toEqual([{userid: 'editor', expire: new Date(200000), anonym: true}]);
    expect(await stats[0].getPresenceUniqueViewUser(context, 100)).toEqual([{userid: 'viewer', expire: new Date(200000), anonym: false}]);
    expect(await stats[0].getPresenceUniqueUser(context, 200)).toEqual([]);

    const period = Date.UTC(2026, 0, 1);
    await stats[0].addPresenceUniqueUsersOfMonth(context, 'editor', period, {version: 1});
    await stats[1].addPresenceUniqueUsersOfMonth(context, 'editor', period, {version: 2});
    await stats[1].addPresenceUniqueViewUsersOfMonth(context, 'viewer', period, {version: 1});
    expect(await stats[0].getPresenceUniqueUsersOfMonth(context)).toEqual({
      '2026-01-01T00:00:00.000Z': {editor: {version: 2}}
    });
    expect(await stats[0].getPresenceUniqueViewUsersOfMonth(context)).toEqual({
      '2026-01-01T00:00:00.000Z': {viewer: {version: 1}}
    });
  });

  test('stores connection samples and aggregates shard counts atomically', async () => {
    const now = Date.now();
    await stats[0].setEditorConnections(context, 1, 2, 3, now - 2000, [{val: 1000}]);
    await stats[1].setEditorConnections(context, 4, 5, 6, now, [{val: 1000}]);
    expect(await stats[0].getEditorConnections(context)).toEqual([{time: now, edit: 4, liveview: 5, view: 6}]);

    await stats[0].setEditorConnectionsCountByShard(context, 'a', 2);
    await stats[1].setEditorConnectionsCountByShard(context, 'b', 3);
    await stats[0].incrEditorConnectionsCountByShard(context, 'a', 1);
    expect(await stats[1].getEditorConnectionsCount(context, [])).toBe(6);

    await stats[0].setViewerConnectionsCountByShard(context, 'a', 7);
    await stats[0].setLiveViewerConnectionsCountByShard(context, 'a', 11);
    expect(await stats[1].getViewerConnectionsCount(context, [])).toBe(7);
    expect(await stats[1].getLiveViewerConnectionsCount(context, [])).toBe(11);
    expect(await stats[1].getEditorConnectionsCount({tenant: 'other-tenant'}, [])).toBe(0);
  });

  test('coordinates notification, shutdown and license state across replicas', async () => {
    expect(await stats[0].lockNotification(context, 'notification', 1)).toBe(true);
    expect(await stats[1].lockNotification(context, 'notification', 1)).toBe(false);
    await new Promise(resolve => setTimeout(resolve, 1100));
    expect(await stats[1].lockNotification(context, 'notification', 1)).toBe(true);

    const shutdownKey = `statistics-contract:${process.pid}:shutdown`;
    await Promise.all(['a', 'b', 'b', 'c'].map(docId => stats[0].addShutdown(shutdownKey, docId)));
    expect(await stats[1].getShutdownCount(shutdownKey)).toBe(3);
    await stats[1].removeShutdown(shutdownKey, 'b');
    expect(await stats[0].getShutdownCount(shutdownKey)).toBe(2);
    await stats[0].cleanupShutdown(shutdownKey);
    expect(await stats[1].getShutdownCount(shutdownKey)).toBe(0);

    const licenseKey = `statistics-contract:${process.pid}:license`;
    await stats[0].setLicense(licenseKey, 'license-value');
    expect(await stats[1].getLicense(licenseKey)).toBe('license-value');

    const proxy = new EditorStat(1);
    await proxy.connect();
    try {
      await proxy.setLicense(licenseKey, 'proxy-license');
      expect(await proxy.getLicense(licenseKey)).toBe('proxy-license');
      expect(await stats[0].getLicense(licenseKey)).toBe('license-value');
      await proxy.deleteKey(licenseKey);
      expect(await proxy.getLicense(licenseKey)).toBeNull();
      expect(await stats[0].getLicense(licenseKey)).toBe('license-value');
    } finally {
      await proxy.close();
    }
  });

  test('falls back to the memory statistics store when Redis statistics fail', async () => {
    const stat = stats[0];
    const original = stat._store.setEditorConnections;
    const fallback = jest.spyOn(stat._memory, 'setEditorConnections');
    stat._store.setEditorConnections = async () => {
      throw new Error('simulated Redis statistics error');
    };

    try {
      await expect(stat.setEditorConnections(context, 1, 2, 3, Date.now(), [{val: 1000}])).resolves.toBeUndefined();
      expect(fallback).toHaveBeenCalledWith(context, 1, 2, 3, expect.any(Number), [{val: 1000}]);
    } finally {
      stat._store.setEditorConnections = original;
      fallback.mockRestore();
    }
  });
});

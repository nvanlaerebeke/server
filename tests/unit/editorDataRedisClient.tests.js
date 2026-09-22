const {describe, test, expect} = require('@jest/globals');
const {EventEmitter} = require('node:events');
const {connectRedisClient, createRedisClient, sendCommand} = require('../../DocService/sources/editorDataRedisClient');

describe('editorDataRedisClient', () => {
  function close(client) {
    if (client.isOpen) {
      client.destroy();
    }
  }

  function standaloneCfg(extra = {}) {
    return Object.assign({host: '127.0.0.1', port: 6379, options: {commandTimeout: 300}}, extra);
  }

  test('selects standalone mode by default', () => {
    const client = createRedisClient(standaloneCfg());
    try {
      expect(client.__editorDataTopology).toBe('standalone');
      expect(client.__editorDataOptions.socket).toMatchObject({host: '127.0.0.1', port: 6379});
      expect(client.__editorDataOptions.disableOfflineQueue).toBe(true);
    } finally {
      close(client);
    }
  });

  test('keeps an explicit command timeout', () => {
    const client = createRedisClient(standaloneCfg({options: {commandTimeout: 1000}}));
    try {
      expect(client.__editorDataCommandTimeout).toBe(1000);
    } finally {
      close(client);
    }
  });

  test('selects sentinel mode only for an explicit or credible sentinel configuration', () => {
    const fabricated = createRedisClient(standaloneCfg({options: {sentinels: [{host: '127.0.0.1', port: 6379}], name: 'mymaster'}}));
    const credible = createRedisClient(
      standaloneCfg({
        options: {
          sentinels: [
            {host: 'sentinel-a', port: 26379},
            {host: 'sentinel-b', port: 26379}
          ],
          name: 'mymaster'
        }
      })
    );
    const explicit = createRedisClient(
      standaloneCfg({mode: 'sentinel', options: {sentinels: [{host: 'sentinel-a', port: 26379}], name: 'mymaster'}})
    );
    try {
      expect(fabricated.__editorDataTopology).toBe('standalone');
      expect(credible.__editorDataTopology).toBe('sentinel');
      expect(explicit.__editorDataTopology).toBe('sentinel');
    } finally {
      close(fabricated);
      close(credible);
      close(explicit);
    }
  });

  test('rejects sentinel mode without sentinel nodes', () => {
    expect(() => createRedisClient(standaloneCfg({mode: 'sentinel'}))).toThrow(/at least one sentinel/);
  });

  test('rejects an unknown mode', () => {
    expect(() => createRedisClient(standaloneCfg({mode: 'Sentinel'}))).toThrow(/expected one of/);
  });

  test('selects cluster mode and normalizes root nodes', () => {
    const client = createRedisClient(
      standaloneCfg({
        optionsCluster: {
          rootNodes: [{url: 'redis://valkey-0:6379'}, 'valkey-1:6380'],
          defaults: {username: 'cluster-user', password: 'cluster-pass'}
        }
      })
    );
    try {
      expect(client.__editorDataTopology).toBe('cluster');
      expect(client.__editorDataOptions.disableOfflineQueue).toBe(true);
      expect(client.__editorDataClusterOptions.rootNodes).toHaveLength(2);
    } finally {
      close(client);
    }
  });

  test('rejects a non-zero logical database in cluster mode', () => {
    expect(() =>
      createRedisClient(
        standaloneCfg({
          options: {db: 3},
          optionsCluster: {rootNodes: [{url: 'redis://valkey-0:6379'}]}
        })
      )
    ).toThrow(/database 0 only/);
  });

  test('rejects cluster mode without root nodes', () => {
    expect(() => createRedisClient(standaloneCfg({mode: 'cluster'}))).toThrow(/root node/i);
  });

  test('normalizes node-redis command arguments and passes the command timeout', async () => {
    const calls = [];
    const client = {
      __editorDataTopology: 'standalone',
      sendCommand(args, options) {
        calls.push([args, options]);
        return Promise.resolve('OK');
      }
    };

    await expect(sendCommand(client, [Buffer.from('PING'), 42], 123)).resolves.toBe('OK');
    expect(calls).toEqual([[['PING', '42'], {timeout: 123}]]);
  });

  test('routes cluster EVAL commands by their first key', async () => {
    const calls = [];
    const client = {
      __editorDataTopology: 'cluster',
      sendCommand(...args) {
        calls.push(args);
        return Promise.resolve('OK');
      }
    };

    await sendCommand(client, ['EVAL', 'return 1', 2, 'first-key', 'second-key'], 456);
    expect(calls).toEqual([['first-key', false, ['EVAL', 'return 1', '2', 'first-key', 'second-key'], {timeout: 456}]]);
  });

  test('uses the sentinel sendCommand signature', async () => {
    const calls = [];
    const client = {
      __editorDataTopology: 'sentinel',
      sendCommand(...args) {
        calls.push(args);
        return Promise.resolve('PONG');
      }
    };

    await expect(sendCommand(client, ['PING'], 321)).resolves.toBe('PONG');
    expect(calls).toEqual([[false, ['PING'], {timeout: 321}]]);
  });

  test('waits for a node-redis client that is open but not ready', async () => {
    const client = new EventEmitter();
    client.isOpen = true;
    client.isReady = false;
    setImmediate(() => {
      client.isReady = true;
      client.emit('ready');
    });

    await connectRedisClient(client);
    expect(client.isReady).toBe(true);
  });
});

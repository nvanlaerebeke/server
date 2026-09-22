'use strict';

const redis = require('redis');

// Redis operations that decide whether a save may proceed must not wait
// indefinitely for a dead connection. The caller receives a denial and can
// retry instead of acting on a command that may be replayed after reconnect.
const DEFAULT_COMMAND_TIMEOUT = 300;
const CONNECT_TIMEOUT = 3000;

const MODES = ['auto', 'standalone', 'sentinel', 'cluster'];

function clone(value) {
  return value ? JSON.parse(JSON.stringify(value)) : {};
}

function nodeOptions(redisCfg, source, database) {
  const options = clone(source);
  if (options.user !== undefined && options.username === undefined) {
    options.username = options.user;
  }
  options.disableOfflineQueue = true;
  if (options.db !== undefined && options.database === undefined) {
    options.database = options.db;
  }
  delete options.db;
  if (database !== undefined) {
    options.database = Number(database);
  }
  if (!options.url) {
    options.socket = Object.assign({}, options.socket, {
      host: options.socket?.host || redisCfg.host,
      port: Number(options.socket?.port || redisCfg.port)
    });
  }
  return options;
}

function nodeAddress(node) {
  if (node && node.host !== undefined) {
    return {host: node.host, port: Number(node.port) || 6379};
  }
  const raw = typeof node === 'string' ? node : node && node.url;
  if (!raw) {
    throw new Error(`Redis node is neither a URL nor a host/port pair: ${JSON.stringify(node)}`);
  }
  const url = new URL(raw.includes('://') ? raw : `redis://${raw}`);
  return {host: url.hostname, port: Number(url.port) || 6379};
}

function rootNodes(nodes) {
  return nodes.map(node => {
    if (node && typeof node === 'object' && node.url) {
      return {url: String(node.url)};
    }
    if (typeof node === 'string' && node.includes('://')) {
      return {url: node};
    }
    const address = nodeAddress(node);
    return {url: `redis://${address.host}:${address.port}`};
  });
}

function sentinelNodes(nodes) {
  return nodes.map(nodeAddress);
}

function fabricatedSentinel(redisCfg, sentinels) {
  return 1 === sentinels.length && sentinels[0].host === redisCfg.host && Number(sentinels[0].port) === Number(redisCfg.port);
}

function withTimeout(promise, timeout, description) {
  let timer;
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(() => {
      const error = new Error(`${description} timed out after ${timeout}ms`);
      error.code = 'ETIMEDOUT';
      reject(error);
    }, timeout);
  });
  return Promise.race([promise, deadline]).finally(() => clearTimeout(timer));
}

function topology(client) {
  return client.__editorDataTopology || 'standalone';
}

function firstKey(args) {
  const command = String(args[0] || '').toUpperCase();
  if (command === 'EVAL' || command === 'EVALSHA' || command === 'EVAL_RO' || command === 'EVALSHA_RO') {
    return args[3];
  }
  if (command === 'PING' || command === 'INFO') {
    return undefined;
  }
  return args[1];
}

function sendCommand(client, args, timeout = DEFAULT_COMMAND_TIMEOUT) {
  const normalized = args.map(value => String(value));
  const commandOptions = {timeout};
  let promise;
  switch (topology(client)) {
    case 'cluster':
      promise = client.sendCommand(firstKey(normalized), false, normalized, commandOptions);
      break;
    case 'sentinel':
      promise = client.sendCommand(false, normalized, commandOptions);
      break;
    default:
      promise = client.sendCommand(normalized, commandOptions);
      break;
  }
  return withTimeout(promise, timeout, `Redis ${normalized[0] || 'command'}`);
}

function defineScript(client, name, script, numberOfKeys) {
  client[name] = (...args) => {
    const keys = args.slice(0, numberOfKeys);
    const values = args.slice(numberOfKeys);
    return sendCommand(client, ['EVAL', script, numberOfKeys, ...keys, ...values]);
  };
}

function evalScript(client, script, keys, args) {
  return sendCommand(client, ['EVAL', script, keys.length, ...keys, ...args]);
}

function createRedisClient(redisCfg, database) {
  const options = Object.assign({}, redisCfg.options);
  const clusterCfg = redisCfg.optionsCluster || {};
  const configuredNodes = clusterCfg.rootNodes || [];
  const sentinels = options.sentinels || [];
  const mode = redisCfg.mode || 'auto';

  if (!MODES.includes(mode)) {
    throw new Error(`services.CoAuthoring.redis.mode is "${mode}"; expected one of ${MODES.join(', ')}`);
  }
  if (mode === 'cluster' && configuredNodes.length === 0) {
    throw new Error('Redis cluster mode requires at least one root node');
  }
  if (mode === 'sentinel' && sentinels.length === 0) {
    throw new Error('Redis sentinel mode requires at least one sentinel');
  }

  let client;
  let clientTopology = 'standalone';
  if (mode === 'cluster' || (mode === 'auto' && configuredNodes.length > 0)) {
    if (configuredNodes.length === 0) {
      throw new Error('Redis cluster mode requires at least one root node');
    }
    const defaults = nodeOptions(redisCfg, Object.assign({}, clusterCfg.defaults, options), database);
    if (defaults.database !== undefined && Number(defaults.database) !== 0) {
      throw new Error('Redis Cluster supports database 0 only');
    }
    delete defaults.database;
    if (defaults.socket) {
      delete defaults.socket.host;
      delete defaults.socket.port;
      if (Object.keys(defaults.socket).length === 0) {
        delete defaults.socket;
      }
    }
    client = redis.createCluster({
      rootNodes: rootNodes(configuredNodes),
      defaults,
      minimizeConnections: clusterCfg.minimizeConnections,
      useReplicas: clusterCfg.useReplicas
    });
    clientTopology = 'cluster';
  } else if (mode === 'sentinel' || (mode === 'auto' && sentinels.length > 0 && !fabricatedSentinel(redisCfg, sentinelNodes(sentinels)))) {
    const sentinelOptions = nodeOptions(redisCfg, options, database);
    const sentinelName = options.name || redisCfg.sentinelName || 'mymaster';
    client = redis.createSentinel({
      name: sentinelName,
      sentinelRootNodes: sentinelNodes(sentinels),
      nodeClientOptions: sentinelOptions,
      sentinelClientOptions: sentinelOptions,
      passthroughClientErrorEvents: true
    });
    clientTopology = 'sentinel';
  } else {
    client = redis.createClient(nodeOptions(redisCfg, options, database));
  }

  client.__editorDataTopology = clientTopology;
  client.__editorDataCommandTimeout = Number(options.commandTimeout) || DEFAULT_COMMAND_TIMEOUT;
  client.__editorDataOptions = nodeOptions(redisCfg, options);
  client.__editorDataClusterOptions = clusterCfg;
  return client;
}

async function connectRedisClient(client) {
  if (client.isReady) {
    return;
  }
  if (client.isOpen) {
    await withTimeout(new Promise(resolve => client.once('ready', resolve)), CONNECT_TIMEOUT, 'Redis ready');
    return;
  }
  await withTimeout(client.connect(), CONNECT_TIMEOUT, 'Redis connect');
}

module.exports = {
  CONNECT_TIMEOUT,
  DEFAULT_COMMAND_TIMEOUT,
  createRedisClient,
  connectRedisClient,
  defineScript,
  evalScript,
  sendCommand,
  topology,
  withTimeout
};

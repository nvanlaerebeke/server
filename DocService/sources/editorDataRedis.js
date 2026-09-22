'use strict';

const config = require('config');
const editorDataMemory = require('./editorDataMemory');
const {createRedisClient, connectRedisClient, sendCommand, withTimeout} = require('./editorDataRedisClient');
const {createFailureReporter} = require('./editorDataRedisReport');
const {createPresenceStore} = require('./editorDataRedisPresence');
const {createSaveLockStore} = require('./editorDataRedisSaveLock');
const {createEditorDataStore} = require('./editorDataRedisData');
const {createEditorStatStore} = require('./editorDataRedisStat');

const CONNECT_TIMEOUT = 3000;

function redisConfig() {
  return config.get('services.CoAuthoring.redis');
}

function installConnectionReporting(client, name) {
  const report = createFailureReporter(name);
  client.on('error', error => report.failure(null, 'connection', error));
  client.on('ready', () => report.success(null));
  client.on('reconnecting', details => report.failure(null, 'reconnect', details || new Error('Redis reconnecting')));
  return report;
}

async function closeClient(client) {
  if (!client || !client.isOpen) {
    return;
  }
  try {
    await withTimeout(client.quit(), 5000, 'Redis quit');
  } catch {
    if (typeof client.destroy === 'function') {
      client.destroy();
    } else if (typeof client.disconnect === 'function') {
      client.disconnect();
    }
  }
}

function EditorData() {
  this._memory = new editorDataMemory.EditorData();
  this._redis = createRedisClient(redisConfig());
  installConnectionReporting(this._redis, 'editorDataRedis.connection');

  const prefix = redisConfig().prefix || 'ds:';
  this._saveLock = createSaveLockStore(this._redis, prefix);
  this._presence = createPresenceStore(this._redis, prefix, Number(config.get('services.CoAuthoring.expire.presence')), this._memory);
  this._data = createEditorDataStore(this._redis, prefix);
}

EditorData.prototype.connect = async function () {
  await this._memory.connect();
  try {
    await withTimeout(connectRedisClient(this._redis), CONNECT_TIMEOUT, 'Redis connect');
  } catch (error) {
    this._connectionError = error;
  }
};
EditorData.prototype.isConnected = function () {
  return Boolean(this._redis.isReady) && this._memory.isConnected();
};
EditorData.prototype.ping = async function () {
  await connectRedisClient(this._redis);
  return sendCommand(this._redis, ['PING']);
};
EditorData.prototype.close = async function () {
  await closeClient(this._redis);
  await this._memory.close();
};
EditorData.prototype.healthCheck = async function () {
  if (!this.isConnected()) {
    return false;
  }
  try {
    await this.ping();
    return true;
  } catch {
    return false;
  }
};

const ROUTES = {
  _saveLock: ['lockSave', 'unlockSave', 'lockAuth', 'unlockAuth'],
  _presence: ['addPresence', 'updatePresence', 'removePresence', 'getPresence', 'getDocumentPresenceExpired', 'removePresenceDocument'],
  _data: [
    'addLocks',
    'addLocksNX',
    'removeLocks',
    'removeAllLocks',
    'getLocks',
    'addMessage',
    'removeMessages',
    'getMessages',
    'setSaved',
    'getdelSaved',
    'setForceSave',
    'getForceSave',
    'checkAndStartForceSave',
    'checkAndSetForceSave',
    'removeForceSave',
    'addForceSaveTimerNX',
    'getForceSaveTimer'
  ]
};

const EXPLICIT = ['cleanDocumentOnExit'];

for (const method of ROUTES._saveLock) {
  const delegate = function (...args) {
    return this._saveLock[method](...args);
  };
  Object.defineProperty(delegate, 'length', {value: editorDataMemory.EditorData.prototype[method].length});
  EditorData.prototype[method] = delegate;
}
for (const method of ROUTES._presence) {
  const delegate = function (...args) {
    return this._presence[method](...args);
  };
  Object.defineProperty(delegate, 'length', {value: editorDataMemory.EditorData.prototype[method].length});
  EditorData.prototype[method] = delegate;
}
for (const method of ROUTES._data) {
  const delegate = function (...args) {
    return this._data[method](...args);
  };
  Object.defineProperty(delegate, 'length', {value: editorDataMemory.EditorData.prototype[method].length});
  EditorData.prototype[method] = delegate;
}

EditorData.prototype.cleanDocumentOnExit = async function (ctx, docId) {
  await this._data.cleanDocumentOnExit(ctx, docId);
  await this._saveLock.cleanup(ctx, docId);
};

function EditorStat(database) {
  this._redis = createRedisClient(redisConfig(), database);
  installConnectionReporting(this._redis, 'editorDataRedisStat.connection');
  this._store = createEditorStatStore(this._redis, redisConfig().prefix || 'ds:');
}

EditorStat.prototype.connect = async function () {
  try {
    await withTimeout(connectRedisClient(this._redis), CONNECT_TIMEOUT, 'Redis connect');
  } catch (error) {
    this._connectionError = error;
  }
};
EditorStat.prototype.isConnected = function () {
  return Boolean(this._redis.isReady);
};
EditorStat.prototype.ping = async function () {
  await connectRedisClient(this._redis);
  return sendCommand(this._redis, ['PING']);
};
EditorStat.prototype.close = function () {
  return closeClient(this._redis);
};
EditorStat.prototype.healthCheck = async function () {
  if (!this.isConnected()) {
    return false;
  }
  try {
    await this.ping();
    return true;
  } catch {
    return false;
  }
};

const STAT_METHODS = {
  addPresenceUniqueUser: 4,
  getPresenceUniqueUser: 2,
  addPresenceUniqueViewUser: 4,
  getPresenceUniqueViewUser: 2,
  addPresenceUniqueUsersOfMonth: 4,
  getPresenceUniqueUsersOfMonth: 1,
  addPresenceUniqueViewUsersOfMonth: 4,
  getPresenceUniqueViewUsersOfMonth: 1,
  setEditorConnections: 6,
  getEditorConnections: 1,
  setEditorConnectionsCountByShard: 3,
  incrEditorConnectionsCountByShard: 3,
  getEditorConnectionsCount: 2,
  setViewerConnectionsCountByShard: 3,
  incrViewerConnectionsCountByShard: 3,
  getViewerConnectionsCount: 2,
  setLiveViewerConnectionsCountByShard: 3,
  incrLiveViewerConnectionsCountByShard: 3,
  getLiveViewerConnectionsCount: 2,
  addShutdown: 2,
  removeShutdown: 2,
  getShutdownCount: 1,
  cleanupShutdown: 1,
  setLicense: 2,
  getLicense: 1,
  removeLicense: 1,
  lockNotification: 3,
  deleteKey: 1
};

for (const [method, arity] of Object.entries(STAT_METHODS)) {
  const delegate = function (...args) {
    return this._store[method](...args);
  };
  Object.defineProperty(delegate, 'length', {value: arity});
  EditorStat.prototype[method] = delegate;
}

module.exports = {
  EditorData,
  EditorStat,
  EXPLICIT,
  NOT_PORTED: [],
  ROUTES
};

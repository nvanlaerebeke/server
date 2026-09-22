'use strict';

const config = require('config');
const crypto = require('crypto');
const ms = require('ms');
const {sendCommand, evalScript} = require('./editorDataRedisClient');
const {jsonDecode, jsonEncode, decodeHash, strictMax, ttlSeconds} = require('./editorDataRedisData');

const cfgPresence = config.get('services.CoAuthoring.expire.presence');
const cfgMonthUniqueUsers = ms(config.get('services.CoAuthoring.expire.monthUniqueUsers'));

const ADD_MONTH_USER_SCRIPT = `
local ttl = redis.call('PTTL', KEYS[2])
redis.call('HSET', KEYS[2], ARGV[4], ARGV[5])
if ttl < 0 then
  redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
  redis.call('PEXPIRE', KEYS[2], ARGV[3])
end
return 1
`;

const ADD_UNIQUE_USER_SCRIPT = `
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
return 1
`;

const GET_UNIQUE_USERS_SCRIPT = `
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if #expired > 0 then
  redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
  for _, userId in ipairs(expired) do
    redis.call('HDEL', KEYS[2], userId)
  end
end
local active = redis.call('ZRANGEBYSCORE', KEYS[1], '(' .. ARGV[1], '+inf', 'WITHSCORES')
local result = {}
for i = 1, #active, 2 do
  local info = redis.call('HGET', KEYS[2], active[i])
  if info then
    table.insert(result, active[i])
    table.insert(result, active[i + 1])
    table.insert(result, info)
  end
end
return result
`;

const SET_CONNECTION_SAMPLE_SCRIPT = `
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[3])
return 1
`;

const SET_SHARD_COUNT_SCRIPT = `
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[4])
return tonumber(ARGV[2])
`;

const INCR_SHARD_COUNT_SCRIPT = `
local value = redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2])
if value < 0 then
  value = 0
  redis.call('HSET', KEYS[1], ARGV[1], 0)
end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[4])
return value
`;

const GET_SHARD_COUNT_SCRIPT = `
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
if #expired > 0 then
  redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
  for _, shardId in ipairs(expired) do
    redis.call('HDEL', KEYS[1], shardId)
  end
end
local values = redis.call('HVALS', KEYS[1])
local result = 0
for _, value in ipairs(values) do
  result = result + tonumber(value)
end
return result
`;

function encodePart(value) {
  return encodeURIComponent(String(value));
}

function ttlMilliseconds(value) {
  const numeric = Number(value);
  if (Number.isFinite(numeric)) {
    return Math.max(1, Math.ceil(numeric * 1000));
  }
  return Math.max(1, Math.ceil(ms(value)));
}

function createEditorStatStore(redis, prefix) {
  const sampleId = `${process.pid}:${crypto.randomBytes(12).toString('hex')}`;
  let sampleSequence = 0;

  function statBase(ctx) {
    return `${prefix}editorStat:{${encodePart(ctx.tenant)}}:`;
  }

  function uniqueKeys(ctx, view) {
    const base = statBase(ctx);
    const suffix = view ? 'view' : 'edit';
    return {
      expiry: `${base}presence:unique:${suffix}:expiry`,
      info: `${base}presence:unique:${suffix}:info`
    };
  }

  function monthIndexKey(ctx, view) {
    return `${statBase(ctx)}presence:month:${view ? 'view' : 'edit'}:index`;
  }

  function monthDataKey(ctx, period, view) {
    return `${statBase(ctx)}presence:month:${view ? 'view' : 'edit'}:${encodePart(period)}`;
  }

  async function addUniqueUser(ctx, userId, expireAt, userInfo, view) {
    const keys = uniqueKeys(ctx, view);
    return evalScript(redis, ADD_UNIQUE_USER_SCRIPT, [keys.expiry, keys.info], [expireAt, userId, jsonEncode(userInfo)]);
  }

  async function getUniqueUsers(ctx, nowUTC, view) {
    const keys = uniqueKeys(ctx, view);
    const values = await evalScript(redis, GET_UNIQUE_USERS_SCRIPT, [keys.expiry, keys.info], [nowUTC]);
    const result = [];
    for (let i = 0; i + 2 < (values || []).length; i += 3) {
      result.push({userid: String(values[i]), expire: new Date(Number(values[i + 1]) * 1000), ...jsonDecode(values[i + 2], {})});
    }
    return result;
  }

  async function addMonthUser(ctx, userId, period, userInfo, view) {
    const now = Date.now();
    const duration = Number(cfgMonthUniqueUsers);
    return evalScript(
      redis,
      ADD_MONTH_USER_SCRIPT,
      [monthIndexKey(ctx, view), monthDataKey(ctx, period, view)],
      [now + duration, period, duration, userId, jsonEncode(userInfo)]
    );
  }

  async function getMonthUsers(ctx, view) {
    const index = monthIndexKey(ctx, view);
    const now = Date.now();
    await sendCommand(redis, ['ZREMRANGEBYSCORE', index, '-inf', now]);
    const periods = await sendCommand(redis, ['ZRANGEBYSCORE', index, now + 1, '+inf']);
    const values = await Promise.all((periods || []).map(period => sendCommand(redis, ['HGETALL', monthDataKey(ctx, period, view)])));
    const result = {};
    for (let i = 0; i < (periods || []).length; i++) {
      const users = decodeHash(values[i]);
      if (Object.keys(users).length > 0) {
        const time = Number(periods[i]);
        if (Number.isFinite(time)) {
          result[new Date(time).toISOString()] = users;
        }
      }
    }
    return result;
  }

  function shardKeys(ctx, type) {
    const base = `${statBase(ctx)}connections:${type}`;
    return {count: `${base}:count`, updated: `${base}:updated`};
  }

  return {
    statBase,
    addPresenceUniqueUser: (ctx, userId, expireAt, userInfo) => addUniqueUser(ctx, userId, expireAt, userInfo, false),
    getPresenceUniqueUser: (ctx, nowUTC) => getUniqueUsers(ctx, nowUTC, false),
    addPresenceUniqueViewUser: (ctx, userId, expireAt, userInfo) => addUniqueUser(ctx, userId, expireAt, userInfo, true),
    getPresenceUniqueViewUser: (ctx, nowUTC) => getUniqueUsers(ctx, nowUTC, true),
    addPresenceUniqueUsersOfMonth: (ctx, userId, period, userInfo) => addMonthUser(ctx, userId, period, userInfo, false),
    getPresenceUniqueUsersOfMonth: ctx => getMonthUsers(ctx, false),
    addPresenceUniqueViewUsersOfMonth: (ctx, userId, period, userInfo) => addMonthUser(ctx, userId, period, userInfo, true),
    getPresenceUniqueViewUsersOfMonth: ctx => getMonthUsers(ctx, true),

    setEditorConnections(ctx, countEdit, countLiveView, countView, now, precision) {
      const maxAge = precision[precision.length - 1].val;
      const data = {time: now, edit: countEdit, liveview: countLiveView, view: countView};
      const member = jsonEncode({id: `${sampleId}:${sampleSequence++}`, data});
      return evalScript(redis, SET_CONNECTION_SAMPLE_SCRIPT, [`${statBase(ctx)}editorconnections`], [now, member, strictMax(now - maxAge + 1)]);
    },

    async getEditorConnections(ctx) {
      const values = await sendCommand(redis, ['ZRANGE', `${statBase(ctx)}editorconnections`, '0', '-1']);
      return (values || []).flatMap(value => {
        const parsed = jsonDecode(value, null);
        return parsed && parsed.data ? [parsed.data] : [];
      });
    },

    setEditorConnectionsCountByShard(ctx, shardId, count) {
      const ttl = ttlSeconds(ctx, 'services.CoAuthoring.expire.presence', cfgPresence);
      const keys = shardKeys(ctx, 'edit');
      return evalScript(redis, SET_SHARD_COUNT_SCRIPT, [keys.count, keys.updated], [shardId, count, Date.now(), ttl]);
    },

    incrEditorConnectionsCountByShard(ctx, shardId, count) {
      return this.setConnectionsCountByShard(ctx, 'edit', shardId, count, INCR_SHARD_COUNT_SCRIPT);
    },

    setViewerConnectionsCountByShard(ctx, shardId, count) {
      const ttl = ttlSeconds(ctx, 'services.CoAuthoring.expire.presence', cfgPresence);
      const keys = shardKeys(ctx, 'view');
      return evalScript(redis, SET_SHARD_COUNT_SCRIPT, [keys.count, keys.updated], [shardId, count, Date.now(), ttl]);
    },

    incrViewerConnectionsCountByShard(ctx, shardId, count) {
      return this.setConnectionsCountByShard(ctx, 'view', shardId, count, INCR_SHARD_COUNT_SCRIPT);
    },

    setLiveViewerConnectionsCountByShard(ctx, shardId, count) {
      const ttl = ttlSeconds(ctx, 'services.CoAuthoring.expire.presence', cfgPresence);
      const keys = shardKeys(ctx, 'liveview');
      return evalScript(redis, SET_SHARD_COUNT_SCRIPT, [keys.count, keys.updated], [shardId, count, Date.now(), ttl]);
    },

    incrLiveViewerConnectionsCountByShard(ctx, shardId, count) {
      return this.setConnectionsCountByShard(ctx, 'liveview', shardId, count, INCR_SHARD_COUNT_SCRIPT);
    },

    setConnectionsCountByShard(ctx, type, shardId, count, script) {
      const ttl = ttlSeconds(ctx, 'services.CoAuthoring.expire.presence', cfgPresence);
      const keys = shardKeys(ctx, type);
      return evalScript(redis, script, [keys.count, keys.updated], [shardId, count, Date.now(), ttl]);
    },

    getEditorConnectionsCount(ctx) {
      return this.getConnectionsCount(ctx, 'edit');
    },
    getViewerConnectionsCount(ctx) {
      return this.getConnectionsCount(ctx, 'view');
    },
    getLiveViewerConnectionsCount(ctx) {
      return this.getConnectionsCount(ctx, 'liveview');
    },

    getConnectionsCount(ctx, type) {
      const ttl = ttlSeconds(ctx, 'services.CoAuthoring.expire.presence', cfgPresence);
      const keys = shardKeys(ctx, type);
      return evalScript(redis, GET_SHARD_COUNT_SCRIPT, [keys.count, keys.updated], [Date.now() - ttl * 1000]);
    },

    addShutdown(key, docId) {
      return sendCommand(redis, ['SADD', key, docId]);
    },
    removeShutdown(key, docId) {
      return sendCommand(redis, ['SREM', key, docId]);
    },
    async getShutdownCount(key) {
      return Number(await sendCommand(redis, ['SCARD', key]));
    },
    cleanupShutdown(key) {
      return sendCommand(redis, ['DEL', key]);
    },
    setLicense(key, value) {
      return sendCommand(redis, ['HSET', key, key, value]);
    },
    getLicense(key) {
      return sendCommand(redis, ['HGET', key, key]);
    },
    removeLicense(key) {
      return sendCommand(redis, ['HDEL', key, key]);
    },
    async lockNotification(ctx, notificationType, ttl) {
      const key = `${statBase(ctx)}notification:${encodePart(notificationType)}`;
      return (await sendCommand(redis, ['SET', key, '1', 'NX', 'PX', ttlMilliseconds(ttl)])) === 'OK';
    },
    deleteKey(key) {
      return sendCommand(redis, ['DEL', key]);
    }
  };
}

module.exports = {createEditorStatStore};

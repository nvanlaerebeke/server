/*
 * (c) Copyright Ascensio System SIA 2010-2024
 *
 * This program is a free software product. You can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License (AGPL)
 * version 3 as published by the Free Software Foundation. In accordance with
 * Section 7(a) of the GNU AGPL its Section 15 shall be amended to the effect
 * that Ascensio System SIA expressly excludes the warranty of non-infringement
 * of any third-party rights.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. For
 * details, see the GNU AGPL at http://www.gnu.org/licenses/agpl-3.0.html
 *
 * The interactive user interfaces in modified source and object code versions
 * of the Program must display Appropriate Legal Notices, as required under
 * Section 7 of the GNU AGPL version 3.
 *
 * All the Product's GUI elements, including illustrations and icon sets, as
 * well as technical writing content are licensed under the terms of the
 * Creative Commons Attribution-ShareAlike 4.0 International. See the License
 * terms at http://creativecommons.org/licenses/by-sa/4.0/legalcode
 *
 */

'use strict';

const config = require('config');
const ms = require('ms');
const {buildKey} = require('./editorDataRedisKeys');
const {evalScript, sendCommand} = require('./editorDataRedisClient');

const cfgLocks = config.get('services.CoAuthoring.expire.locks');
const cfgMessage = config.get('services.CoAuthoring.expire.message');
const cfgForceSave = config.get('services.CoAuthoring.expire.forcesave');
const cfgSaved = config.get('services.CoAuthoring.expire.saved');
const cfgMonthUniqueUsers = ms(config.get('services.CoAuthoring.expire.monthUniqueUsers'));

const ADD_LOCKS_SCRIPT = `
for i = 1, #ARGV - 1, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
if #ARGV > 0 then
  redis.call('EXPIRE', KEYS[1], ARGV[#ARGV])
end
return 1
`;

const ADD_LOCKS_NX_SCRIPT = `
local conflict = {}
for i = 1, #ARGV - 1, 2 do
  if redis.call('HEXISTS', KEYS[1], ARGV[i]) == 1 then
    table.insert(conflict, ARGV[i])
    table.insert(conflict, ARGV[i + 1])
  else
    redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
  end
end
if redis.call('HLEN', KEYS[1]) > 0 then
  redis.call('EXPIRE', KEYS[1], ARGV[#ARGV])
end
return {conflict, redis.call('HGETALL', KEYS[1])}
`;

const REMOVE_LOCKS_SCRIPT = `
for i = 1, #ARGV, 2 do
  if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[i + 1] then
    redis.call('HDEL', KEYS[1], ARGV[i])
  end
end
return redis.call('HLEN', KEYS[1])
`;

const ADD_MESSAGE_SCRIPT = `
local length = redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return length
`;

const GETDEL_SCRIPT = `
local value = redis.call('GET', KEYS[1])
if value then
  redis.call('DEL', KEYS[1])
end
return value
`;

const START_FORCE_SAVE_SCRIPT = `
local raw = redis.call('HGET', KEYS[1], 'state')
if not raw then
  return nil
end
local ok, value = pcall(cjson.decode, raw)
if not ok or value.started then
  return nil
end
value.started = true
value.ended = false
value.convertInfo = nil
local updated = cjson.encode(value)
redis.call('HSET', KEYS[1], 'state', updated)
redis.call('EXPIRE', KEYS[1], ARGV[1])
return updated
`;

const SET_FORCE_SAVE_SCRIPT = `
local raw = redis.call('HGET', KEYS[1], 'state')
if not raw then
  return nil
end
local ok, value = pcall(cjson.decode, raw)
if not ok then
  return nil
end
local expectedTime = cjson.decode(ARGV[1])
local expectedIndex = cjson.decode(ARGV[2])
if value.time ~= expectedTime or value.index ~= expectedIndex then
  return nil
end
value.started = ARGV[3] == '1'
value.ended = ARGV[4] == '1'
if ARGV[5] == '1' then
  value.convertInfo = cjson.decode(ARGV[6])
else
  value.convertInfo = nil
end
local updated = cjson.encode(value)
redis.call('HSET', KEYS[1], 'state', updated)
redis.call('EXPIRE', KEYS[1], ARGV[7])
return updated
`;

const STORE_FORCE_SAVE_SCRIPT = `
redis.call('HSET', KEYS[1], 'state', ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return 1
`;

const CLEAN_DATA_SCRIPT = `
redis.call('DEL', unpack(KEYS))
return 1
`;

const POP_EXPIRED_SCRIPT = `
local values = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if #values > 0 then
  redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
end
return values
`;

function ttlSeconds(ctx, path, fallback) {
  const value = ctx && typeof ctx.getCfg === 'function' ? ctx.getCfg(path, fallback) : fallback;
  const numeric = Number(value);
  return Math.max(1, Math.ceil(Number.isFinite(numeric) ? numeric : ms(value) / 1000));
}

function jsonEncode(value) {
  const encoded = JSON.stringify(value);
  return encoded === undefined ? 'null' : encoded;
}

function jsonDecode(value, fallback) {
  if (value === null || value === undefined) {
    return fallback;
  }
  try {
    return JSON.parse(String(value));
  } catch {
    return fallback;
  }
}

function decodeHash(value) {
  if (!value) {
    return {};
  }
  if (!Array.isArray(value)) {
    return Object.fromEntries(Object.entries(value).map(([field, entry]) => [field, jsonDecode(entry, null)]));
  }
  const result = {};
  for (let i = 0; i + 1 < value.length; i += 2) {
    result[String(value[i])] = jsonDecode(value[i + 1], null);
  }
  return result;
}

function argsFromObject(value) {
  const result = [];
  for (const field in value) {
    if (Object.hasOwn(value, field)) {
      result.push(field, jsonEncode(value[field]));
    }
  }
  return result;
}

function strictMax(value) {
  return String(Math.ceil(Number(value)) - 1);
}

function createEditorDataStore(redis, prefix) {
  const dataPrefix = `${prefix}editorData:`;
  const forceSaveTimerKey = `${prefix}editorData:{index}:forcesavetimer`;

  function keys(ctx, docId) {
    const base = buildKey(dataPrefix, ctx.tenant, docId);
    return {
      locks: `${base}:locks`,
      messages: `${base}:message`,
      saved: `${base}:saved`,
      forceSave: `${base}:forcesave`
    };
  }

  return {
    keys,

    addLocks(ctx, docId, locks) {
      const args = argsFromObject(locks);
      if (args.length === 0) {
        return Promise.resolve();
      }
      args.push(String(ttlSeconds(ctx, 'services.CoAuthoring.expire.locks', cfgLocks)));
      return evalScript(redis, ADD_LOCKS_SCRIPT, [keys(ctx, docId).locks], args);
    },

    async addLocksNX(ctx, docId, locks) {
      const args = argsFromObject(locks);
      if (args.length === 0) {
        return {lockConflict: {}, allLocks: await this.getLocks(ctx, docId)};
      }
      args.push(String(ttlSeconds(ctx, 'services.CoAuthoring.expire.locks', cfgLocks)));
      const result = await evalScript(redis, ADD_LOCKS_NX_SCRIPT, [keys(ctx, docId).locks], args);
      return {lockConflict: decodeHash(result?.[0]), allLocks: decodeHash(result?.[1])};
    },

    removeLocks(ctx, docId, locks) {
      const args = argsFromObject(locks);
      return args.length === 0 ? Promise.resolve() : evalScript(redis, REMOVE_LOCKS_SCRIPT, [keys(ctx, docId).locks], args);
    },

    removeAllLocks(ctx, docId) {
      return sendCommand(redis, ['DEL', keys(ctx, docId).locks]);
    },

    async getLocks(ctx, docId) {
      return decodeHash(await sendCommand(redis, ['HGETALL', keys(ctx, docId).locks]));
    },

    addMessage(ctx, docId, message) {
      const ttl = ttlSeconds(ctx, 'services.CoAuthoring.expire.message', cfgMessage);
      return evalScript(redis, ADD_MESSAGE_SCRIPT, [keys(ctx, docId).messages], [jsonEncode(message), String(ttl)]);
    },

    removeMessages(ctx, docId) {
      return sendCommand(redis, ['DEL', keys(ctx, docId).messages]);
    },

    async getMessages(ctx, docId) {
      const values = await sendCommand(redis, ['LRANGE', keys(ctx, docId).messages, '0', '-1']);
      return (values || []).map(value => jsonDecode(value, null));
    },

    setSaved(ctx, docId, status) {
      const ttl = ttlSeconds(ctx, 'services.CoAuthoring.expire.saved', cfgSaved);
      return sendCommand(redis, ['SET', keys(ctx, docId).saved, status, 'EX', ttl]);
    },

    getdelSaved(ctx, docId) {
      return evalScript(redis, GETDEL_SCRIPT, [keys(ctx, docId).saved], []);
    },

    setForceSave(ctx, docId, time, index, baseUrl, changeInfo, convertInfo) {
      const value = {time, index, baseUrl, changeInfo, started: false, ended: false, convertInfo};
      const ttl = ttlSeconds(ctx, 'services.CoAuthoring.expire.forcesave', cfgForceSave);
      return evalScript(redis, STORE_FORCE_SAVE_SCRIPT, [keys(ctx, docId).forceSave], [jsonEncode(value), String(ttl)]);
    },

    async getForceSave(ctx, docId) {
      const value = await sendCommand(redis, ['HGET', keys(ctx, docId).forceSave, 'state']);
      return jsonDecode(value, null);
    },

    async checkAndStartForceSave(ctx, docId) {
      const ttl = ttlSeconds(ctx, 'services.CoAuthoring.expire.forcesave', cfgForceSave);
      const value = await evalScript(redis, START_FORCE_SAVE_SCRIPT, [keys(ctx, docId).forceSave], [String(ttl)]);
      return jsonDecode(value, undefined);
    },

    async checkAndSetForceSave(ctx, docId, time, index, started, ended, convertInfo) {
      const ttl = ttlSeconds(ctx, 'services.CoAuthoring.expire.forcesave', cfgForceSave);
      const hasConvertInfo = convertInfo !== undefined;
      const value = await evalScript(
        redis,
        SET_FORCE_SAVE_SCRIPT,
        [keys(ctx, docId).forceSave],
        [
          jsonEncode(time),
          jsonEncode(index),
          started ? '1' : '0',
          ended ? '1' : '0',
          hasConvertInfo ? '1' : '0',
          jsonEncode(convertInfo),
          String(ttl)
        ]
      );
      return jsonDecode(value, undefined);
    },

    removeForceSave(ctx, docId) {
      return sendCommand(redis, ['DEL', keys(ctx, docId).forceSave]);
    },

    addForceSaveTimerNX(ctx, docId, expireAt) {
      const member = JSON.stringify([String(ctx.tenant), String(docId)]);
      return sendCommand(redis, ['ZADD', forceSaveTimerKey, 'NX', expireAt, member]);
    },

    async getForceSaveTimer(now) {
      const values = await evalScript(redis, POP_EXPIRED_SCRIPT, [forceSaveTimerKey], [strictMax(now)]);
      return (values || []).flatMap(value => {
        try {
          const parsed = JSON.parse(String(value));
          return Array.isArray(parsed) && parsed.length === 2 ? [parsed] : [];
        } catch {
          return [];
        }
      });
    },

    async cleanDocumentOnExit(ctx, docId) {
      const documentKeys = keys(ctx, docId);
      await evalScript(redis, CLEAN_DATA_SCRIPT, Object.values(documentKeys), []);
      await sendCommand(redis, ['ZREM', forceSaveTimerKey, JSON.stringify([String(ctx.tenant), String(docId)])]);
    }
  };
}

module.exports = {
  cfgMonthUniqueUsers,
  createEditorDataStore,
  decodeHash,
  jsonDecode,
  jsonEncode,
  sendCommand,
  strictMax,
  ttlSeconds
};

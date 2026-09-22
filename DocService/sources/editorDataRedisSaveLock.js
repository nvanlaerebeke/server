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

const {buildKey} = require('./editorDataRedisKeys');
const {createFailureReporter} = require('./editorDataRedisReport');
const {defineScript, sendCommand} = require('./editorDataRedisClient');

// Owner-token locks, not fencing tokens - see REDIS_EDITORDATA.md.

const LOCK_SCRIPT = `
local current = redis.call('GET', KEYS[1])
if current == false or current == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
`;

const UNLOCK_SCRIPT = `
local current = redis.call('GET', KEYS[1])
if current == false then
  return {2, false}
end
if current == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return {1, current}
end
return {0, current}
`;

// Must stay numerically identical to commondefines.js's c_oAscUnlockRes -
// duplicated to keep this module dependency-free; callers compare directly.
const UNLOCK_RES = {LOCKED: 0, UNLOCKED: 1, EMPTY: 2};

function ttlToMs(ttl) {
  // `ttl` is in seconds at every call site, as in editorDataMemory.js.
  return ttl * 1000;
}

// Caller owns the client's lifetime.
function createSaveLockStore(redis, prefix) {
  defineScript(redis, 'saveLockScript', LOCK_SCRIPT, 1);
  defineScript(redis, 'saveUnlockScript', UNLOCK_SCRIPT, 1);

  const report = createFailureReporter('editorDataRedisSaveLock');

  const lockSavePrefix = `${prefix}lockSave:`;
  const lockAuthPrefix = `${prefix}lockAuth:`;

  // Fail closed: an error must come back as a denial, never a throw and
  // never a false grant. buildKey stays inside the try - encodeURIComponent
  // throws on malformed unicode.
  async function lock(keyPrefix, ctx, docId, userId, ttl) {
    try {
      const key = buildKey(keyPrefix, ctx.tenant, docId);
      const res = await redis.saveLockScript(key, userId, ttlToMs(ttl));
      report.success(ctx);
      return res === 1;
    } catch (err) {
      report.failure(ctx, 'lock', err);
      return false;
    }
  }
  // LOCKED, not UNLOCKED, on error: callers only take their "release
  // succeeded" branch on UNLOCKED, and we don't know whether it happened.
  async function unlock(keyPrefix, ctx, docId, userId) {
    try {
      const key = buildKey(keyPrefix, ctx.tenant, docId);
      const reply = await redis.saveUnlockScript(key, userId);
      const [code] = Array.isArray(reply) ? reply : [reply];
      report.success(ctx);
      if (code === 1) return UNLOCK_RES.UNLOCKED;
      if (code === 0) return UNLOCK_RES.LOCKED;
      return UNLOCK_RES.EMPTY;
    } catch (err) {
      report.failure(ctx, 'unlock', err);
      return UNLOCK_RES.LOCKED;
    }
  }

  return {
    lockSave: (ctx, docId, userId, ttl) => lock(lockSavePrefix, ctx, docId, userId, ttl),
    unlockSave: (ctx, docId, userId) => unlock(lockSavePrefix, ctx, docId, userId),
    lockAuth: (ctx, docId, userId, ttl) => lock(lockAuthPrefix, ctx, docId, userId, ttl),
    unlockAuth: (ctx, docId, userId) => unlock(lockAuthPrefix, ctx, docId, userId),
    async cleanup(ctx, docId) {
      // Safe to swallow: both keys carry their own PX expiry, so a failed DEL
      // only delays removal. Throwing here would abort the caller's remaining
      // cleanup (e.g. unlockWopiDoc) on a transient blip.
      try {
        await sendCommand(redis, ['DEL', buildKey(lockSavePrefix, ctx.tenant, docId), buildKey(lockAuthPrefix, ctx.tenant, docId)]);
        report.success(ctx);
      } catch (err) {
        report.failure(ctx, 'cleanup', err);
      }
    }
  };
}

module.exports = {createSaveLockStore, UNLOCK_RES};

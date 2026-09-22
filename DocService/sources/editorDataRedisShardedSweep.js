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

const {encodePair, decodePair, shardIndex} = require('./editorDataRedisKeys');
const {defineScript, sendCommand} = require('./editorDataRedisClient');

// "Which (tenant, docId) pairs are due for a sweep", spread over N sorted
// sets. See REDIS_EDITORDATA.md for why it's sharded.

// One script, so two replicas sweeping concurrently can't both claim the
// same entry. LIMIT bounds a single call against a large backlog.
const CLAIM_SCRIPT = `
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due > 0 then
  redis.call('ZREM', KEYS[1], unpack(due))
end
return due
`;

// One script: split into ZSCORE-then-ZADD round trips, two concurrent
// trackers could both read the old score and the later ZADD would undo the
// earlier one.
const TRACK_SCRIPT = `
local existing = redis.call('ZSCORE', KEYS[1], ARGV[2])
if not existing or tonumber(existing) < tonumber(ARGV[1]) then
  redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
end
return 1
`;

const CLAIM_BATCH_SIZE = 100;

// `commandNamePrefix` must be unique per sweep instance sharing one client -
// script helpers are attached to the client by name.
function createShardedSweep(redis, keyPrefix, numShards, commandNamePrefix) {
  const claimCommand = `${commandNamePrefix}Claim`;
  const trackCommand = `${commandNamePrefix}Track`;
  defineScript(redis, claimCommand, CLAIM_SCRIPT, 1);
  defineScript(redis, trackCommand, TRACK_SCRIPT, 1);

  function shardKey(tenant, docId) {
    return `${keyPrefix}${shardIndex(tenant, docId, numShards)}`;
  }

  return {
    // Several connections on one document each track their own expiry; the
    // sweep should fire on the latest, hence forwards-only.
    async track(tenant, docId, expiresAt) {
      const key = shardKey(tenant, docId);
      const member = encodePair(tenant, docId);
      await redis[trackCommand](key, expiresAt, member);
    },
    async untrack(tenant, docId) {
      const key = shardKey(tenant, docId);
      const member = encodePair(tenant, docId);
      await sendCommand(redis, ['ZREM', key, member]);
    },
    // [tenant, docId] pairs due at `now`.
    async claimExpired(now) {
      const results = [];
      for (let shard = 0; shard < numShards; shard++) {
        const key = `${keyPrefix}${shard}`;
        let due;
        do {
          due = await redis[claimCommand](key, now, CLAIM_BATCH_SIZE);
          for (const member of due) {
            results.push(decodePair(member));
          }
        } while (due.length === CLAIM_BATCH_SIZE);
      }
      return results;
    }
  };
}

module.exports = {createShardedSweep};

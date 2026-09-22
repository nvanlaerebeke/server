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

// encodeURIComponent so a ':' inside tenant or docId can't be mistaken for
// the separator. The braces are a Redis Cluster hash tag: only what is
// inside them is hashed, so one document's keys share a slot and its
// two-key scripts and DELs are not CROSSSLOT.
function buildKey(prefix, tenant, docId) {
  return `${prefix}{${encodeURIComponent(tenant)}:${encodeURIComponent(docId)}}`;
}

function encodePair(tenant, docId) {
  return `${encodeURIComponent(tenant)}:${encodeURIComponent(docId)}`;
}
function decodePair(member) {
  const idx = member.indexOf(':');
  return [decodeURIComponent(member.slice(0, idx)), decodeURIComponent(member.slice(idx + 1))];
}

// Must stay deterministic per (tenant, docId): track() and untrack() have to
// agree on the shard. Keyed on both, not tenant alone - a single-tenant
// deployment would otherwise put every document in one shard.
function shardIndex(tenant, docId, numShards) {
  let hash = 0;
  for (let i = 0; i < tenant.length; i++) {
    hash = (hash * 31 + tenant.charCodeAt(i)) >>> 0;
  }
  for (let i = 0; i < docId.length; i++) {
    hash = (hash * 31 + docId.charCodeAt(i)) >>> 0;
  }
  return hash % numShards;
}

module.exports = {buildKey, encodePair, decodePair, shardIndex};

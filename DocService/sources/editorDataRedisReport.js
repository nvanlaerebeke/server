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

const operationContext = require('./../../Common/sources/operationContext');

// Reports Redis failures without drowning the log in them. Both stores
// swallow their errors by design, which left an outage looking healthy; but
// a broken deployment fails on every operation, so an error per catch hides
// the signal just as well. Log the transition, then a periodic reminder
// carrying the suppressed count, then the recovery.

const REPEAT_INTERVAL_MS = 60000;

function createFailureReporter(storeName) {
  let failing = false;
  let lastReportedAt = 0;
  let suppressed = 0;

  function logger(ctx) {
    return (ctx && ctx.logger) || operationContext.global.logger;
  }

  return {
    failure(ctx, operation, err) {
      const now = Date.now();
      suppressed++;
      if (failing && now - lastReportedAt < REPEAT_INTERVAL_MS) {
        return;
      }
      logger(ctx).error(
        '%s: Redis %s failed, degrading (%d failure(s) since last report): %s',
        storeName,
        operation,
        suppressed,
        (err && err.message) || err
      );
      failing = true;
      lastReportedAt = now;
      suppressed = 0;
    },

    success(ctx) {
      if (!failing) {
        return;
      }
      failing = false;
      suppressed = 0;
      logger(ctx).info('%s: Redis recovered', storeName);
    }
  };
}

module.exports = {createFailureReporter};

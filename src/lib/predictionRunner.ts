import Dexie from 'dexie';
import { db, type PredictionRecord } from '../data/db';
import { TF_MS, lookalikeDrafts, discoveryDrafts } from '../engine/predictor';
import { discoverWords, type DiscoveryResult } from '../engine/discovery';
import type { PatternCandle } from '../engine/patterns';
import { getOpenPredictions, updatePrediction, addPredictionIfNew } from '../data/predictions';
import { getSetting, logError } from '../data/repositories';

let busy = false;
const processedLastT = new Map<string, number>();
const discoveryCache = new Map<string, { result: DiscoveryResult; t: number }>();

async function runCycle(): Promise<void> {
  if (busy) return;
  busy = true;

  try {
    // A) SCORING PASS (always runs, even when disabled)
    const openPreds = await getOpenPredictions(200);
    const now = Date.now();

    for (const p of openPreds) {
      try {
        const stepMs = TF_MS[p.tf] || 60000;
        const candle = await db.candles.get(['binance', p.sym, p.tf, p.targetT]);

        if (candle && candle.closed === true) {
          const exitPrice = candle.c;
          let status: 'won' | 'lost' | 'tie';

          if (p.direction === 'up') {
            if (exitPrice > p.entryPrice) status = 'won';
            else if (exitPrice < p.entryPrice) status = 'lost';
            else status = 'tie';
          } else {
            if (exitPrice < p.entryPrice) status = 'won';
            else if (exitPrice > p.entryPrice) status = 'lost';
            else status = 'tie';
          }

          let resultPct = p.entryPrice > 0 ? ((exitPrice - p.entryPrice) / p.entryPrice) * 100 : 0;
          if (p.direction === 'down') {
            resultPct *= -1;
          }

          await updatePrediction(p.id, {
            status,
            exitPrice,
            resultPct,
            scoredAt: Date.now(),
          });
        } else if (!candle && now > p.targetT + stepMs + 6 * 3600 * 1000) {
          await updatePrediction(p.id, {
            status: 'expired',
            scoredAt: Date.now(),
          });
        }
      } catch (err) {
        logError('PredictionRunner', `Scoring failed for ${p.id}: ${String(err)}`);
      }
    }

    // B) PREDICTION PASS (only if predLabEnabled)
    const predLabEnabled = await getSetting<boolean>('predLabEnabled', false);
    if (!predLabEnabled) {
      return;
    }

    const tracked = await getSetting<string[]>('trackedSymbols', ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']);
    const defaultSymbols = tracked.slice(0, 3);
    const symbols = await getSetting<string[]>('predLabSymbols', defaultSymbols);
    const timeframes = await getSetting<string[]>('predLabTimeframes', ['5m', '15m']);

    for (const sym of symbols) {
      for (const tf of timeframes) {
        const pairKey = `${sym}|${tf}`;
        const stepMs = TF_MS[tf] || 60000;

        await new Promise((resolve) => setTimeout(resolve, 50));

        try {
          const rows = await db.candles
            .where('[src+sym+tf+t]')
            .between(['binance', sym, tf, Dexie.minKey], ['binance', sym, tf, Dexie.maxKey])
            .reverse()
            .limit(20000)
            .toArray();

          const candles: PatternCandle[] = rows
            .reverse()
            .filter((r) => r.closed)
            .map((r) => ({ t: r.t, o: r.o, h: r.h, l: r.l, c: r.c, v: r.v }));

          if (candles.length < 500) continue;

          const last = candles[candles.length - 1];
          if (Date.now() - (last.t + stepMs) > 3 * stepMs) {
            continue; // data is stale
          }

          if (processedLastT.get(pairKey) === last.t) {
            continue;
          }
          processedLastT.set(pairKey, last.t);

          const lDrafts = lookalikeDrafts(candles);

          const cachedDisc = discoveryCache.get(pairKey);
          let discResult: DiscoveryResult;
          if (!cachedDisc || Date.now() - cachedDisc.t > 6 * 3600 * 1000) {
            discResult = discoverWords(candles);
            discoveryCache.set(pairKey, { result: discResult, t: Date.now() });
          } else {
            discResult = cachedDisc.result;
          }
          const dDrafts = discoveryDrafts(discResult);

          const drafts = [...lDrafts, ...dDrafts];
          for (const draft of drafts) {
            const id = `${sym}|${tf}|${draft.source}|${draft.note}|${draft.horizon}|${last.t}`;
            const targetT = last.t + draft.horizon * stepMs;

            const rec: PredictionRecord = {
              id,
              t: Date.now(),
              sym,
              tf,
              source: draft.source,
              direction: draft.direction,
              horizon: draft.horizon,
              entryT: last.t,
              entryPrice: last.c,
              targetT,
              confidence: draft.confidence,
              baseline: draft.baseline,
              note: draft.note,
              status: 'open',
            };

            await addPredictionIfNew(rec);
          }
        } catch (err) {
          logError('PredictionRunner', `Prediction pass failed for ${pairKey}: ${String(err)}`);
        }
      }
    }
  } catch (err) {
    logError('PredictionRunner', `Cycle failed: ${String(err)}`);
  } finally {
    busy = false;
  }
}

export function startPredictionRunner(): () => void {
  const intervalId = setInterval(() => {
    runCycle().catch((err) => {
      try {
        logError('PredictionRunner', String(err?.message || err));
      } catch {}
    });
  }, 20000);

  // Trigger first cycle promptly
  setTimeout(() => {
    runCycle().catch((err) => {
      try {
        logError('PredictionRunner', String(err?.message || err));
      } catch {}
    });
  }, 1000);

  return () => {
    clearInterval(intervalId);
  };
}

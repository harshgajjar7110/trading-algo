/**
 * EntryRadar Component
 * 
 * Visualizes entry predictions for PE and CE options with:
 * - Distance to trigger levels
 * - Estimated time to entry
 * - Visual indicators for readiness
 */

'use client';

import React from 'react';
import { clsx } from 'clsx';
import type { EntryPrediction } from '@/types';

interface EntryRadarProps {
  pePrediction: EntryPrediction | null;
  cePrediction: EntryPrediction | null;
  marketTrend?: 'bullish' | 'bearish' | 'neutral';
}

// SVG Icons
const TargetIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <circle cx="12" cy="12" r="10" />
    <circle cx="12" cy="12" r="6" />
    <circle cx="12" cy="12" r="2" />
    <line x1="12" y1="2" x2="12" y2="6" />
    <line x1="12" y1="18" x2="12" y2="22" />
    <line x1="2" y1="12" x2="6" y2="12" />
    <line x1="18" y1="12" x2="22" y2="12" />
  </svg>
);

const ArrowDownIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <line x1="12" y1="5" x2="12" y2="19" />
    <polyline points="19 12 12 19 5 12" />
  </svg>
);

const ArrowUpIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <line x1="12" y1="19" x2="12" y2="5" />
    <polyline points="5 12 12 5 19 12" />
  </svg>
);

const ClockIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <circle cx="12" cy="12" r="10" />
    <polyline points="12 6 12 12 16 14" />
  </svg>
);

// Format time estimate
const formatTimeEstimate = (timeStr: string | null | undefined): string => {
  if (!timeStr) return '-- min';
  return timeStr;
};

// Single option prediction card
const OptionCard: React.FC<{
  type: 'PE' | 'CE';
  prediction: EntryPrediction | null;
  isPreferred: boolean;
}> = ({ type, prediction, isPreferred }) => {
  const isPE = type === 'PE';
  const colorClass = isPE ? 'text-rose-500' : 'text-emerald-500';
  const bgClass = isPE ? 'bg-rose-50' : 'bg-emerald-50';
  const borderClass = isPE ? 'border-rose-200' : 'border-emerald-200';
  
  const distance = prediction?.distance_percent ?? prediction?.distance_to_trigger ?? null;
  const isReady = prediction?.status === 'READY';
  const timeEstimate = prediction?.estimated_time;
  const triggerPrice = prediction?.trigger_price ?? null;

  return (
    <div className={clsx(
      "relative rounded-xl border-2 p-5 transition-all duration-300",
      isReady ? "border-solid" : "border-dashed",
      isReady ? borderClass : "border-gray-200",
      isReady ? bgClass : "bg-gray-50/50",
      isPreferred && "ring-2 ring-offset-2 ring-blue-400"
    )}>
      {/* Preferred badge */}
      {isPreferred && (
        <div className="absolute -top-3 left-1/2 -translate-x-1/2">
          <span className="px-3 py-1 text-xs font-semibold rounded-full bg-blue-500 text-white shadow-sm">
            Preferred
          </span>
        </div>
      )}

      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <span className={clsx(
            "text-2xl font-bold",
            colorClass
          )}>
            {type}
          </span>
          <span className="text-sm text-gray-400">
            {isPE ? 'Put' : 'Call'}
          </span>
        </div>
        
        {isReady ? (
          <span className={clsx(
            "px-2 py-1 text-xs font-semibold rounded-full",
            "bg-white shadow-sm",
            colorClass
          )}>
            Ready
          </span>
        ) : (
          <span className="px-2 py-1 text-xs font-medium rounded-full bg-gray-100 text-gray-500">
            Waiting
          </span>
        )}
      </div>

      {prediction ? (
        <div className="space-y-4">
          {/* Distance indicator */}
          <div>
            <div className="flex items-center justify-between text-sm mb-2">
              <span className="text-gray-500">Distance to trigger</span>
              <span className={clsx(
                "font-mono font-semibold",
                isReady ? colorClass : "text-gray-600"
              )}>
                {distance !== null ? `${distance.toFixed(2)}%` : '--'}
              </span>
            </div>
            
            {/* Progress bar */}
            <div className="h-3 bg-gray-200 rounded-full overflow-hidden">
              <div 
                className={clsx(
                  "h-full rounded-full transition-all duration-500",
                  isPE ? "bg-rose-500" : "bg-emerald-500"
                )}
                style={{ 
                  width: `${Math.max(0, Math.min(100, 100 - (distance || 0)))}%` 
                }}
              />
            </div>
          </div>

          {/* Trigger price */}
          {triggerPrice !== null && (
            <div className="flex items-center justify-between">
              <span className="text-sm text-gray-500">Trigger at</span>
              <span className="font-mono font-medium text-gray-700">
                ₹{triggerPrice.toFixed(2)}
              </span>
            </div>
          )}

          {/* Time estimate */}
          <div className={clsx(
            "flex items-center gap-2 p-3 rounded-lg",
            isReady ? "bg-white/60" : "bg-gray-100"
          )}>
            <ClockIcon className={clsx(
              "h-4 w-4",
              isReady ? colorClass : "text-gray-400"
            )} />
            <span className="text-sm text-gray-600">ETA:</span>
            <span className={clsx(
              "font-mono font-semibold",
              isReady ? colorClass : "text-gray-600"
            )}>
              {formatTimeEstimate(timeEstimate)}
            </span>
          </div>

          {/* Blocking reasons if any */}
          {prediction?.blocking_reasons && prediction.blocking_reasons.length > 0 && (
            <div className="mt-2">
              <span className="text-xs text-gray-500">Waiting for:</span>
              <ul className="mt-1 space-y-1">
                {prediction.blocking_reasons.map((reason, idx) => (
                  <li key={idx} className="text-xs text-amber-600 flex items-center gap-1">
                    <span className="h-1 w-1 rounded-full bg-amber-500" />
                    {reason}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      ) : (
        <div className="text-center py-6">
          <TargetIcon className="h-10 w-10 mx-auto mb-2 text-gray-300" />
          <p className="text-sm text-gray-400">No prediction available</p>
        </div>
      )}
    </div>
  );
};

export const EntryRadar: React.FC<EntryRadarProps> = ({
  pePrediction,
  cePrediction,
  marketTrend = 'neutral',
}) => {
  // Determine preferred option based on market trend
  const preferredOption = marketTrend === 'bearish' ? 'PE' : 
                          marketTrend === 'bullish' ? 'CE' : null;

  return (
    <div className="rounded-xl border bg-white shadow-sm p-5">
      <div className="flex items-center justify-between mb-5">
        <div>
          <h3 className="text-sm font-medium text-gray-500">Entry Radar</h3>
          <p className="text-xs text-gray-400 mt-0.5">
            Live entry predictions for PE and CE options
          </p>
        </div>
        
        {/* Market trend indicator */}
        <div className={clsx(
          "flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium",
          marketTrend === 'bullish' && "bg-emerald-50 text-emerald-600",
          marketTrend === 'bearish' && "bg-rose-50 text-rose-600",
          marketTrend === 'neutral' && "bg-gray-50 text-gray-600"
        )}>
          {marketTrend === 'bullish' && <ArrowUpIcon className="h-3.5 w-3.5" />}
          {marketTrend === 'bearish' && <ArrowDownIcon className="h-3.5 w-3.5" />}
          <span className="capitalize">{marketTrend}</span>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <OptionCard 
          type="PE" 
          prediction={pePrediction} 
          isPreferred={preferredOption === 'PE'}
        />
        <OptionCard 
          type="CE" 
          prediction={cePrediction} 
          isPreferred={preferredOption === 'CE'}
        />
      </div>

      {/* Legend */}
      <div className="mt-4 pt-4 border-t border-gray-100 flex items-center gap-6 text-xs text-gray-400">
        <div className="flex items-center gap-1.5">
          <div className="h-2 w-2 rounded-full bg-rose-500" />
          <span>PE = Put Option</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="h-2 w-2 rounded-full bg-emerald-500" />
          <span>CE = Call Option</span>
        </div>
      </div>
    </div>
  );
};

export default EntryRadar;

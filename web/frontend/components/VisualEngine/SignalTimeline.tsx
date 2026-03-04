/**
 * SignalTimeline Component
 * 
 * Displays recent entry/exit signals in a timeline format.
 */

'use client';

import React from 'react';
import { clsx } from 'clsx';
import type { EntrySignal } from '@/types';

interface SignalTimelineProps {
  signals: EntrySignal[];
  maxDisplay?: number;
}

// SVG Icons
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

const LogInIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M15 3h4a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2h-4" />
    <polyline points="10 17 15 12 10 7" />
    <line x1="15" y1="12" x2="3" y2="12" />
  </svg>
);

const LogOutIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" />
    <polyline points="16 17 21 12 16 7" />
    <line x1="21" y1="12" x2="9" y2="12" />
  </svg>
);

// Format timestamp for display
const formatTime = (timestamp: string): string => {
  try {
    const date = new Date(timestamp);
    return date.toLocaleTimeString('en-IN', { 
      hour: '2-digit', 
      minute: '2-digit',
      second: '2-digit',
      hour12: false 
    });
  } catch {
    return timestamp;
  }
};

// Format relative time
const formatRelativeTime = (timestamp: string): string => {
  try {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffSecs = Math.floor(diffMs / 1000);
    
    if (diffSecs < 60) return 'just now';
    if (diffSecs < 3600) return `${Math.floor(diffSecs / 60)}m ago`;
    if (diffSecs < 86400) return `${Math.floor(diffSecs / 3600)}h ago`;
    return date.toLocaleDateString();
  } catch {
    return '';
  }
};

const SignalItem: React.FC<{
  signal: EntrySignal;
  isLast: boolean;
}> = ({ signal, isLast }) => {
  const isPE = signal.side === 'PE';
  const isEntry = signal.type === 'ENTRY';
  
  const sideColor = isPE ? 'text-rose-500' : 'text-emerald-500';
  const sideBg = isPE ? 'bg-rose-50' : 'bg-emerald-50';
  const sideBorder = isPE ? 'border-rose-200' : 'border-emerald-200';

  return (
    <div className="relative flex gap-4 pb-4 last:pb-0">
      {/* Timeline line */}
      {!isLast && (
        <div className="absolute left-[19px] top-10 bottom-0 w-px bg-gray-200" />
      )}
      
      {/* Icon */}
      <div className={clsx(
        "relative z-10 flex-shrink-0 w-10 h-10 rounded-full flex items-center justify-center border-2",
        sideBg,
        sideBorder,
        sideColor
      )}>
        {isEntry ? (
          <LogInIcon className="h-4 w-4" />
        ) : (
          <LogOutIcon className="h-4 w-4" />
        )}
      </div>
      
      {/* Content */}
      <div className="flex-1 min-w-0 pt-1">
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <span className={clsx(
              "font-bold text-lg",
              sideColor
            )}>
              {signal.side}
            </span>
            <span className={clsx(
              "px-2 py-0.5 text-xs font-medium rounded-full",
              isEntry 
                ? "bg-blue-50 text-blue-600" 
                : "bg-amber-50 text-amber-600"
            )}>
              {isEntry ? 'ENTRY' : 'EXIT'}
            </span>
          </div>
          <div className="text-right">
            <div className="text-sm font-mono font-medium text-gray-700">
              ₹{signal.price.toFixed(2)}
            </div>
            <div className="text-xs text-gray-400" title={signal.timestamp}>
              {formatRelativeTime(signal.timestamp)}
            </div>
          </div>
        </div>
        
        {/* Time */}
        <div className="text-xs text-gray-400 mt-1">
          {formatTime(signal.timestamp)}
        </div>
        
        {/* Reason */}
        <p className="text-sm text-gray-600 mt-1.5 leading-relaxed">
          {signal.reason}
        </p>
        
        {/* Filters info */}
        {(signal.filters_passed.length > 0 || signal.filters_failed.length > 0) && (
          <div className="flex flex-wrap gap-2 mt-2">
            {signal.filters_passed.map((filter, idx) => (
              <span 
                key={`pass-${idx}`}
                className="inline-flex items-center gap-1 px-2 py-0.5 text-xs rounded-full bg-emerald-50 text-emerald-600"
              >
                <span className="h-1 w-1 rounded-full bg-emerald-500" />
                {filter}
              </span>
            ))}
            {signal.filters_failed.map((filter, idx) => (
              <span 
                key={`fail-${idx}`}
                className="inline-flex items-center gap-1 px-2 py-0.5 text-xs rounded-full bg-red-50 text-red-600"
              >
                <span className="h-1 w-1 rounded-full bg-red-500" />
                {filter}
              </span>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export const SignalTimeline: React.FC<SignalTimelineProps> = ({ 
  signals, 
  maxDisplay = 5 
}) => {
  const displaySignals = signals.slice(0, maxDisplay);
  const hasMore = signals.length > maxDisplay;

  return (
    <div className="rounded-xl border bg-white shadow-sm p-5">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-sm font-medium text-gray-500">Recent Signals</h3>
          <p className="text-xs text-gray-400 mt-0.5">
            Last {displaySignals.length} signals
          </p>
        </div>
        
        {/* Summary stats */}
        <div className="flex items-center gap-3 text-xs">
          <div className="flex items-center gap-1">
            <div className="h-2 w-2 rounded-full bg-emerald-500" />
            <span className="text-gray-500">
              {signals.filter(s => s.type === 'ENTRY').length} entries
            </span>
          </div>
          <div className="flex items-center gap-1">
            <div className="h-2 w-2 rounded-full bg-amber-500" />
            <span className="text-gray-500">
              {signals.filter(s => s.type === 'EXIT').length} exits
            </span>
          </div>
        </div>
      </div>

      {displaySignals.length > 0 ? (
        <div className="space-y-0">
          {displaySignals.map((signal, index) => (
            <SignalItem 
              key={`${signal.timestamp}-${index}`}
              signal={signal}
              isLast={index === displaySignals.length - 1}
            />
          ))}
        </div>
      ) : (
        <div className="text-center py-8 text-gray-400">
          <svg className="h-12 w-12 mx-auto mb-3 text-gray-200" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
            <circle cx="12" cy="12" r="10" />
            <line x1="12" y1="8" x2="12" y2="12" />
            <line x1="12" y1="16" x2="12.01" y2="16" />
          </svg>
          <p className="text-sm">No signals yet</p>
          <p className="text-xs mt-1">Signals will appear when trades occur</p>
        </div>
      )}

      {hasMore && (
        <div className="mt-4 pt-3 border-t border-gray-100 text-center">
          <span className="text-xs text-gray-400">
            +{signals.length - maxDisplay} more signals
          </span>
        </div>
      )}
    </div>
  );
};

export default SignalTimeline;

/**
 * AlgoStatusCard Component
 * 
 * Displays the algorithm status with uptime and connection health.
 * Shows a visual indicator when data becomes stale.
 */

'use client';

import React, { useMemo } from 'react';
import { clsx, type ClassValue } from 'clsx';
import type { AlgoStatus } from './types';

interface AlgoStatusCardProps {
  status: AlgoStatus;
  uptimeSeconds: number | null;
  lastUpdated: string | null;
  isLoading?: boolean;
  onRefresh?: () => void;
}

// SVG Icons as components
const PlayIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <polygon points="5 3 19 12 5 21 5 3" />
  </svg>
);

const PauseIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="6" y="4" width="4" height="16" />
    <rect x="14" y="4" width="4" height="16" />
  </svg>
);

const SquareIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="3" width="18" height="18" rx="2" ry="2" />
  </svg>
);

const AlertIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="10" />
    <line x1="12" y1="8" x2="12" y2="12" />
    <line x1="12" y1="16" x2="12.01" y2="16" />
  </svg>
);

const ClockIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="10" />
    <polyline points="12 6 12 12 16 14" />
  </svg>
);

const RefreshIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <polyline points="23 4 23 10 17 10" />
    <polyline points="1 20 1 14 7 14" />
    <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
  </svg>
);

const STATUS_CONFIG: Record<AlgoStatus, { label: string; colorClass: string; bgClass: string; icon: React.ReactNode }> = {
  running: {
    label: 'Running',
    colorClass: 'text-emerald-500',
    bgClass: 'bg-emerald-500',
    icon: <PlayIcon className="h-4 w-4" />,
  },
  paused: {
    label: 'Paused',
    colorClass: 'text-amber-500',
    bgClass: 'bg-amber-500',
    icon: <PauseIcon className="h-4 w-4" />,
  },
  stopped: {
    label: 'Stopped',
    colorClass: 'text-gray-500',
    bgClass: 'bg-gray-500',
    icon: <SquareIcon className="h-4 w-4" />,
  },
  error: {
    label: 'Error',
    colorClass: 'text-red-500',
    bgClass: 'bg-red-500',
    icon: <AlertIcon className="h-4 w-4" />,
  },
};

function formatUptime(seconds: number | null): string {
  if (seconds === null || seconds === undefined) return '--:--:--';
  
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  
  return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
}

function formatLastUpdated(timestamp: string | null): string {
  if (!timestamp) return 'Never';
  
  const date = new Date(timestamp);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSecs = Math.floor(diffMs / 1000);
  
  if (diffSecs < 60) return `${diffSecs}s ago`;
  if (diffSecs < 3600) return `${Math.floor(diffSecs / 60)}m ago`;
  if (diffSecs < 86400) return `${Math.floor(diffSecs / 3600)}h ago`;
  return date.toLocaleTimeString();
}

export const AlgoStatusCard: React.FC<AlgoStatusCardProps> = ({
  status,
  uptimeSeconds,
  lastUpdated,
  isLoading = false,
  onRefresh,
}) => {
  const config = STATUS_CONFIG[status];
  
  const isStale = useMemo(() => {
    if (!lastUpdated) return false;
    const lastUpdate = new Date(lastUpdated).getTime();
    const now = Date.now();
    return now - lastUpdate > 60000; // 1 minute
  }, [lastUpdated]);

  return (
    <div className={clsx(
      "relative overflow-hidden rounded-xl border bg-white shadow-sm transition-all duration-300",
      isStale && "border-amber-400 shadow-amber-100"
    )}>
      {/* Status indicator strip */}
      <div className={clsx(
        "absolute left-0 top-0 bottom-0 w-1.5 transition-colors duration-300",
        config.bgClass
      )} />
      
      <div className="p-5 pl-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-sm font-medium text-gray-500">
            Algorithm Status
          </h3>
          <div className="flex items-center gap-2">
            {isStale && (
              <span className="px-2 py-0.5 text-xs font-medium rounded-full border border-amber-400 text-amber-600 bg-amber-50">
                Stale
              </span>
            )}
            {onRefresh && (
              <button
                onClick={onRefresh}
                disabled={isLoading}
                className={clsx(
                  "p-1.5 rounded-md transition-colors hover:bg-gray-100",
                  isLoading && "animate-spin"
                )}
                aria-label="Refresh"
              >
                <RefreshIcon className="h-4 w-4 text-gray-500" />
              </button>
            )}
          </div>
        </div>
        
        <div className="space-y-4">
          {/* Main status display */}
          <div className="flex items-center gap-3">
            <div className={clsx(
              "h-12 w-12 rounded-full flex items-center justify-center",
              "bg-opacity-10",
              config.colorClass.replace('text-', 'bg-').replace('500', '100')
            )}>
              <div className={config.colorClass}>
                {config.icon}
              </div>
            </div>
            <div>
              <div className={clsx(
                "text-2xl font-bold",
                config.colorClass
              )}>
                {config.label}
              </div>
              <div className="text-xs text-gray-400">
                Survivor Strategy
              </div>
            </div>
          </div>
          
          {/* Uptime display */}
          <div className="flex items-center gap-2 text-sm">
            <ClockIcon className="h-4 w-4 text-gray-400" />
            <span className="text-gray-500">Uptime:</span>
            <span className="font-mono font-medium text-gray-700">
              {formatUptime(uptimeSeconds)}
            </span>
          </div>
          
          {/* Last updated */}
          <div className="text-xs text-gray-400">
            Last updated: {formatLastUpdated(lastUpdated)}
          </div>
          
          {/* Pulse animation for running state */}
          {status === 'running' && (
            <div className="flex items-center gap-2">
              <div className="relative flex h-2.5 w-2.5">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
                <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-emerald-500" />
              </div>
              <span className="text-xs text-emerald-600 font-medium">Live</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default AlgoStatusCard;

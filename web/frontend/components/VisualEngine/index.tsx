/**
 * VisualEngine Component
 * 
 * Main container component for the Visual Engine dashboard.
 * Fetches and displays real-time strategy state including:
 * - Algorithm status and uptime
 * - Entry filters (RSI, EMA, ADX, Gap Risk)
 * - Entry predictions for PE/CE options
 * - Recent signals timeline
 * - Daily trading statistics
 */

'use client';

import React, { useState, useEffect, useCallback, useRef } from 'react';
import { clsx } from 'clsx';
import { strategyAPI } from '@/lib/api';
import type { VisualStateResponse } from '@/types';

import { AlgoStatusCard } from './AlgoStatusCard';
import { FilterPanel } from './FilterPanel';
import { EntryRadar } from './EntryRadar';
import { SignalTimeline } from './SignalTimeline';
import { DailyStats } from './DailyStats';
import type { AlgoStatus } from './types';

// Refresh icon SVG
const RefreshIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polyline points="23 4 23 10 17 10" />
    <polyline points="1 20 1 14 7 14" />
    <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15" />
  </svg>
);

const AlertIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <circle cx="12" cy="12" r="10" />
    <line x1="12" y1="8" x2="12" y2="12" />
    <line x1="12" y1="16" x2="12.01" y2="16" />
  </svg>
);

// Convert API status to component status
const mapStatus = (isRunning: boolean): AlgoStatus => {
  return isRunning ? 'running' : 'stopped';
};

interface VisualEngineProps {
  refreshInterval?: number; // milliseconds
  onError?: (error: Error) => void;
}

export const VisualEngine: React.FC<VisualEngineProps> = ({
  refreshInterval = 10000, // 10 seconds default
  onError,
}) => {
  const [visualState, setVisualState] = useState<VisualStateResponse | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const intervalRef = useRef<NodeJS.Timeout | null>(null);

  const fetchVisualState = useCallback(async (showLoading = false) => {
    if (showLoading) setIsLoading(true);
    
    try {
      const data = await strategyAPI.getVisualState();
      setVisualState(data);
      setLastUpdated(new Date());
      setError(null);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to fetch visual state';
      setError(errorMessage);
      onError?.(err instanceof Error ? err : new Error(errorMessage));
    } finally {
      if (showLoading) setIsLoading(false);
    }
  }, [onError]);

  // Initial fetch
  useEffect(() => {
    fetchVisualState(true);
  }, [fetchVisualState]);

  // Set up polling
  useEffect(() => {
    intervalRef.current = setInterval(() => {
      fetchVisualState(false);
    }, refreshInterval);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [fetchVisualState, refreshInterval]);

  // Handle manual refresh
  const handleRefresh = useCallback(() => {
    fetchVisualState(true);
  }, [fetchVisualState]);

  // Loading state
  if (isLoading && !visualState) {
    return (
      <div className="w-full max-w-7xl mx-auto p-4">
        <div className="animate-pulse space-y-4">
          <div className="h-8 bg-gray-200 rounded w-1/4"></div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-40 bg-gray-100 rounded-xl"></div>
            ))}
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div className="h-64 bg-gray-100 rounded-xl"></div>
            <div className="h-64 bg-gray-100 rounded-xl"></div>
          </div>
        </div>
      </div>
    );
  }

  // Error state
  if (error && !visualState) {
    return (
      <div className="w-full max-w-7xl mx-auto p-4">
        <div className="rounded-xl border border-red-200 bg-red-50 p-6 text-center">
          <AlertIcon className="h-12 w-12 text-red-400 mx-auto mb-3" />
          <h3 className="text-lg font-semibold text-red-700 mb-2">
            Failed to Load Visual Engine
          </h3>
          <p className="text-sm text-red-600 mb-4">{error}</p>
          <button
            onClick={handleRefresh}
            className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  const state = visualState?.data;
  const isRunning = state?.is_running ?? false;
  const uptimeSeconds = state?.uptime_seconds ?? null;

  return (
    <div className="w-full max-w-7xl mx-auto p-4 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-gray-800">Visual Engine</h2>
          <p className="text-sm text-gray-500">
            Real-time strategy visualization and entry predictions
          </p>
        </div>
        
        <div className="flex items-center gap-3">
          {/* Last updated indicator */}
          {lastUpdated && (
            <span className="text-xs text-gray-400">
              Updated {lastUpdated.toLocaleTimeString()}
            </span>
          )}
          
          {/* Refresh button */}
          <button
            onClick={handleRefresh}
            disabled={isLoading}
            className={clsx(
              "flex items-center gap-2 px-3 py-2 text-sm font-medium rounded-lg",
              "bg-white border border-gray-200 text-gray-600",
              "hover:bg-gray-50 hover:border-gray-300 transition-colors",
              isLoading && "opacity-50 cursor-not-allowed"
            )}
          >
            <RefreshIcon className={clsx("h-4 w-4", isLoading && "animate-spin")} />
            Refresh
          </button>
        </div>
      </div>

      {/* Error banner (non-blocking) */}
      {error && (
        <div className="rounded-lg bg-amber-50 border border-amber-200 p-3 flex items-center gap-2">
          <AlertIcon className="h-4 w-4 text-amber-500 flex-shrink-0" />
          <p className="text-sm text-amber-700">
            Data may be stale: {error}
          </p>
        </div>
      )}

      {/* Strategy not running warning */}
      {visualState && !isRunning && (
        <div className="rounded-lg bg-blue-50 border border-blue-200 p-4 flex items-start gap-3">
          <AlertIcon className="h-5 w-5 text-blue-500 flex-shrink-0 mt-0.5" />
          <div>
            <p className="text-sm font-medium text-blue-700">
              Strategy Not Running
            </p>
            <p className="text-sm text-blue-600 mt-1">
              Start the strategy to see live data and entry predictions.
            </p>
          </div>
        </div>
      )}

      {/* Main dashboard grid */}
      {state && (
        <>
          {/* Top row: Status, Filters */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
            <AlgoStatusCard
              status={mapStatus(isRunning)}
              uptimeSeconds={uptimeSeconds}
              lastUpdated={lastUpdated?.toISOString() ?? null}
              isLoading={isLoading}
              onRefresh={handleRefresh}
            />
            
            <div className="lg:col-span-2">
              <FilterPanel filters={state.filters.items} />
            </div>
          </div>

          {/* Middle row: Entry Radar */}
          <EntryRadar
            pePrediction={state.predictions.pe}
            cePrediction={state.predictions.ce}
            marketTrend={state.market_context.trend?.toLowerCase() as 'bullish' | 'bearish' | 'neutral'}
          />

          {/* Bottom row: Signals and Stats */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <SignalTimeline signals={state.signals} />
            <DailyStats stats={state.daily_stats} />
          </div>
        </>
      )}
    </div>
  );
};

export default VisualEngine;

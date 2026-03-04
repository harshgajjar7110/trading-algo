/**
 * VisualEngine Component Types
 * 
 * Component-specific type definitions for the Visual Engine dashboard.
 * These types extend or specialize the base types from '@/types'.
 */

import type {
  FilterStatus,
  EntryPrediction,
  EntrySignal,
  DailyStats,
  MarketContext,
} from '@/types';

// ============================================================================
// Filter Panel Types
// ============================================================================

export interface FilterConfig {
  id: string;
  name: string;
  description: string;
  icon: string;
  color: 'green' | 'red' | 'yellow' | 'blue' | 'gray';
}

export interface FilterDisplayData {
  config: FilterConfig;
  status: FilterStatus;
}

// ============================================================================
// Entry Radar Types
// ============================================================================

export interface RadarOption {
  type: 'PE' | 'CE';
  label: string;
  prediction: EntryPrediction | null;
  isActive: boolean;
  distancePercent: number;
}

export interface RadarState {
  pe: RadarOption;
  ce: RadarOption;
  marketTrend: 'bullish' | 'bearish' | 'neutral';
}

// ============================================================================
// Signal Timeline Types
// ============================================================================

export interface TimelineSignal {
  id: string;
  timestamp: string;
  optionType: 'PE' | 'CE';
  action: 'ENTRY' | 'EXIT';
  price: number;
  reason: string;
  isSuccess?: boolean;
}

// ============================================================================
// Status Card Types
// ============================================================================

export type AlgoStatus = 'running' | 'paused' | 'stopped' | 'error';

export interface StatusDisplayConfig {
  status: AlgoStatus;
  uptime: number | null;
  lastUpdate: string | null;
  isStale: boolean;
}

// ============================================================================
// Daily Stats Types
// ============================================================================

export interface StatsCardData {
  label: string;
  value: string | number;
  change?: number;
  trend?: 'up' | 'down' | 'neutral';
  prefix?: string;
  suffix?: string;
  decimals?: number;
}

// ============================================================================
// Visual Engine Props
// ============================================================================

export interface VisualEngineProps {
  refreshInterval?: number; // milliseconds
  onError?: (error: Error) => void;
}

export interface VisualEngineState {
  isLoading: boolean;
  error: string | null;
  lastUpdated: Date | null;
  visualState: {
    is_running: boolean;
    uptime_seconds: number | null;
    filters: Record<string, FilterStatus>;
    predictions: {
      pe: EntryPrediction | null;
      ce: EntryPrediction | null;
    };
    signals: EntrySignal[];
    market_context: MarketContext;
    daily_stats: DailyStats;
  } | null;
}

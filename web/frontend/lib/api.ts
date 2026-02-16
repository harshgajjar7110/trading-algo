/**
 * API Client for the Survivor Trading Backend
 */

import {
  StrategyState,
  StrategyConfig,
  StrategyStartResponse,
  StrategyStopResponse,
  ConfigValidationResponse,
  PositionsResponse,
  OrdersResponse,
  TradesResponse,
  Quote,
  NiftyData,
  Funds,
} from '@/types';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

/**
 * Generic fetch wrapper with error handling
 */
async function fetchAPI<T>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE}${endpoint}`;
  
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ message: 'Unknown error' }));
    throw new Error(error.message || `HTTP error! status: ${response.status}`);
  }

  return response.json();
}

// =============================================================================
// Strategy API
// =============================================================================

export const strategyAPI = {
  /**
   * Get current strategy status
   */
  getStatus: () => fetchAPI<StrategyState>('/api/strategy/status'),

  /**
   * Start the strategy
   */
  start: (configOverride?: Partial<StrategyConfig>) =>
    fetchAPI<StrategyStartResponse>('/api/strategy/start', {
      method: 'POST',
      body: JSON.stringify({ config_override: configOverride }),
    }),

  /**
   * Stop the strategy
   */
  stop: () =>
    fetchAPI<StrategyStopResponse>('/api/strategy/stop', {
      method: 'POST',
    }),

  /**
   * Restart the strategy
   */
  restart: (configOverride?: Partial<StrategyConfig>) =>
    fetchAPI<StrategyStartResponse>('/api/strategy/restart', {
      method: 'POST',
      body: JSON.stringify({ config_override: configOverride }),
    }),
};

// =============================================================================
// Configuration API
// =============================================================================

export const configAPI = {
  /**
   * Get current configuration
   */
  get: () => fetchAPI<StrategyConfig>('/api/config'),

  /**
   * Update configuration
   */
  update: (updates: Partial<StrategyConfig>) =>
    fetchAPI<StrategyConfig>('/api/config', {
      method: 'PUT',
      body: JSON.stringify(updates),
    }),

  /**
   * Validate configuration
   */
  validate: (config: StrategyConfig) =>
    fetchAPI<ConfigValidationResponse>('/api/config/validate', {
      method: 'POST',
      body: JSON.stringify(config),
    }),

  /**
   * Reset configuration to defaults
   */
  reset: () =>
    fetchAPI<StrategyConfig>('/api/config/reset', {
      method: 'POST',
    }),
};

// =============================================================================
// Positions & Orders API
// =============================================================================

export const tradingAPI = {
  /**
   * Get all positions
   */
  getPositions: () => fetchAPI<PositionsResponse>('/api/positions'),

  /**
   * Get all orders
   */
  getOrders: () => fetchAPI<OrdersResponse>('/api/orders'),

  /**
   * Get all trades
   */
  getTrades: () => fetchAPI<TradesResponse>('/api/trades'),
};

// =============================================================================
// Market Data API
// =============================================================================

export const marketAPI = {
  /**
   * Get quote for a symbol
   */
  getQuote: (symbol: string) =>
    fetchAPI<Quote>(`/api/market/quote/${encodeURIComponent(symbol)}`),

  /**
   * Get NIFTY data with strategy context
   */
  getNiftyData: () => fetchAPI<NiftyData>('/api/market/nifty'),

  /**
   * Get account funds
   */
  getFunds: () => fetchAPI<Funds>('/api/market/funds'),
};

// =============================================================================
// Health Check
// =============================================================================

export const healthAPI = {
  check: () => fetchAPI<{ status: string; version: string }>('/health'),
};

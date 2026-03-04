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
  GreeksResponse,
  ScenarioAnalysis,
  LoginUrlResponse,
  AuthCallbackResponse,
  AuthStatusResponse,
  StrategyInfo,
  StrategyDetails,
  StrategyPreview,
  CurrentStrategy,
  VisualStateResponse,
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
  
  console.log(`[API] ${options.method || 'GET'} ${url}`);
  
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });

  console.log(`[API] Response: ${response.status} ${response.statusText}`);

  if (!response.ok) {
    const error = await response.json().catch(() => ({ message: `HTTP ${response.status}: ${response.statusText}` }));
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

  /**
   * Get visual state for the Visual Engine dashboard
   */
  getVisualState: () => fetchAPI<VisualStateResponse>('/api/strategy/visual-state'),

  /**
   * Refresh visual state cache
   */
  refreshVisualState: () =>
    fetchAPI<VisualStateResponse>('/api/strategy/visual-state/refresh', {
      method: 'POST',
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
// Strategy Selector API (V2)
// =============================================================================

export const strategySelectorAPI = {
  /**
   * Get available strategies
   */
  getAvailableStrategies: () =>
    fetchAPI<{ strategies: StrategyInfo[]; current: CurrentStrategy | null }>('/api/strategy/available'),

  /**
   * Get strategy details
   */
  getStrategyDetails: (strategyId: string) =>
    fetchAPI<StrategyDetails>(`/api/strategy/${encodeURIComponent(strategyId)}/details`),

  /**
   * Preview strategy configuration before starting
   */
  previewConfig: (strategyId: string, configOverride?: Partial<StrategyConfig>) =>
    fetchAPI<StrategyPreview>('/api/strategy/preview-config', {
      method: 'POST',
      body: JSON.stringify({ strategy_id: strategyId, config_override: configOverride }),
    }),

  /**
   * Start strategy (with optional confirmation)
   */
  start: (strategyId: string, configOverride?: Partial<StrategyConfig>, confirmed?: boolean) =>
    fetchAPI<StrategyStartResponse & { requires_confirmation?: boolean; preview?: StrategyPreview }>('/api/strategy/start', {
      method: 'POST',
      body: JSON.stringify({ strategy_id: strategyId, config_override: configOverride, confirmed }),
    }),

  /**
   * Stop current strategy
   */
  stop: () =>
    fetchAPI<StrategyStopResponse>('/api/strategy/stop', { method: 'POST' }),

  /**
   * Restart strategy
   */
  restart: (strategyId?: string, configOverride?: Partial<StrategyConfig>, confirmed?: boolean) =>
    fetchAPI<StrategyStartResponse & { requires_confirmation?: boolean; preview?: StrategyPreview }>('/api/strategy/restart', {
      method: 'POST',
      body: JSON.stringify({ strategy_id: strategyId, config_override: configOverride, confirmed }),
    }),

  /**
   * Get current running strategy
   */
  getCurrentStrategy: () =>
    fetchAPI<CurrentStrategy>('/api/strategy/current'),

  /**
   * Compare all strategies
   */
  compareStrategies: () =>
    fetchAPI<StrategyInfo[]>('/api/strategy/compare'),

  /**
   * Get full strategy status (state + current strategy)
   */
  getStatus: () =>
    fetchAPI<{ state: StrategyState; current_strategy: CurrentStrategy; available_strategies: Record<string, string> }>('/api/strategy/status'),
};

// =============================================================================
// Greeks API
// =============================================================================

export const greeksAPI = {
  /**
   * Get portfolio Greeks (Delta, Gamma, Theta, Vega, Rho)
   */
  getPortfolioGreeks: (params?: { underlying_price?: number; risk_free_rate?: number }) =>
    fetchAPI<GreeksResponse>(`/api/greeks/portfolio?${new URLSearchParams(params as Record<string, string>).toString()}`),

  /**
   * Get scenario analysis for portfolio Greeks
   */
  getScenarioAnalysis: (params?: { days_forward?: number; risk_free_rate?: number }) =>
    fetchAPI<ScenarioAnalysis>(`/api/greeks/scenario?${new URLSearchParams(params as Record<string, string>).toString()}`),

  /**
   * Get Greeks for a specific position
   */
  getPositionGreeks: (symbol: string, params?: { underlying_price?: number }) =>
    fetchAPI<GreeksResponse>(`/api/greeks/position/${encodeURIComponent(symbol)}?${new URLSearchParams(params as Record<string, string>).toString()}`),
};

// =============================================================================
// Health Check
// =============================================================================

export const healthAPI = {
  check: () => fetchAPI<{ status: string; version: string }>('/health'),
};

// =============================================================================
// Authentication API
// =============================================================================

export const authAPI = {
  /**
   * Get current authentication status
   */
  getStatus: () => fetchAPI<AuthStatusResponse>('/api/auth/status'),

  /**
   * Get login URL for broker authentication
   */
  getLoginUrl: () => fetchAPI<LoginUrlResponse>('/api/auth/login-url'),

  /**
   * Submit request token after broker login
   */
  submitToken: (request_token: string) =>
    fetchAPI<AuthCallbackResponse>('/api/auth/callback', {
      method: 'POST',
      body: JSON.stringify({ request_token }),
    }),

  /**
   * Verify that the current token is valid
   */
  verifyToken: () => fetchAPI<TokenVerifyResponse>('/api/auth/verify'),
};

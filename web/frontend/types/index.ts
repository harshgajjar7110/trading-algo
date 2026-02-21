/**
 * TypeScript types for the Survivor Trading UI
 */

// =============================================================================
// Enums
// =============================================================================

export enum StrategyStatus {
  STOPPED = 'STOPPED',
  STARTING = 'STARTING',
  RUNNING = 'RUNNING',
  STOPPING = 'STOPPING',
  ERROR = 'ERROR',
}

export enum OrderStatus {
  PENDING = 'PENDING',
  OPEN = 'OPEN',
  COMPLETE = 'COMPLETE',
  CANCELLED = 'CANCELLED',
  REJECTED = 'REJECTED',
}

export enum TransactionType {
  BUY = 'BUY',
  SELL = 'SELL',
}

// =============================================================================
// Strategy Types
// =============================================================================

export interface StrategyState {
  status: StrategyStatus;
  nifty_pe_last_value: number | null;
  nifty_ce_last_value: number | null;
  pe_reset_flag: boolean;
  ce_reset_flag: boolean;
  last_update: string | null;
  error_message: string | null;
  uptime_seconds: number | null;
}

export interface StrategyStartResponse {
  success: boolean;
  message: string;
  status: StrategyStatus;
}

export interface StrategyStopResponse {
  success: boolean;
  message: string;
  status: StrategyStatus;
}

// =============================================================================
// Configuration Types
// =============================================================================

export interface StrategyConfig {
  // Core Parameters
  index_symbol: string;
  symbol_initials: string;

  // Gap Parameters
  pe_gap: number;
  ce_gap: number;
  pe_reset_gap: number;
  ce_reset_gap: number;

  // Strike Selection
  pe_symbol_gap: number;
  ce_symbol_gap: number;

  // Position Sizing
  pe_quantity: number;
  ce_quantity: number;

  // Risk Management
  min_price_to_sell: number;
  sell_multiplier_threshold: number;

  // Reference Points
  pe_start_point: number;
  ce_start_point: number;

  // Order Settings
  exchange: string;
  order_type: string;
  product_type: string;
  trans_type: string;
  tag: string;

  // Entry Filters
  entry_filter_type: string;
  rsi_period: number;
  rsi_min: number;
  rsi_max: number;
  adx_period: number;
  adx_threshold: number;
  ema_period: number;
  history_period_days: number;
}

export interface ConfigValidationResponse {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

// =============================================================================
// Strategy Selector Types (V2)
// =============================================================================

export interface StrategyInfo {
  id: string;
  name: string;
  description: string;
  risk_level: 'low' | 'medium' | 'medium-high' | 'high';
  recommended_capital: string;
  tags: string[];
  key_params: string[];
}

export interface StrategyDetails {
  id: string;
  name: string;
  description: string;
  version: string;
  risk_level: string;
  recommended_capital: string;
  tags: string[];
  default_params: Record<string, number | string>;
  current_config: StrategyConfig;
  config_path: string;
}

export interface StrategyConfigDifference {
  param: string;
  default: number | string;
  current: number | string;
}

export interface StrategyPreview {
  strategy_id: string;
  strategy_name: string;
  config: StrategyConfig;
  is_valid: boolean;
  validation_errors: string[];
  differences_from_default: StrategyConfigDifference[];
  risk_level: string;
  recommended_capital: string;
}

export interface CurrentStrategy {
  running: boolean;
  id?: string;
  strategy_id?: string;
  name?: string;
  status?: string;
  started_at?: string;
  pid?: number;
  config_summary?: Record<string, number | string>;
  message?: string;
}

// =============================================================================
// Position Types
// =============================================================================

export interface Position {
  symbol: string;
  exchange: string;
  quantity: number;
  average_price: number;
  current_price: number | null;
  pnl: number | null;
  pnl_percent: number | null;
  product_type: string;
  side: string;
}

export interface PositionsResponse {
  positions: Position[];
  total_pnl: number;
  total_pnl_percent: number;
  error?: string;  // Error message if positions couldn't be fetched
}

// =============================================================================
// Order Types
// =============================================================================

export interface Order {
  order_id: string;
  symbol: string;
  exchange: string;
  transaction_type: TransactionType;
  quantity: number;
  price: number;
  status: OrderStatus;
  order_type: string;
  product_type: string;
  timestamp: string;
  tag: string | null;
  filled_quantity: number | null;
  average_price: number | null;
}

export interface OrdersResponse {
  orders: Order[];
  count: number;
}

export interface Trade {
  trade_id: string;
  order_id: string;
  symbol: string;
  exchange: string;
  transaction_type: TransactionType;
  quantity: number;
  price: number;
  timestamp: string;
  tag: string | null;
}

export interface TradesResponse {
  trades: Trade[];
  count: number;
}

// =============================================================================
// Market Data Types
// =============================================================================

export interface Quote {
  symbol: string;
  exchange: string;
  last_price: number;
  change: number | null;
  change_percent: number | null;
  bid: number | null;
  ask: number | null;
  volume: number | null;
  high: number | null;
  low: number | null;
  open: number | null;
  close: number | null;
  timestamp: string | null;
}

export interface NiftyData {
  quote: Quote;
  pe_reference: number | null;
  ce_reference: number | null;
  pe_gap_to_trigger: number | null;
  ce_gap_to_trigger: number | null;
}

export interface Funds {
  equity: number;
  available_cash: number;
  used_margin: number;
  net: number;
}

// =============================================================================
// WebSocket Types
// =============================================================================

export interface WSMessage {
  type: string;
  data: Record<string, unknown>;
  timestamp: string;
}

export interface PriceUpdate {
  symbol: string;
  last_price: number;
  change: number | null;
  timestamp: string;
}

export interface OrderUpdate {
  order_id: string;
  symbol: string;
  status: OrderStatus;
  quantity: number;
  filled_quantity: number | null;
  price: number | null;
  timestamp: string;
}

// =============================================================================
// API Error Types
// =============================================================================

export interface APIError {
  code: string;
  message: string;
  details: Record<string, unknown> | null;
}

export interface HTTPError {
  error: APIError;
}

// =============================================================================
// Payoff Analysis Types
// =============================================================================

export enum OptionType {
  CALL = 'CE',
  PUT = 'PE',
}

export interface PayoffPoint {
  underlying_price: number;
  pnl: number;
  pnl_percent: number;
}

export interface PositionPayoff {
  symbol: string;
  option_type: OptionType;
  strike_price: number;
  side: string;
  quantity: number;
  premium: number;
  breakeven: number;
  breakeven_percent: number;
  max_profit: number | null;
  max_loss: number | null;
  current_underlying_price: number;
  current_pnl: number;
  payoff_curve: PayoffPoint[];
}

export interface PortfolioPayoff {
  positions: PositionPayoff[];
  combined_payoff_curve: PayoffPoint[];
  combined_breakeven_points: number[];
  total_max_profit: number | null;
  total_max_loss: number | null;
  current_nifty_price: number;
  total_current_pnl: number;
  price_range_start: number;
  price_range_end: number;
}

// =============================================================================
// Greeks Types
// =============================================================================

export interface PositionGreeks {
  symbol: string;
  option_type: string;
  strike: number;
  quantity: number;
  entry_price: number;
  delta: number;
  gamma: number;
  theta: number;
  vega: number;
  rho: number;
  expiry_date?: string;
  index_name?: string;
}

export interface PortfolioGreeks {
  delta: number;
  gamma: number;
  theta: number;
  vega: number;
  rho: number;
}

export interface GreeksInterpretation {
  delta: string;
  gamma: string;
  theta: string;
  vega: string;
  rho: string;
}

export interface GreeksResponse {
  positions: PositionGreeks[];
  portfolio: PortfolioGreeks;
  interpretation: GreeksInterpretation;
  underlying_price: number;
  calculation_date: string;
}

export interface ScenarioResult {
  price_change_pct: number;
  price_change_points: number;
  volatility_change: number;
  days_forward: number;
  estimated_pnl: number;
  breakdown: {
    delta_pnl: number;
    gamma_pnl: number;
    theta_pnl: number;
    vega_pnl: number;
  };
}

export interface ScenarioAnalysis {
  current_greeks: PortfolioGreeks;
  scenarios: ScenarioResult[];
  underlying_price: number;
  days_forward: number;
}

// =============================================================================
// Authentication Types
// =============================================================================

export interface LoginUrlResponse {
  success: boolean;
  login_url: string | null;
  broker: string;
  message: string;
}

export interface AuthCallbackResponse {
  success: boolean;
  message: string;
  access_token: string | null;
}

export interface AuthStatusResponse {
  authenticated: boolean;
  broker: string;
  message: string;
}

export interface TokenVerifyResponse {
  valid: boolean;
  message: string;
  profile?: Record<string, unknown>;
}

'use client';

import { useEffect, useState, useCallback } from 'react';
import Link from 'next/link';
import { StrategyConfig, ConfigValidationResponse } from '@/types';
import { configAPI } from '@/lib/api';

// =============================================================================
// Types
// =============================================================================
type ConfigFieldType = 'text' | 'number' | 'select' | 'boolean';

interface ConfigField {
  key: keyof StrategyConfig;
  label: string;
  type: ConfigFieldType;
  options?: string[];
  description: string;
  min?: number;
  max?: number;
  step?: number;
}

interface ConfigCategory {
  id: string;
  title: string;
  icon: string;
  description: string;
  fields: ConfigField[];
}

// =============================================================================
// CONFIGURATION CATEGORIES (15 Enhanced Categories)
// =============================================================================
const CONFIG_CATEGORIES: ConfigCategory[] = [
  {
    id: 'symbol-market',
    title: 'Symbol & Market Settings',
    icon: '📊',
    description: 'Core trading symbol and market configuration',
    fields: [
      { key: 'index_symbol', label: 'Index Symbol', type: 'text', description: 'Underlying index for tracking (e.g., NSE:NIFTY 50)' },
      { key: 'symbol_initials', label: 'Symbol Initials', type: 'text', description: 'Option series identifier (e.g., NIFTY26FEB)' },
      { key: 'exchange', label: 'Exchange', type: 'select', options: ['NFO', 'BFO', 'MCX'], description: 'Exchange for trading' },
      { key: 'product_type', label: 'Product Type', type: 'select', options: ['NRML', 'MIS', 'CNC'], description: 'Product type for orders (NRML = overnight, MIS = intraday)' },
      { key: 'order_type', label: 'Order Type', type: 'select', options: ['MARKET', 'LIMIT'], description: 'Default order type for execution' },
      { key: 'trans_type', label: 'Transaction Type', type: 'select', options: ['SELL', 'BUY'], description: 'Transaction type for strategy orders (usually SELL for option writing)' },
      { key: 'tag', label: 'Order Tag', type: 'text', description: 'Tag to identify strategy orders in broker system' },
    ],
  },
  {
    id: 'strike-selection',
    title: 'Strike Selection',
    icon: '🎯',
    description: 'Configure strike price selection based on distance from spot',
    fields: [
      { key: 'pe_symbol_gap', label: 'PE Strike Distance', type: 'number', description: 'Points below current price for PE strike selection (e.g., 600 = 6 strikes for NIFTY)', min: 0, step: 50 },
      { key: 'ce_symbol_gap', label: 'CE Strike Distance', type: 'number', description: 'Points above current price for CE strike selection (e.g., 600 = 6 strikes for NIFTY)', min: 0, step: 50 },
    ],
  },
  {
    id: 'reference-points',
    title: 'Reference Points',
    icon: '📍',
    description: 'Starting reference values for price movement tracking',
    fields: [
      { key: 'pe_start_point', label: 'PE Start Point', type: 'number', description: 'Initial PE reference value (0 = use current market price at strategy start)', min: 0, step: 100 },
      { key: 'ce_start_point', label: 'CE Start Point', type: 'number', description: 'Initial CE reference value (0 = use current market price at strategy start)', min: 0, step: 100 },
    ],
  },
  {
    id: 'price-gaps',
    title: 'Price Gaps (Movement Thresholds)',
    icon: '📈',
    description: 'Price movement thresholds for triggering trades and resets',
    fields: [
      { key: 'pe_gap', label: 'PE Gap (Trigger)', type: 'number', description: 'NIFTY upward movement threshold to trigger PE sells (in points)', min: 1, step: 5 },
      { key: 'ce_gap', label: 'CE Gap (Trigger)', type: 'number', description: 'NIFTY downward movement threshold to trigger CE sells (in points)', min: 1, step: 5 },
      { key: 'pe_reset_gap', label: 'PE Reset Gap', type: 'number', description: 'Favorable downward movement to reset PE reference (in points)', min: 1, step: 5 },
      { key: 'ce_reset_gap', label: 'CE Reset Gap', type: 'number', description: 'Favorable upward movement to reset CE reference (in points)', min: 1, step: 5 },
      { key: 'sell_multiplier_threshold', label: 'Sell Multiplier Threshold', type: 'number', description: 'Maximum allowed position multiplier for pyramid scaling', min: 1, step: 0.5 },
    ],
  },
  {
    id: 'quantity-settings',
    title: 'Quantity Settings',
    icon: '💼',
    description: 'Base position sizing for each option type',
    fields: [
      { key: 'pe_quantity', label: 'PE Quantity', type: 'number', description: 'Base quantity for PE (Put) option trades', min: 1, step: 1 },
      { key: 'ce_quantity', label: 'CE Quantity', type: 'number', description: 'Base quantity for CE (Call) option trades', min: 1, step: 1 },
    ],
  },
  {
    id: 'entry-filters',
    title: 'Entry Filters',
    icon: '🚦',
    description: 'Filters to control when trades can be entered',
    fields: [
      { key: 'entry_filter_type', label: 'Entry Filter Type', type: 'select', options: ['NONE', 'RSI', 'EMA', 'ADX', 'ALL'], description: 'Type of entry filter to use (NONE = no filters, ALL = all filters must pass)' },
      { key: 'min_price_to_sell', label: 'Min Premium to Sell', type: 'number', description: 'Minimum option premium threshold for execution (filters out low-premium trades)', min: 1, step: 1 },
      { key: 'history_period_days', label: 'History Period (Days)', type: 'number', description: 'Days of historical data to load for indicator calculations', min: 1, max: 30, step: 1 },
    ],
  },
  {
    id: 'technical-indicators',
    title: 'Technical Indicators',
    icon: '📉',
    description: 'RSI, EMA, ADX, and ATR indicator settings',
    fields: [
      { key: 'rsi_period', label: 'RSI Period', type: 'number', description: 'RSI calculation period (typically 14)', min: 2, max: 50, step: 1 },
      { key: 'rsi_min', label: 'RSI Minimum', type: 'number', description: 'Minimum RSI for entry - blocks CE sells when RSI < min (oversold)', min: 0, max: 100, step: 5 },
      { key: 'rsi_max', label: 'RSI Maximum', type: 'number', description: 'Maximum RSI for entry - blocks PE sells when RSI > max (overbought)', min: 0, max: 100, step: 5 },
      { key: 'ema_period', label: 'EMA Period', type: 'number', description: 'EMA period for trend detection filter', min: 5, max: 200, step: 5 },
      { key: 'adx_period', label: 'ADX Period', type: 'number', description: 'ADX calculation period for trend strength measurement', min: 5, max: 50, step: 1 },
      { key: 'adx_threshold', label: 'ADX Threshold', type: 'number', description: 'Minimum ADX value to confirm strong trend (typically 25)', min: 10, max: 50, step: 5 },
      { key: 'atr_period', label: 'ATR Period', type: 'number', description: 'ATR period for volatility measurement', min: 5, max: 50, step: 1 },
      { key: 'atr_history_days', label: 'ATR History Days', type: 'number', description: 'Days of history to load for ATR calculations', min: 1, max: 10, step: 1 },
    ],
  },
  {
    id: 'position-limits',
    title: 'Position Limits',
    icon: '🛡️',
    description: 'Maximum position constraints for risk management',
    fields: [
      { key: 'max_positions_per_side', label: 'Max Positions Per Side', type: 'number', description: 'Maximum PE or CE positions separately (e.g., 3 = max 3 PE and 3 CE)', min: 1, max: 10, step: 1 },
      { key: 'max_total_positions', label: 'Max Total Positions', type: 'number', description: 'Maximum combined positions (PE + CE) across both sides', min: 1, max: 20, step: 1 },
      { key: 'max_consecutive_losses', label: 'Max Consecutive Losses', type: 'number', description: 'Pause trading after N consecutive losses (risk circuit breaker)', min: 1, max: 10, step: 1 },
    ],
  },
  {
    id: 'volatility-sizing',
    title: 'Volatility-Based Sizing',
    icon: '📊',
    description: 'Dynamic position sizing based on market volatility',
    fields: [
      { key: 'volatility_sizing', label: 'Enable Volatility Sizing', type: 'boolean', description: 'Enable automatic position size reduction in high volatility periods' },
      { key: 'high_vol_size_reduction', label: 'High Vol Reduction', type: 'number', description: 'Trade at X% of normal size in high volatility (0.5 = 50% size)', min: 0.1, max: 1, step: 0.1 },
      { key: 'enable_dynamic_gaps', label: 'Enable Dynamic Gaps', type: 'boolean', description: 'Enable ATR-based dynamic gap adjustment for adaptive thresholds' },
      { key: 'atr_multiplier_pe', label: 'ATR Multiplier PE', type: 'number', description: 'ATR multiplier for PE gap calculation when dynamic gaps enabled', min: 0.5, max: 20, step: 0.5 },
      { key: 'atr_multiplier_ce', label: 'ATR Multiplier CE', type: 'number', description: 'ATR multiplier for CE gap calculation when dynamic gaps enabled', min: 0.5, max: 20, step: 0.5 },
    ],
  },
  {
    id: 'profit-target',
    title: 'Profit Target Settings',
    icon: '💰',
    description: 'Automatic profit taking configuration',
    fields: [
      { key: 'profit_target_enabled', label: 'Enable Profit Target', type: 'boolean', description: 'Enable automatic profit target exits' },
      { key: 'profit_target_percent', label: 'Profit Target %', type: 'number', description: 'Exit when X% of premium collected is retained as profit', min: 10, max: 90, step: 5 },
      { key: 'profit_check_interval', label: 'Check Interval (sec)', type: 'number', description: 'Seconds between profit target evaluation checks', min: 5, max: 300, step: 5 },
    ],
  },
  {
    id: 'stop-loss',
    title: 'Stop-Loss Settings',
    icon: '🛑',
    description: 'Automated stop-loss order configuration',
    fields: [
      { key: 'sl_enabled', label: 'Enable Stop-Loss Orders', type: 'boolean', description: 'Enable automatic SL order placement with broker' },
      { key: 'sl_percentage', label: 'SL Percentage', type: 'number', description: 'SL at % of entry price (for shorts: entry × (1 + sl%/100))', min: 10, max: 200, step: 5 },
      { key: 'sl_order_type', label: 'SL Order Type', type: 'select', options: ['STOP', 'STOP_LIMIT'], description: 'STOP (SL-M) triggers at market, STOP_LIMIT (SL) places limit order' },
      { key: 'sl_limit_buffer', label: 'SL Limit Buffer', type: 'number', description: 'Points buffer for limit price (for SL orders)', min: 0, max: 1, step: 0.05 },
      { key: 'sl_reconcile_on_start', label: 'Reconcile on Start', type: 'boolean', description: 'Run SL reconciliation at algo startup to sync with broker' },
    ],
  },
  {
    id: 'trailing-stop',
    title: 'Trailing Stop Settings',
    icon: '🔄',
    description: 'Trailing stop-loss for protecting profits',
    fields: [
      { key: 'trailing_stop_enabled', label: 'Enable Trailing Stop', type: 'boolean', description: 'Enable trailing stop loss to protect profits' },
      { key: 'trailing_stop_distance', label: 'Trailing Distance %', type: 'number', description: 'Exit if profit falls X% from peak (protects running profits)', min: 0.1, max: 5, step: 0.1 },
      { key: 'stop_loss_multiplier', label: 'Legacy SL Multiplier', type: 'number', description: 'Exit if premium doubles (used when sl_enabled is false)', min: 1, max: 5, step: 0.5 },
    ],
  },
  {
    id: 'daily-risk',
    title: 'Daily Risk Management',
    icon: '⚠️',
    description: 'Daily loss limits and square-off settings',
    fields: [
      { key: 'max_daily_loss_percent', label: 'Max Daily Loss %', type: 'number', description: 'Stop trading at X% daily loss (negative value, e.g., -3)', min: -10, max: 0, step: 0.5 },
      { key: 'square_off_time', label: 'Square Off Time', type: 'text', description: 'Auto close all positions at this time (24h format, e.g., 15:25)' },
    ],
  },
  {
    id: 'position-init',
    title: 'Position Initialization',
    icon: '🚀',
    description: 'Settings for position initialization on strategy start',
    fields: [
      { key: 'enable_position_init', label: 'Enable Position Init', type: 'boolean', description: 'Enable position initialization (may create reverse orders to match target)' },
    ],
  },
  {
    id: 'logging',
    title: 'Logging',
    icon: '📝',
    description: 'Debug and data logging settings',
    fields: [
      { key: 'log_tick_data', label: 'Log Tick Data', type: 'boolean', description: 'Enable detailed tick-by-tick data logging (high disk usage)' },
    ],
  },
];

// =============================================================================
// COMPONENT
// =============================================================================
export default function ConfigPage() {
  const [config, setConfig] = useState<StrategyConfig | null>(null);
  const [originalConfig, setOriginalConfig] = useState<StrategyConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [validation, setValidation] = useState<ConfigValidationResponse | null>(null);
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(
    () => new Set(['symbol-market', 'price-gaps', 'stop-loss'])
  );
  const [searchTerm, setSearchTerm] = useState('');

  const fetchConfig = useCallback(async () => {
    try {
      setLoading(true);
      const data = await configAPI.get();
      setConfig(data);
      setOriginalConfig(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch configuration');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchConfig();
  }, [fetchConfig]);

  const handleChange = (key: keyof StrategyConfig, value: string | number | boolean) => {
    if (!config) return;

    setConfig(prev => ({
      ...prev!,
      [key]: value,
    }));

    // Clear messages
    setSuccess(null);
    setError(null);
    setValidation(null);
  };

  const handleSave = async () => {
    if (!config) return;

    try {
      setSaving(true);

      // Validate first
      const validationResult = await configAPI.validate(config);
      setValidation(validationResult);

      if (!validationResult.valid) {
        setError('Configuration has validation errors');
        return;
      }

      // Save
      await configAPI.update(config);
      setOriginalConfig(config);
      setSuccess('Configuration saved successfully!');

      // Clear success message after 3 seconds
      setTimeout(() => setSuccess(null), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save configuration');
    } finally {
      setSaving(false);
    }
  };

  const handleReset = () => {
    setConfig(originalConfig);
    setSuccess(null);
    setError(null);
    setValidation(null);
  };

  const toggleCategory = (categoryId: string) => {
    setExpandedCategories(prev => {
      const newSet = new Set(prev);
      if (newSet.has(categoryId)) {
        newSet.delete(categoryId);
      } else {
        newSet.add(categoryId);
      }
      return newSet;
    });
  };

  const expandAll = () => {
    setExpandedCategories(new Set(CONFIG_CATEGORIES.map(c => c.id)));
  };

  const collapseAll = () => {
    setExpandedCategories(new Set());
  };

  const hasChanges = JSON.stringify(config) !== JSON.stringify(originalConfig);

  // Filter categories based on search
  const filteredCategories = searchTerm
    ? CONFIG_CATEGORIES.map(cat => ({
        ...cat,
        fields: cat.fields.filter(
          f =>
            f.label.toLowerCase().includes(searchTerm.toLowerCase()) ||
            f.description.toLowerCase().includes(searchTerm.toLowerCase()) ||
            (f.key as string).toLowerCase().includes(searchTerm.toLowerCase())
        ),
      })).filter(cat => cat.fields.length > 0)
    : CONFIG_CATEGORIES;

  const renderField = (field: ConfigField) => {
    if (!config) return null;

    const value = config[field.key];
    const hasError = validation?.errors.some(e => e.toLowerCase().includes((field.key as string).toLowerCase()));

    if (field.type === 'boolean') {
      return (
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={() => handleChange(field.key, !value)}
            className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2 ${
              value ? 'bg-primary-600' : 'bg-gray-200'
            }`}
          >
            <span
              className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                value ? 'translate-x-6' : 'translate-x-1'
              }`}
            />
          </button>
          <span className="text-sm text-gray-700">{value ? 'Enabled' : 'Disabled'}</span>
        </div>
      );
    }

    if (field.type === 'select') {
      return (
        <select
          id={field.key as string}
          className={`form-input ${hasError ? 'border-red-500 focus:border-red-500 focus:ring-red-500' : ''}`}
          value={String(value ?? '')}
          onChange={(e) => handleChange(field.key, e.target.value)}
        >
          {field.options?.map((option) => (
            <option key={option} value={option}>
              {option}
            </option>
          ))}
        </select>
      );
    }

    return (
      <input
        id={field.key as string}
        type={field.type}
        className={`form-input ${hasError ? 'border-red-500 focus:border-red-500 focus:ring-red-500' : ''}`}
        value={typeof value === 'boolean' ? String(value) : (value ?? '')}
        onChange={(e) => handleChange(
          field.key,
          field.type === 'number' ? parseFloat(e.target.value) || 0 : e.target.value
        )}
        min={field.min}
        max={field.max}
        step={field.step}
      />
    );
  };

  const renderCategory = (category: ConfigCategory) => {
    const isExpanded = expandedCategories.has(category.id);
    const hasErrors = validation?.errors.some(e =>
      category.fields.some(f => e.toLowerCase().includes((f.key as string).toLowerCase()))
    );

    return (
      <div
        key={category.id}
        className={`bg-white rounded-xl shadow-sm border overflow-hidden transition-all duration-200 ${
          hasErrors ? 'border-red-300 ring-1 ring-red-300' : 'border-gray-200'
        }`}
      >
        <button
          type="button"
          onClick={() => toggleCategory(category.id)}
          className="w-full px-6 py-4 flex items-center justify-between bg-gray-50 hover:bg-gray-100 transition-colors"
        >
          <div className="flex items-center gap-3">
            <span className="text-2xl">{category.icon}</span>
            <div className="text-left">
              <h3 className="text-lg font-semibold text-gray-900">{category.title}</h3>
              <p className="text-sm text-gray-500">{category.description}</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            {hasErrors && (
              <span className="px-2 py-1 text-xs font-medium text-red-700 bg-red-100 rounded-full">
                Error
              </span>
            )}
            <svg
              className={`w-5 h-5 text-gray-500 transform transition-transform ${isExpanded ? 'rotate-180' : ''}`}
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </div>
        </button>

        {isExpanded && (
          <div className="p-6 space-y-5">
            {category.fields.map((field) => (
              <div key={field.key as string} className="form-group">
                <div className="flex items-center justify-between mb-2">
                  <label
                    className="form-label text-sm font-medium text-gray-700"
                    htmlFor={field.key as string}
                  >
                    {field.label}
                  </label>
                  <code className="text-xs text-gray-400 bg-gray-100 px-2 py-0.5 rounded">
                    {field.key as string}
                  </code>
                </div>
                {renderField(field)}
                <p className="mt-1.5 text-sm text-gray-500">{field.description}</p>
                {field.min !== undefined && field.max !== undefined && (
                  <p className="mt-1 text-xs text-gray-400">
                    Range: {field.min} to {field.max}
                    {field.step && field.step < 1 ? ` (step: ${field.step})` : ''}
                  </p>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    );
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading configuration...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200 sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Strategy Configuration</h1>
              <p className="text-sm text-gray-500 mt-1">
                Configure parameters for the Enhanced Survivor trading strategy
              </p>
            </div>
            <Link href="/" className="btn-secondary">
              ← Back to Dashboard
            </Link>
          </div>
        </div>
      </header>

      <main className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Search and Controls */}
        <div className="mb-6 space-y-4">
          <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
            <div className="relative flex-1 max-w-md">
              <input
                type="text"
                placeholder="Search settings..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-primary-500 focus:border-primary-500"
              />
              <svg
                className="absolute left-3 top-2.5 h-5 w-5 text-gray-400"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
            </div>
            <div className="flex gap-2">
              <button onClick={expandAll} className="btn-secondary text-sm">
                Expand All
              </button>
              <button onClick={collapseAll} className="btn-secondary text-sm">
                Collapse All
              </button>
            </div>
          </div>
        </div>

        {/* Alerts */}
        {error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-lg p-4">
            <div className="flex items-start gap-3">
              <svg className="h-5 w-5 text-red-500 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              <p className="text-red-800">{error}</p>
            </div>
          </div>
        )}

        {success && (
          <div className="mb-6 bg-green-50 border border-green-200 rounded-lg p-4">
            <div className="flex items-start gap-3">
              <svg className="h-5 w-5 text-green-500 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              <p className="text-green-800">{success}</p>
            </div>
          </div>
        )}

        {validation && validation.errors.length > 0 && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-lg p-4">
            <h4 className="font-medium text-red-800 mb-2 flex items-center gap-2">
              <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
              Validation Errors:
            </h4>
            <ul className="list-disc list-inside text-red-700 text-sm space-y-1">
              {validation.errors.map((err, i) => (
                <li key={i}>{err}</li>
              ))}
            </ul>
          </div>
        )}

        {validation && validation.warnings.length > 0 && (
          <div className="mb-6 bg-yellow-50 border border-yellow-200 rounded-lg p-4">
            <h4 className="font-medium text-yellow-800 mb-2 flex items-center gap-2">
              <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              Warnings:
            </h4>
            <ul className="list-disc list-inside text-yellow-700 text-sm space-y-1">
              {validation.warnings.map((warning, i) => (
                <li key={i}>{warning}</li>
              ))}
            </ul>
          </div>
        )}

        {/* Configuration Categories */}
        <div className="space-y-4">
          {filteredCategories.map(renderCategory)}
        </div>

        {/* Empty State for Search */}
        {filteredCategories.length === 0 && (
          <div className="text-center py-12">
            <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
            <h3 className="mt-2 text-sm font-medium text-gray-900">No settings found</h3>
            <p className="mt-1 text-sm text-gray-500">Try adjusting your search terms.</p>
          </div>
        )}

        {/* Action Buttons */}
        <div className="mt-8 flex flex-col sm:flex-row justify-between items-center gap-4">
          <div className="text-sm text-gray-500">
            {hasChanges ? (
              <span className="flex items-center gap-2 text-amber-600">
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                Unsaved changes
              </span>
            ) : (
              <span className="flex items-center gap-2 text-green-600">
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
                All changes saved
              </span>
            )}
          </div>
          <div className="flex gap-4">
            <button
              onClick={handleReset}
              disabled={!hasChanges || saving}
              className="btn-secondary"
            >
              Reset Changes
            </button>
            <button
              onClick={handleSave}
              disabled={!hasChanges || saving}
              className="btn-primary"
            >
              {saving ? (
                <span className="flex items-center gap-2">
                  <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                  Saving...
                </span>
              ) : (
                'Save Configuration'
              )}
            </button>
          </div>
        </div>
      </main>
    </div>
  );
}

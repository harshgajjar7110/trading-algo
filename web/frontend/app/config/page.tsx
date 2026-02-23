'use client';

import { useEffect, useState, useCallback } from 'react';
import Link from 'next/link';
import { StrategyConfig, ConfigValidationResponse } from '@/types';
import { configAPI } from '@/lib/api';

type ConfigTab = 'base' | 'enhanced';

interface ConfigField {
  key: keyof StrategyConfig;
  label: string;
  type: 'text' | 'number' | 'select';
  options?: string[];
  description: string;
}

interface ConfigGroup {
  title: string;
  fields: ConfigField[];
}

// =============================================================================
// BASE CONFIGURATION GROUPS
// =============================================================================
const BASE_CONFIG_GROUPS: ConfigGroup[] = [
  {
    title: '📊 Index & Symbol Configuration',
    fields: [
      { key: 'index_symbol', label: 'Index Symbol', type: 'text', description: 'Underlying index for tracking (e.g., NSE:NIFTY 50)' },
      { key: 'symbol_initials', label: 'Symbol Initials', type: 'text', description: 'Option series identifier (e.g., NIFTY26FEB)' },
    ],
  },
  {
    title: '📈 Gap Parameters (Trade Triggers)',
    fields: [
      { key: 'pe_gap', label: 'PE Gap', type: 'number', description: 'NIFTY upward movement threshold to trigger PE sells' },
      { key: 'ce_gap', label: 'CE Gap', type: 'number', description: 'NIFTY downward movement threshold to trigger CE sells' },
      { key: 'pe_reset_gap', label: 'PE Reset Gap', type: 'number', description: 'Favorable movement threshold to reset PE reference' },
      { key: 'ce_reset_gap', label: 'CE Reset Gap', type: 'number', description: 'Favorable movement threshold to reset CE reference' },
    ],
  },
  {
    title: '🎯 Strike Selection (Distance from Spot)',
    fields: [
      { key: 'pe_symbol_gap', label: 'PE Symbol Gap', type: 'number', description: 'Distance below current price for PE strike selection' },
      { key: 'ce_symbol_gap', label: 'CE Symbol Gap', type: 'number', description: 'Distance above current price for CE strike selection' },
    ],
  },
  {
    title: '💼 Position Sizing',
    fields: [
      { key: 'pe_quantity', label: 'PE Quantity', type: 'number', description: 'Base quantity for PE option trades' },
      { key: 'ce_quantity', label: 'CE Quantity', type: 'number', description: 'Base quantity for CE option trades' },
    ],
  },
  {
    title: '🛡️ Risk Management',
    fields: [
      { key: 'min_price_to_sell', label: 'Min Price to Sell', type: 'number', description: 'Minimum option premium threshold for execution' },
      { key: 'sell_multiplier_threshold', label: 'Sell Multiplier Threshold', type: 'number', description: 'Maximum allowed position multiplier' },
    ],
  },
  {
    title: '📍 Reference Points (Starting Values)',
    fields: [
      { key: 'pe_start_point', label: 'PE Start Point', type: 'number', description: 'Initial PE reference value (0 = current market price)' },
      { key: 'ce_start_point', label: 'CE Start Point', type: 'number', description: 'Initial CE reference value (0 = current market price)' },
    ],
  },
  {
    title: '⚙️ Order Settings',
    fields: [
      { key: 'exchange', label: 'Exchange', type: 'select', options: ['NFO'], description: 'Exchange for trading' },
      { key: 'order_type', label: 'Order Type', type: 'select', options: ['MARKET', 'LIMIT'], description: 'Order type for execution' },
      { key: 'product_type', label: 'Product Type', type: 'select', options: ['NRML', 'MIS'], description: 'Product type for orders' },
      { key: 'trans_type', label: 'Transaction Type', type: 'select', options: ['BUY', 'SELL'], description: 'Transaction type for all orders' },
    ],
  },
];

// =============================================================================
// ENHANCED CONFIGURATION GROUPS
// =============================================================================
const ENHANCED_CONFIG_GROUPS: ConfigGroup[] = [
  {
    title: '🔧 Entry Filters & Technical Indicators',
    fields: [
      { key: 'entry_filter_type', label: 'Entry Filter Type', type: 'select', options: ['NONE', 'RSI', 'EMA', 'ADX', 'ALL'], description: 'Type of entry filter to use (NONE = base strategy behavior)' },
      { key: 'rsi_period', label: 'RSI Period', type: 'number', description: 'RSI calculation period' },
      { key: 'rsi_min', label: 'RSI Min', type: 'number', description: 'Minimum RSI for entry (don\'t sell CE if RSI < min)' },
      { key: 'rsi_max', label: 'RSI Max', type: 'number', description: 'Maximum RSI for entry (don\'t sell PE if RSI > max)' },
      { key: 'adx_period', label: 'ADX Period', type: 'number', description: 'ADX calculation period for trend strength' },
      { key: 'adx_threshold', label: 'ADX Threshold', type: 'number', description: 'Minimum ADX to confirm trend strength' },
      { key: 'ema_period', label: 'EMA Period', type: 'number', description: 'EMA period for trend detection filter' },
    ],
  },
  {
    title: '📊 ATR Settings',
    fields: [
      { key: 'atr_period', label: 'ATR Period', type: 'number', description: 'ATR period for volatility measurement' },
      { key: 'atr_history_days', label: 'ATR History Days', type: 'number', description: 'Days of history to load for indicators' },
    ],
  },
  {
    title: '🚫 Position Limits',
    fields: [
      { key: 'max_positions_per_side', label: 'Max Positions Per Side', type: 'number', description: 'Maximum PE or CE positions separately' },
      { key: 'max_total_positions', label: 'Max Total Positions', type: 'number', description: 'Maximum combined positions (PE + CE)' },
      { key: 'max_consecutive_losses', label: 'Max Consecutive Losses', type: 'number', description: 'Pause trading after N consecutive losses' },
    ],
  },
  {
    title: '🛑 Stop-Loss Settings',
    fields: [
      { key: 'sl_enabled', label: 'SL Enabled', type: 'select', options: ['true', 'false'], description: 'Enable automatic SL order placement' },
      { key: 'sl_percentage', label: 'SL Percentage', type: 'number', description: 'SL at % of entry price (for shorts: entry × (1 + sl%/100))' },
      { key: 'sl_order_type', label: 'SL Order Type', type: 'select', options: ['STOP', 'STOP_LIMIT'], description: 'STOP (SL-M) or STOP_LIMIT (SL)' },
      { key: 'sl_limit_buffer', label: 'SL Limit Buffer', type: 'number', description: 'Points buffer for limit price (for SL orders)' },
      { key: 'sl_reconcile_on_start', label: 'SL Reconcile on Start', type: 'select', options: ['true', 'false'], description: 'Run SL reconciliation at algo startup' },
      { key: 'sl_state_file', label: 'SL State File', type: 'text', description: 'Path to position state file' },
    ],
  },
  {
    title: '💰 Profit Target Settings',
    fields: [
      { key: 'profit_target_enabled', label: 'Profit Target Enabled', type: 'select', options: ['true', 'false'], description: 'Enable profit target exit' },
      { key: 'profit_target_percent', label: 'Profit Target Percent', type: 'number', description: 'Exit when X% of premium collected as profit' },
    ],
  },
  {
    title: '📉 Legacy & Trailing Stop-Loss',
    fields: [
      { key: 'stop_loss_multiplier', label: 'Stop Loss Multiplier', type: 'number', description: 'Exit if premium doubles (used when sl_enabled is false)' },
      { key: 'trailing_stop_enabled', label: 'Trailing Stop Enabled', type: 'select', options: ['true', 'false'], description: 'Enable trailing stop loss' },
      { key: 'trailing_stop_distance', label: 'Trailing Stop Distance', type: 'number', description: 'Exit if profit falls X% from peak' },
    ],
  },
  {
    title: '⏰ Daily Limits & Square Off',
    fields: [
      { key: 'max_daily_loss_percent', label: 'Max Daily Loss %', type: 'number', description: 'Stop trading at X% daily loss (negative value)' },
      { key: 'square_off_time', label: 'Square Off Time', type: 'text', description: 'Close all positions at this time (HH:MM format)' },
    ],
  },
  {
    title: '🔄 Dynamic Gap Adjustment',
    fields: [
      { key: 'enable_dynamic_gaps', label: 'Enable Dynamic Gaps', type: 'select', options: ['true', 'false'], description: 'Enable ATR-based dynamic gap adjustment' },
      { key: 'atr_multiplier_pe', label: 'ATR Multiplier PE', type: 'number', description: 'ATR multiplier for PE gap calculation' },
      { key: 'atr_multiplier_ce', label: 'ATR Multiplier CE', type: 'number', description: 'ATR multiplier for CE gap calculation' },
    ],
  },
  {
    title: '📊 Volatility-based Position Sizing',
    fields: [
      { key: 'volatility_sizing', label: 'Volatility Sizing', type: 'select', options: ['true', 'false'], description: 'Enable volatility-based position sizing' },
      { key: 'high_vol_size_reduction', label: 'High Vol Size Reduction', type: 'number', description: 'Trade at X% size in high volatility (0.5 = 50%)' },
    ],
  },
];

export default function ConfigPage() {
  const [config, setConfig] = useState<StrategyConfig | null>(null);
  const [originalConfig, setOriginalConfig] = useState<StrategyConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [validation, setValidation] = useState<ConfigValidationResponse | null>(null);
  const [activeTab, setActiveTab] = useState<ConfigTab>('base');

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

  const hasChanges = JSON.stringify(config) !== JSON.stringify(originalConfig);

  const renderField = (field: ConfigField) => {
    if (!config) return null;

    const value = config[field.key];

    if (field.type === 'select') {
      return (
        <select
          id={field.key as string}
          className="form-input"
          value={String(value)}
          onChange={(e) => {
            const newValue = e.target.value;
            if (field.key === 'sl_enabled' || field.key === 'sl_reconcile_on_start' ||
                field.key === 'profit_target_enabled' || field.key === 'trailing_stop_enabled' ||
                field.key === 'enable_dynamic_gaps' || field.key === 'volatility_sizing') {
              handleChange(field.key, newValue === 'true');
            } else {
              handleChange(field.key, newValue);
            }
          }}
        >
          {field.options?.map((option) => (
            <option key={option} value={option}>
              {option === 'true' ? 'Yes' : option === 'false' ? 'No' : option}
            </option>
          ))}
        </select>
      );
    }

    return (
      <input
        id={field.key as string}
        type={field.type}
        className="form-input"
        value={typeof value === 'boolean' ? String(value) : (value ?? '')}
        onChange={(e) => handleChange(
          field.key,
          field.type === 'number' ? parseFloat(e.target.value) || 0 : e.target.value
        )}
      />
    );
  };

  const renderConfigGroups = (groups: ConfigGroup[]) => (
    <div className="space-y-6">
      {groups.map((group) => (
        <div key={group.title} className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
            <h3 className="text-lg font-medium text-gray-900">{group.title}</h3>
          </div>
          <div className="p-6 space-y-4">
            {group.fields.map((field) => (
              <div key={field.key as string} className="form-group">
                <label className="form-label" htmlFor={field.key as string}>
                  {field.label}
                </label>
                {renderField(field)}
                <p className="mt-1 text-sm text-gray-500">{field.description}</p>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );

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
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Strategy Configuration</h1>
              <p className="text-sm text-gray-500 mt-1">
                Configure parameters for the Survivor trading strategy
              </p>
            </div>
            <Link href="/" className="btn-secondary">
              ← Back to Dashboard
            </Link>
          </div>
        </div>
      </header>

      <main className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Tab Navigation */}
        <div className="mb-8">
          <div className="border-b border-gray-200">
            <nav className="-mb-px flex space-x-8" aria-label="Tabs">
              <button
                onClick={() => setActiveTab('base')}
                className={`
                  whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm
                  ${activeTab === 'base'
                    ? 'border-primary-500 text-primary-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  }
                `}
              >
                <span className="flex items-center gap-2">
                  <span>⚙️</span>
                  Base Configuration
                  <span className="ml-2 px-2 py-0.5 rounded-full text-xs bg-blue-100 text-blue-800">
                    Core
                  </span>
                </span>
              </button>
              <button
                onClick={() => setActiveTab('enhanced')}
                className={`
                  whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm
                  ${activeTab === 'enhanced'
                    ? 'border-primary-500 text-primary-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  }
                `}
              >
                <span className="flex items-center gap-2">
                  <span>✨</span>
                  Enhanced Configuration
                  <span className="ml-2 px-2 py-0.5 rounded-full text-xs bg-purple-100 text-purple-800">
                    Advanced
                  </span>
                </span>
              </button>
            </nav>
          </div>
          <p className="mt-3 text-sm text-gray-500">
            {activeTab === 'base'
              ? 'Base configuration includes essential parameters for the Survivor strategy. These settings are required for all strategy versions.'
              : 'Enhanced configuration provides advanced features like position limits, stop-loss management, and dynamic gap adjustments.'}
          </p>
        </div>

        {/* Alerts */}
        {error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-lg p-4">
            <p className="text-red-800">{error}</p>
          </div>
        )}

        {success && (
          <div className="mb-6 bg-green-50 border border-green-200 rounded-lg p-4">
            <p className="text-green-800">{success}</p>
          </div>
        )}

        {validation && validation.warnings.length > 0 && (
          <div className="mb-6 bg-yellow-50 border border-yellow-200 rounded-lg p-4">
            <h4 className="font-medium text-yellow-800 mb-2">Warnings:</h4>
            <ul className="list-disc list-inside text-yellow-700 text-sm">
              {validation.warnings.map((warning, i) => (
                <li key={i}>{warning}</li>
              ))}
            </ul>
          </div>
        )}

        {/* Configuration Form */}
        {activeTab === 'base'
          ? renderConfigGroups(BASE_CONFIG_GROUPS)
          : renderConfigGroups(ENHANCED_CONFIG_GROUPS)
        }

        {/* Action Buttons */}
        <div className="mt-8 flex justify-between items-center">
          <div className="flex items-center gap-4">
            <button
              onClick={() => setActiveTab(activeTab === 'base' ? 'enhanced' : 'base')}
              className="btn-secondary"
            >
              {activeTab === 'base' ? 'Go to Enhanced →' : '← Go to Base'}
            </button>
          </div>
          <div className="flex space-x-4">
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
              {saving ? 'Saving...' : 'Save Configuration'}
            </button>
          </div>
        </div>
      </main>
    </div>
  );
}

'use client';

import { useEffect, useState, useCallback } from 'react';
import Link from 'next/link';
import { StrategyConfig, ConfigValidationResponse } from '@/types';
import { configAPI } from '@/lib/api';

const CONFIG_GROUPS = [
  {
    title: 'Index & Symbol Configuration',
    fields: [
      { key: 'index_symbol', label: 'Index Symbol', type: 'text', description: 'Underlying index for tracking (e.g., NSE:NIFTY 50)' },
      { key: 'symbol_initials', label: 'Symbol Initials', type: 'text', description: 'Option series identifier (e.g., NIFTY26FEB)' },
    ],
  },
  {
    title: 'Gap Parameters (Trade Triggers)',
    fields: [
      { key: 'pe_gap', label: 'PE Gap', type: 'number', description: 'NIFTY upward movement threshold to trigger PE sells' },
      { key: 'ce_gap', label: 'CE Gap', type: 'number', description: 'NIFTY downward movement threshold to trigger CE sells' },
      { key: 'pe_reset_gap', label: 'PE Reset Gap', type: 'number', description: 'Favorable movement threshold to reset PE reference' },
      { key: 'ce_reset_gap', label: 'CE Reset Gap', type: 'number', description: 'Favorable movement threshold to reset CE reference' },
    ],
  },
  {
    title: 'Strike Selection (Distance from Spot)',
    fields: [
      { key: 'pe_symbol_gap', label: 'PE Symbol Gap', type: 'number', description: 'Distance below current price for PE strike selection' },
      { key: 'ce_symbol_gap', label: 'CE Symbol Gap', type: 'number', description: 'Distance above current price for CE strike selection' },
    ],
  },
  {
    title: 'Position Sizing',
    fields: [
      { key: 'pe_quantity', label: 'PE Quantity', type: 'number', description: 'Base quantity for PE option trades' },
      { key: 'ce_quantity', label: 'CE Quantity', type: 'number', description: 'Base quantity for CE option trades' },
    ],
  },
  {
    title: 'Risk Management',
    fields: [
      { key: 'min_price_to_sell', label: 'Min Price to Sell', type: 'number', description: 'Minimum option premium threshold for execution' },
      { key: 'sell_multiplier_threshold', label: 'Sell Multiplier Threshold', type: 'number', description: 'Maximum allowed position multiplier' },
    ],
  },
  {
    title: 'Reference Points (Starting Values)',
    fields: [
      { key: 'pe_start_point', label: 'PE Start Point', type: 'number', description: 'Initial PE reference value (0 = current market price)' },
      { key: 'ce_start_point', label: 'CE Start Point', type: 'number', description: 'Initial CE reference value (0 = current market price)' },
    ],
  },
  {
    title: 'Order Settings',
    fields: [
      { key: 'exchange', label: 'Exchange', type: 'select', options: ['NFO'], description: 'Exchange for trading' },
      { key: 'order_type', label: 'Order Type', type: 'select', options: ['MARKET', 'LIMIT'], description: 'Order type for execution' },
      { key: 'product_type', label: 'Product Type', type: 'select', options: ['NRML', 'MIS'], description: 'Product type for orders' },
      { key: 'trans_type', label: 'Transaction Type', type: 'select', options: ['BUY', 'SELL'], description: 'Transaction type for all orders' },
    ],
  },
  {
    title: 'Entry Filters',
    fields: [
      { key: 'entry_filter_type', label: 'Entry Filter Type', type: 'select', options: ['NONE', 'RSI_ADX', 'EMA', 'BOTH'], description: 'Type of entry filter to use' },
      { key: 'rsi_period', label: 'RSI Period', type: 'number', description: 'RSI period for calculation' },
      { key: 'rsi_min', label: 'RSI Min', type: 'number', description: 'Minimum RSI for Bullish entry' },
      { key: 'rsi_max', label: 'RSI Max', type: 'number', description: 'Maximum RSI for Bearish entry' },
      { key: 'adx_period', label: 'ADX Period', type: 'number', description: 'ADX period for calculation' },
      { key: 'adx_threshold', label: 'ADX Threshold', type: 'number', description: 'Minimum ADX to confirm trend' },
      { key: 'ema_period', label: 'EMA Period', type: 'number', description: 'EMA period for trend filter' },
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

  const handleChange = (key: string, value: string | number) => {
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
        <div className="space-y-6">
          {CONFIG_GROUPS.map((group) => (
            <div key={group.title} className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
              <div className="px-6 py-4 border-b border-gray-200 bg-gray-50">
                <h3 className="text-lg font-medium text-gray-900">{group.title}</h3>
              </div>
              <div className="p-6 space-y-4">
                {group.fields.map((field) => (
                  <div key={field.key} className="form-group">
                    <label className="form-label" htmlFor={field.key}>
                      {field.label}
                    </label>
                    {field.type === 'select' ? (
                      <select
                        id={field.key}
                        className="form-input"
                        value={config?.[field.key as keyof StrategyConfig] || ''}
                        onChange={(e) => handleChange(field.key, e.target.value)}
                      >
                        {field.options?.map((option) => (
                          <option key={option} value={option}>
                            {option}
                          </option>
                        ))}
                      </select>
                    ) : (
                      <input
                        id={field.key}
                        type={field.type}
                        className="form-input"
                        value={config?.[field.key as keyof StrategyConfig] ?? ''}
                        onChange={(e) => handleChange(
                          field.key,
                          field.type === 'number' ? parseFloat(e.target.value) || 0 : e.target.value
                        )}
                      />
                    )}
                    <p className="mt-1 text-sm text-gray-500">{field.description}</p>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>

        {/* Action Buttons */}
        <div className="mt-8 flex justify-end space-x-4">
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
      </main>
    </div>
  );
}

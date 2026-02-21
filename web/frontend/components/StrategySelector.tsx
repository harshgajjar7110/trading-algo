'use client';

import { useState, useEffect, useCallback } from 'react';
import { StrategyInfo, StrategyDetails, StrategyPreview, CurrentStrategy } from '@/types';
import { strategySelectorAPI } from '@/lib/api';
import { formatCurrency } from '@/lib/utils';

interface StrategySelectorProps {
  onStrategyChange?: () => void;
}

export default function StrategySelector({ onStrategyChange }: StrategySelectorProps) {
  const [strategies, setStrategies] = useState<StrategyInfo[]>([]);
  const [currentStrategy, setCurrentStrategy] = useState<CurrentStrategy | null>(null);
  const [selectedStrategy, setSelectedStrategy] = useState<string>('');
  const [strategyDetails, setStrategyDetails] = useState<StrategyDetails | null>(null);
  const [preview, setPreview] = useState<StrategyPreview | null>(null);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [showConfirmation, setShowConfirmation] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  // Fetch available strategies and current strategy
  const fetchStrategies = useCallback(async () => {
    try {
      setError(null);
      console.log('Fetching available strategies...');
      const data = await strategySelectorAPI.getAvailableStrategies();
      console.log('Received strategies:', data);
      setStrategies(data.strategies || []);
      setCurrentStrategy(data.current);
    } catch (err) {
      console.error('Failed to fetch strategies:', err);
      setError(`Failed to load strategies: ${err instanceof Error ? err.message : 'Unknown error'}`);
      setStrategies([]);
    } finally {
      setInitialLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchStrategies();
    // Poll every 5 seconds to check current strategy
    const interval = setInterval(fetchStrategies, 5000);
    return () => clearInterval(interval);
  }, [fetchStrategies]);

  // Load strategy details when selected
  const handleStrategySelect = async (strategyId: string) => {
    setSelectedStrategy(strategyId);
    setStrategyDetails(null);
    setPreview(null);
    setShowConfirmation(false);
    setError(null);

    if (!strategyId) return;

    try {
      setLoading(true);
      const details = await strategySelectorAPI.getStrategyDetails(strategyId);
      setStrategyDetails(details);
      
      // Get preview with default config
      const previewData = await strategySelectorAPI.previewConfig(strategyId);
      setPreview(previewData);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load strategy details');
    } finally {
      setLoading(false);
    }
  };

  // Start strategy (with confirmation)
  const handleStartStrategy = async (confirmed: boolean = false) => {
    if (!selectedStrategy) return;

    try {
      setLoading(true);
      setError(null);

      const result = await strategySelectorAPI.start(selectedStrategy, undefined, confirmed);

      if (result.requires_confirmation) {
        // Show confirmation dialog
        setPreview(result.preview || null);
        setShowConfirmation(true);
        setLoading(false);
        return;
      }

      if (result.success) {
        setSuccess(`Strategy "${result.strategy_id}" started successfully!`);
        setShowConfirmation(false);
        await fetchStrategies();
        onStrategyChange?.();
        setTimeout(() => setSuccess(null), 5000);
      } else {
        setError(result.message || 'Failed to start strategy');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to start strategy');
    } finally {
      setLoading(false);
    }
  };

  // Stop strategy
  const handleStopStrategy = async () => {
    try {
      setLoading(true);
      setError(null);

      const result = await strategySelectorAPI.stop();

      if (result.success) {
        setSuccess('Strategy stopped successfully');
        await fetchStrategies();
        onStrategyChange?.();
        setTimeout(() => setSuccess(null), 5000);
      } else {
        setError(result.message || 'Failed to stop strategy');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to stop strategy');
    } finally {
      setLoading(false);
    }
  };

  const getRiskColor = (riskLevel: string) => {
    switch (riskLevel.toLowerCase()) {
      case 'low': return 'text-green-600 bg-green-50';
      case 'medium': return 'text-yellow-600 bg-yellow-50';
      case 'medium-high': return 'text-orange-600 bg-orange-50';
      case 'high': return 'text-red-600 bg-red-50';
      default: return 'text-gray-600 bg-gray-50';
    }
  };

  return (
    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
      <div className="px-6 py-4 border-b border-gray-200">
        <h3 className="text-lg font-medium text-gray-900">Strategy Selection</h3>
        <p className="text-sm text-gray-500 mt-1">
          Choose which trading strategy to run
        </p>
      </div>

      <div className="p-6 space-y-6">
        {/* Current Strategy Status */}
        {currentStrategy?.running && (
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
            <div className="flex items-center justify-between">
              <div>
                <h4 className="text-sm font-medium text-blue-900">Currently Running</h4>
                <p className="text-lg font-semibold text-blue-800 mt-1">
                  {currentStrategy.name}
                </p>
                <p className="text-xs text-blue-600 mt-1">
                  Started: {currentStrategy.started_at ? new Date(currentStrategy.started_at).toLocaleString() : 'Unknown'}
                </p>
              </div>
              <button
                onClick={handleStopStrategy}
                disabled={loading}
                className="btn-danger"
              >
                {loading ? 'Stopping...' : 'Stop Strategy'}
              </button>
            </div>
          </div>
        )}

        {/* Debug Info - Remove in production */}
        {process.env.NODE_ENV === 'development' && (
          <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 text-xs text-gray-600">
            <details>
              <summary>Debug Info</summary>
              <pre className="mt-2 overflow-auto">
                {JSON.stringify({
                  strategiesCount: strategies.length,
                  initialLoading,
                  selectedStrategy,
                  currentStrategyRunning: currentStrategy?.running,
                }, null, 2)}
              </pre>
            </details>
          </div>
        )}

        {/* Messages */}
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-800">
            {error}
          </div>
        )}
        {success && (
          <div className="bg-green-50 border border-green-200 rounded-lg p-4 text-green-800">
            {success}
          </div>
        )}

        {/* Strategy Selector */}
        {!currentStrategy?.running && (
          <>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Select Strategy
              </label>
              <select
                value={selectedStrategy}
                onChange={(e) => handleStrategySelect(e.target.value)}
                disabled={loading || initialLoading}
                className="w-full rounded-lg border border-gray-300 px-4 py-2 focus:outline-none focus:ring-2 focus:ring-primary-500 disabled:bg-gray-100"
              >
                <option value="">
                  {initialLoading ? 'Loading strategies...' : 'Choose a strategy...'}
                </option>
                {strategies.map((strategy) => (
                  <option key={strategy.id} value={strategy.id}>
                    {strategy.name} ({strategy.risk_level} risk)
                  </option>
                ))}
              </select>
              {initialLoading && (
                <p className="text-xs text-gray-500 mt-1">
                  Loading available strategies...
                </p>
              )}
              {!initialLoading && strategies.length === 0 && !error && (
                <p className="text-xs text-gray-500 mt-1">
                  No strategies available. Check backend connection.
                </p>
              )}
            </div>

            {/* Strategy Details */}
            {strategyDetails && preview && !showConfirmation && (
              <div className="border border-gray-200 rounded-lg p-4 space-y-4">
                {/* Header */}
                <div className="flex items-start justify-between">
                  <div>
                    <h4 className="font-medium text-gray-900">{strategyDetails.name}</h4>
                    <p className="text-sm text-gray-500 mt-1">{strategyDetails.description}</p>
                  </div>
                  <span className={`px-2 py-1 rounded text-xs font-medium ${getRiskColor(strategyDetails.risk_level)}`}>
                    {strategyDetails.risk_level} risk
                  </span>
                </div>

                {/* Key Info */}
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <span className="text-gray-500">Recommended Capital:</span>
                    <span className="ml-2 font-medium">{strategyDetails.recommended_capital}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">Version:</span>
                    <span className="ml-2 font-medium">{strategyDetails.version}</span>
                  </div>
                </div>

                {/* Tags */}
                <div className="flex flex-wrap gap-2">
                  {strategyDetails.tags.map((tag) => (
                    <span key={tag} className="px-2 py-1 bg-gray-100 text-gray-600 rounded text-xs">
                      {tag}
                    </span>
                  ))}
                </div>

                {/* Validation Errors */}
                {preview.validation_errors.length > 0 && (
                  <div className="bg-red-50 border border-red-200 rounded p-3">
                    <h5 className="text-sm font-medium text-red-800 mb-2">Configuration Errors:</h5>
                    <ul className="list-disc list-inside text-sm text-red-700">
                      {preview.validation_errors.map((err, idx) => (
                        <li key={idx}>{err}</li>
                      ))}
                    </ul>
                  </div>
                )}

                {/* Start Button */}
                <button
                  onClick={() => handleStartStrategy(false)}
                  disabled={loading || !preview.is_valid}
                  className="w-full btn-success disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {loading ? 'Loading...' : 'Review & Start Strategy'}
                </button>
              </div>
            )}

            {/* Confirmation Dialog */}
            {showConfirmation && preview && (
              <div className="border-2 border-primary-500 rounded-lg p-4 space-y-4 bg-primary-50">
                <div className="flex items-center space-x-2">
                  <svg className="w-5 h-5 text-primary-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                  </svg>
                  <h4 className="font-medium text-primary-900">Confirm Strategy Configuration</h4>
                </div>

                <p className="text-sm text-primary-700">
                  Please review the configuration below before starting the strategy:
                </p>

                {/* Config Summary */}
                <div className="bg-white rounded border border-primary-200 p-3 space-y-2">
                  <h5 className="text-sm font-medium text-gray-700">Key Parameters:</h5>
                  <div className="grid grid-cols-2 gap-2 text-sm">
                    <div><span className="text-gray-500">Symbol:</span> {preview.config.symbol_initials}</div>
                    <div><span className="text-gray-500">PE Gap:</span> {preview.config.pe_gap}</div>
                    <div><span className="text-gray-500">CE Gap:</span> {preview.config.ce_gap}</div>
                    <div><span className="text-gray-500">PE Qty:</span> {preview.config.pe_quantity}</div>
                    <div><span className="text-gray-500">CE Qty:</span> {preview.config.ce_quantity}</div>
                    {preview.config.entry_filter_type && preview.config.entry_filter_type !== 'NONE' && (
                      <div><span className="text-gray-500">Filter:</span> {preview.config.entry_filter_type}</div>
                    )}
                  </div>
                </div>

                {/* Differences from Default */}
                {preview.differences_from_default.length > 0 && (
                  <div className="bg-yellow-50 rounded border border-yellow-200 p-3">
                    <h5 className="text-sm font-medium text-yellow-800 mb-2">Custom Parameters:</h5>
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="text-yellow-700">
                          <th className="text-left">Parameter</th>
                          <th className="text-left">Default</th>
                          <th className="text-left">Current</th>
                        </tr>
                      </thead>
                      <tbody>
                        {preview.differences_from_default.map((diff, idx) => (
                          <tr key={idx} className="text-yellow-900">
                            <td>{diff.param}</td>
                            <td className="text-yellow-600">{diff.default}</td>
                            <td className="font-medium">{diff.current}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {/* Risk Warning */}
                <div className="flex items-center space-x-2 text-sm text-primary-700">
                  <span className="font-medium">Risk Level:</span>
                  <span className={`px-2 py-0.5 rounded text-xs ${getRiskColor(preview.risk_level)}`}>
                    {preview.risk_level}
                  </span>
                  <span>Recommended Capital: {preview.recommended_capital}</span>
                </div>

                {/* Action Buttons */}
                <div className="flex space-x-3 pt-2">
                  <button
                    onClick={() => handleStartStrategy(true)}
                    disabled={loading}
                    className="flex-1 btn-success"
                  >
                    {loading ? 'Starting...' : 'Confirm & Start'}
                  </button>
                  <button
                    onClick={() => setShowConfirmation(false)}
                    disabled={loading}
                    className="flex-1 btn-secondary"
                  >
                    Go Back
                  </button>
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

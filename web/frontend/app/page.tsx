'use client';

import { useEffect, useState, useCallback } from 'react';
import Link from 'next/link';
import { 
  StrategyState, 
  StrategyStatus, 
  NiftyData, 
  PositionsResponse, 
  Funds,
  Position,
  WSMessage
} from '@/types';
import { strategyAPI, marketAPI, tradingAPI } from '@/lib/api';
import { useWebSocket } from '@/hooks/useWebSocket';
import PayoffChart from '@/components/PayoffChart';
import { 
  formatCurrency, 
  formatNumber, 
  formatPercent, 
  formatUptime,
  getStatusColor,
  getPnLColor
} from '@/lib/utils';

type SortField = 'symbol' | 'quantity' | 'pnl' | 'pnl_percent' | 'average_price';
type SortDirection = 'asc' | 'desc';

export default function Dashboard() {
  // State
  const [strategyState, setStrategyState] = useState<StrategyState | null>(null);
  const [niftyData, setNiftyData] = useState<NiftyData | null>(null);
  const [positions, setPositions] = useState<PositionsResponse | null>(null);
  const [funds, setFunds] = useState<Funds | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actionLoading, setActionLoading] = useState(false);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  
  // Sorting state
  const [sortField, setSortField] = useState<SortField>('pnl');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');

  // Fetch initial data
  const fetchData = useCallback(async () => {
    try {
      const [state, nifty, pos, fundsData] = await Promise.all([
        strategyAPI.getStatus(),
        marketAPI.getNiftyData(),
        tradingAPI.getPositions(),
        marketAPI.getFunds(),
      ]);
      setStrategyState(state);
      setNiftyData(nifty);
      setPositions(pos);
      setFunds(fundsData);
      setLastRefresh(new Date());
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch data');
    } finally {
      setLoading(false);
    }
  }, []);

  // WebSocket handlers
  const handleStrategyState = useCallback((data: StrategyState) => {
    setStrategyState(data);
  }, []);

  const handlePriceUpdate = useCallback((data: { symbol: string; last_price: number; change: number | null }) => {
    if (niftyData && data.symbol === 'NSE:NIFTY 50') {
      setNiftyData(prev => prev ? {
        ...prev,
        quote: {
          ...prev.quote,
          last_price: data.last_price,
          change: data.change,
        }
      } : null);
    }
  }, [niftyData]);

  // WebSocket connection
  const { isConnected } = useWebSocket({
    onStrategyState: handleStrategyState,
    onPriceUpdate: handlePriceUpdate,
  });

  // Strategy actions
  const handleStart = async () => {
    try {
      setActionLoading(true);
      await strategyAPI.start();
      await fetchData();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to start strategy');
    } finally {
      setActionLoading(false);
    }
  };

  const handleStop = async () => {
    try {
      setActionLoading(true);
      await strategyAPI.stop();
      await fetchData();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to stop strategy');
    } finally {
      setActionLoading(false);
    }
  };

  const handleRestart = async () => {
    try {
      setActionLoading(true);
      await strategyAPI.restart();
      await fetchData();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to restart strategy');
    } finally {
      setActionLoading(false);
    }
  };

  // Sorting functions
  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('desc');
    }
  };

  const getSortedPositions = (): Position[] => {
    if (!positions?.positions) return [];
    
    const sorted = [...positions.positions].sort((a, b) => {
      let aVal: number | string = 0;
      let bVal: number | string = 0;
      
      switch (sortField) {
        case 'symbol':
          aVal = a.symbol;
          bVal = b.symbol;
          break;
        case 'quantity':
          aVal = Math.abs(a.quantity);
          bVal = Math.abs(b.quantity);
          break;
        case 'pnl':
          aVal = a.pnl || 0;
          bVal = b.pnl || 0;
          break;
        case 'pnl_percent':
          aVal = a.pnl_percent || 0;
          bVal = b.pnl_percent || 0;
          break;
        case 'average_price':
          aVal = a.average_price;
          bVal = b.average_price;
          break;
      }
      
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDirection === 'asc' 
          ? aVal.localeCompare(bVal) 
          : bVal.localeCompare(aVal);
      }
      
      return sortDirection === 'asc' 
        ? (aVal as number) - (bVal as number) 
        : (bVal as number) - (aVal as number);
    });
    
    return sorted;
  };

  const SortIndicator = ({ field }: { field: SortField }) => (
    <span className="ml-1">
      {sortField === field ? (
        sortDirection === 'asc' ? '↑' : '↓'
      ) : (
        <span className="text-gray-300">↕</span>
      )}
    </span>
  );

  // Initial fetch - refresh every 10 seconds
  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 10000); // Refresh every 10s
    return () => clearInterval(interval);
  }, [fetchData]);

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading dashboard...</p>
        </div>
      </div>
    );
  }

  const sortedPositions = getSortedPositions();

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Survivor Trading Dashboard</h1>
              <p className="text-sm text-gray-500 mt-1">
                Real-time monitoring for options trading strategy
                {lastRefresh && (
                  <span className="ml-2 text-xs">
                    • Last updated: {lastRefresh.toLocaleTimeString()}
                  </span>
                )}
              </p>
            </div>
            <div className="flex items-center space-x-4">
              {/* Connection Status */}
              <div className="flex items-center space-x-2">
                <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500' : 'bg-red-500'}`}></div>
                <span className="text-sm text-gray-600">
                  {isConnected ? 'Connected' : 'Disconnected'}
                </span>
              </div>
              
              {/* Manual Refresh */}
              <button 
                onClick={fetchData}
                className="text-sm text-primary-600 hover:text-primary-700"
              >
                ↻ Refresh
              </button>
              
              {/* Navigation */}
              <Link href="/config" className="btn-secondary">
                Configuration
              </Link>
              <Link href="/history" className="btn-secondary">
                Trade History
              </Link>
            </div>
          </div>
        </div>
      </header>

      {/* Error Alert */}
      {error && (
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 mt-4">
          <div className="bg-red-50 border border-red-200 rounded-lg p-4">
            <p className="text-red-800">{error}</p>
            <button 
              onClick={() => setError(null)}
              className="text-red-600 text-sm mt-2 hover:underline"
            >
              Dismiss
            </button>
          </div>
        </div>
      )}

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Top Row - Key Metrics */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          {/* Strategy Status */}
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 card-hover">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-medium text-gray-500">Strategy Status</h3>
              <span className={`badge ${getStatusColor(strategyState?.status || 'STOPPED')}`}>
                {strategyState?.status || 'UNKNOWN'}
              </span>
            </div>
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span className="text-gray-500">Uptime</span>
                <span className="font-medium">{formatUptime(strategyState?.uptime_seconds)}</span>
              </div>
              {strategyState?.error_message && (
                <p className="text-red-600 text-sm">{strategyState.error_message}</p>
              )}
            </div>
            {/* Strategy Controls */}
            <div className="mt-4 flex space-x-2">
              {strategyState?.status === StrategyStatus.RUNNING ? (
                <>
                  <button
                    onClick={handleStop}
                    disabled={actionLoading}
                    className="btn-danger text-xs flex-1"
                  >
                    Stop
                  </button>
                  <button
                    onClick={handleRestart}
                    disabled={actionLoading}
                    className="btn-secondary text-xs flex-1"
                  >
                    Restart
                  </button>
                </>
              ) : (
                <button
                  onClick={handleStart}
                  disabled={actionLoading}
                  className="btn-success text-xs w-full"
                >
                  Start Strategy
                </button>
              )}
            </div>
          </div>

          {/* NIFTY Price */}
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 card-hover">
            <h3 className="text-sm font-medium text-gray-500 mb-4">NIFTY 50</h3>
            <div className="flex items-baseline space-x-2">
              <span className="text-3xl font-bold text-gray-900">
                {formatNumber(niftyData?.quote.last_price)}
              </span>
              {niftyData?.quote.change !== null && niftyData?.quote.change !== undefined && (
                <span className={`text-sm font-medium ${getPnLColor(niftyData.quote.change)}`}>
                  {niftyData.quote.change >= 0 ? '+' : ''}{formatNumber(niftyData.quote.change)}
                  ({formatPercent(niftyData.quote.change_percent)})
                </span>
              )}
            </div>
            {/* Reference Values */}
            <div className="mt-4 grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-gray-500">PE Ref</span>
                <p className="font-medium">{formatNumber(strategyState?.nifty_pe_last_value)}</p>
              </div>
              <div>
                <span className="text-gray-500">CE Ref</span>
                <p className="font-medium">{formatNumber(strategyState?.nifty_ce_last_value)}</p>
              </div>
            </div>
          </div>

          {/* Total PnL */}
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 card-hover">
            <h3 className="text-sm font-medium text-gray-500 mb-4">Total P&L</h3>
            <div className="flex items-baseline space-x-2">
              <span className={`text-3xl font-bold ${getPnLColor(positions?.total_pnl)}`}>
                {formatCurrency(positions?.total_pnl)}
              </span>
            </div>
            <div className="mt-4 text-sm">
              <div className="flex justify-between">
                <span className="text-gray-500">Positions</span>
                <span className="font-medium">{positions?.positions.length || 0}</span>
              </div>
            </div>
          </div>

          {/* Available Margin */}
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 card-hover">
            <h3 className="text-sm font-medium text-gray-500 mb-4">Account Funds</h3>
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span className="text-gray-500">Available</span>
                <span className="font-medium text-green-600">
                  {formatCurrency(funds?.available_cash)}
                </span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-gray-500">Used Margin</span>
                <span className="font-medium">
                  {formatCurrency(funds?.used_margin)}
                </span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-gray-500">Net Worth</span>
                <span className="font-medium">
                  {formatCurrency(funds?.net)}
                </span>
              </div>
            </div>
          </div>
        </div>

        {/* Positions Table */}
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
            <h3 className="text-lg font-medium text-gray-900">Active Positions</h3>
            <span className="text-sm text-gray-500">
              Click headers to sort
            </span>
          </div>
          {positions && positions.positions.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="data-table">
                <thead>
                  <tr>
                    <th 
                      className="cursor-pointer hover:bg-gray-100"
                      onClick={() => handleSort('symbol')}
                    >
                      Symbol <SortIndicator field="symbol" />
                    </th>
                    <th>Side</th>
                    <th 
                      className="cursor-pointer hover:bg-gray-100"
                      onClick={() => handleSort('quantity')}
                    >
                      Quantity <SortIndicator field="quantity" />
                    </th>
                    <th 
                      className="cursor-pointer hover:bg-gray-100"
                      onClick={() => handleSort('average_price')}
                    >
                      Avg Price <SortIndicator field="average_price" />
                    </th>
                    <th 
                      className="cursor-pointer hover:bg-gray-100"
                      onClick={() => handleSort('pnl')}
                    >
                      P&L <SortIndicator field="pnl" />
                    </th>
                    <th 
                      className="cursor-pointer hover:bg-gray-100"
                      onClick={() => handleSort('pnl_percent')}
                    >
                      P&L % <SortIndicator field="pnl_percent" />
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {sortedPositions.map((position, index) => (
                    <tr key={index}>
                      <td className="font-medium">{position.symbol}</td>
                      <td>
                        <span className={`badge ${position.side === 'SHORT' ? 'badge-danger' : 'badge-success'}`}>
                          {position.side}
                        </span>
                      </td>
                      <td>{Math.abs(position.quantity)}</td>
                      <td>{formatCurrency(position.average_price)}</td>
                      <td className={getPnLColor(position.pnl)}>
                        {formatCurrency(position.pnl)}
                      </td>
                      <td className={getPnLColor(position.pnl_percent)}>
                        {formatPercent(position.pnl_percent)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="px-6 py-12 text-center text-gray-500">
              No active positions
            </div>
          )}
        </div>

        {/* Payoff Analysis Section */}
        {positions && positions.positions.length > 0 && (
          <div className="mt-6">
            <PayoffChart />
          </div>
        )}
      </main>
    </div>
  );
}

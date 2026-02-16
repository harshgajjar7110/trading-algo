'use client';

import { useEffect, useState, useCallback } from 'react';
import Link from 'next/link';
import { OrdersResponse, TradesResponse, Order, Trade } from '@/types';
import { tradingAPI } from '@/lib/api';
import { formatDateTime, formatCurrency, getPnLColor } from '@/lib/utils';

type TabType = 'orders' | 'trades';

export default function HistoryPage() {
  const [activeTab, setActiveTab] = useState<TabType>('orders');
  const [orders, setOrders] = useState<OrdersResponse | null>(null);
  const [trades, setTrades] = useState<TradesResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    try {
      setLoading(true);
      const [ordersData, tradesData] = await Promise.all([
        tradingAPI.getOrders(),
        tradingAPI.getTrades(),
      ]);
      setOrders(ordersData);
      setTrades(tradesData);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch data');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 30000); // Refresh every 30s
    return () => clearInterval(interval);
  }, [fetchData]);

  const getStatusBadge = (status: string) => {
    const statusClasses: Record<string, string> = {
      COMPLETE: 'badge-success',
      CANCELLED: 'badge-danger',
      REJECTED: 'badge-danger',
      PENDING: 'badge-warning',
      OPEN: 'badge-warning',
    };
    return statusClasses[status] || 'badge-gray';
  };

  if (loading && !orders && !trades) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading trade history...</p>
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
              <h1 className="text-2xl font-bold text-gray-900">Trade History</h1>
              <p className="text-sm text-gray-500 mt-1">
                View historical orders and trades
              </p>
            </div>
            <Link href="/" className="btn-secondary">
              ← Back to Dashboard
            </Link>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-lg p-4">
            <p className="text-red-800">{error}</p>
          </div>
        )}

        {/* Tabs */}
        <div className="border-b border-gray-200 mb-6">
          <nav className="-mb-px flex space-x-8">
            <button
              onClick={() => setActiveTab('orders')}
              className={`py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'orders'
                  ? 'border-primary-500 text-primary-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              Orders ({orders?.count || 0})
            </button>
            <button
              onClick={() => setActiveTab('trades')}
              className={`py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'trades'
                  ? 'border-primary-500 text-primary-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              Trades ({trades?.count || 0})
            </button>
          </nav>
        </div>

        {/* Orders Table */}
        {activeTab === 'orders' && (
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
            {orders && orders.orders.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Order ID</th>
                      <th>Symbol</th>
                      <th>Type</th>
                      <th>Side</th>
                      <th>Qty</th>
                      <th>Price</th>
                      <th>Status</th>
                      <th>Time</th>
                    </tr>
                  </thead>
                  <tbody>
                    {orders.orders.map((order: Order) => (
                      <tr key={order.order_id}>
                        <td className="font-mono text-xs">{order.order_id}</td>
                        <td className="font-medium">{order.symbol}</td>
                        <td>{order.order_type}</td>
                        <td>
                          <span className={`badge ${order.transaction_type === 'SELL' ? 'badge-danger' : 'badge-success'}`}>
                            {order.transaction_type}
                          </span>
                        </td>
                        <td>{order.quantity}</td>
                        <td>{formatCurrency(order.price)}</td>
                        <td>
                          <span className={`badge ${getStatusBadge(order.status)}`}>
                            {order.status}
                          </span>
                        </td>
                        <td className="text-sm text-gray-500">{formatDateTime(order.timestamp)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="px-6 py-12 text-center text-gray-500">
                No orders found
              </div>
            )}
          </div>
        )}

        {/* Trades Table */}
        {activeTab === 'trades' && (
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
            {trades && trades.trades.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Trade ID</th>
                      <th>Order ID</th>
                      <th>Symbol</th>
                      <th>Side</th>
                      <th>Qty</th>
                      <th>Price</th>
                      <th>Value</th>
                      <th>Time</th>
                    </tr>
                  </thead>
                  <tbody>
                    {trades.trades.map((trade: Trade) => (
                      <tr key={trade.trade_id}>
                        <td className="font-mono text-xs">{trade.trade_id}</td>
                        <td className="font-mono text-xs">{trade.order_id}</td>
                        <td className="font-medium">{trade.symbol}</td>
                        <td>
                          <span className={`badge ${trade.transaction_type === 'SELL' ? 'badge-danger' : 'badge-success'}`}>
                            {trade.transaction_type}
                          </span>
                        </td>
                        <td>{trade.quantity}</td>
                        <td>{formatCurrency(trade.price)}</td>
                        <td>{formatCurrency(trade.price * trade.quantity)}</td>
                        <td className="text-sm text-gray-500">{formatDateTime(trade.timestamp)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="px-6 py-12 text-center text-gray-500">
                No trades found
              </div>
            )}
          </div>
        )}
      </main>
    </div>
  );
}

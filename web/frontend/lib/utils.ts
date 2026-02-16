/**
 * Utility functions for the Survivor Trading UI
 */

import { clsx, type ClassValue } from 'clsx';

/**
 * Merge class names with clsx
 */
export function cn(...inputs: ClassValue[]) {
  return clsx(inputs);
}

/**
 * Format a number as currency (INR)
 */
export function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-';
  return new Intl.NumberFormat('en-IN', {
    style: 'currency',
    currency: 'INR',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

/**
 * Format a number with commas
 */
export function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-';
  return new Intl.NumberFormat('en-IN').format(value);
}

/**
 * Format a percentage
 */
export function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-';
  return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
}

/**
 * Format a timestamp
 */
export function formatTime(timestamp: string | null | undefined): string {
  if (!timestamp) return '-';
  const date = new Date(timestamp);
  return date.toLocaleTimeString('en-IN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

/**
 * Format a date
 */
export function formatDate(timestamp: string | null | undefined): string {
  if (!timestamp) return '-';
  const date = new Date(timestamp);
  return date.toLocaleDateString('en-IN', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  });
}

/**
 * Format a datetime
 */
export function formatDateTime(timestamp: string | null | undefined): string {
  if (!timestamp) return '-';
  const date = new Date(timestamp);
  return date.toLocaleString('en-IN', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

/**
 * Format uptime in seconds to human readable
 */
export function formatUptime(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined) return '-';
  
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  
  if (hours > 0) {
    return `${hours}h ${minutes}m ${secs}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${secs}s`;
  }
  return `${secs}s`;
}

/**
 * Get status color class
 */
export function getStatusColor(status: string): string {
  switch (status) {
    case 'RUNNING':
      return 'text-green-600 bg-green-100';
    case 'STOPPED':
      return 'text-gray-600 bg-gray-100';
    case 'STARTING':
    case 'STOPPING':
      return 'text-yellow-600 bg-yellow-100';
    case 'ERROR':
      return 'text-red-600 bg-red-100';
    default:
      return 'text-gray-600 bg-gray-100';
  }
}

/**
 * Get PnL color class
 */
export function getPnLColor(value: number | null | undefined): string {
  if (value === null || value === undefined) return 'text-gray-600';
  return value >= 0 ? 'text-green-600' : 'text-red-600';
}

/**
 * Debounce function
 */
export function debounce<T extends (...args: unknown[]) => unknown>(
  func: T,
  wait: number
): (...args: Parameters<T>) => void {
  let timeout: NodeJS.Timeout | null = null;
  
  return (...args: Parameters<T>) => {
    if (timeout) clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
}

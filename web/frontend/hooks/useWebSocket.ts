'use client';

/**
 * WebSocket hook for real-time data streaming
 */

import { useEffect, useRef, useState, useCallback } from 'react';
import { WSMessage, StrategyState, PriceUpdate, OrderUpdate } from '@/types';

const WS_URL = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8000/ws';

interface UseWebSocketOptions {
  onMessage?: (message: WSMessage) => void;
  onPriceUpdate?: (data: PriceUpdate) => void;
  onStrategyState?: (data: StrategyState) => void;
  onOrderUpdate?: (data: OrderUpdate) => void;
  onConnect?: () => void;
  onDisconnect?: () => void;
  reconnectInterval?: number;
  maxReconnectAttempts?: number;
}

interface UseWebSocketReturn {
  isConnected: boolean;
  subscribe: (symbols: string[]) => void;
  unsubscribe: (symbols: string[]) => void;
  sendMessage: (message: Record<string, unknown>) => void;
  reconnect: () => void;
}

export function useWebSocket(options: UseWebSocketOptions = {}): UseWebSocketReturn {
  const {
    reconnectInterval = 5000,
    maxReconnectAttempts = 10,
  } = options;

  // Use refs for callbacks to avoid reconnection loops
  const onMessageRef = useRef(options.onMessage);
  const onPriceUpdateRef = useRef(options.onPriceUpdate);
  const onStrategyStateRef = useRef(options.onStrategyState);
  const onOrderUpdateRef = useRef(options.onOrderUpdate);
  const onConnectRef = useRef(options.onConnect);
  const onDisconnectRef = useRef(options.onDisconnect);

  // Update refs when callbacks change
  useEffect(() => {
    onMessageRef.current = options.onMessage;
    onPriceUpdateRef.current = options.onPriceUpdate;
    onStrategyStateRef.current = options.onStrategyState;
    onOrderUpdateRef.current = options.onOrderUpdate;
    onConnectRef.current = options.onConnect;
    onDisconnectRef.current = options.onDisconnect;
  });

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const isUnmountedRef = useRef(false);
  
  const [isConnected, setIsConnected] = useState(false);

  const connect = useCallback(() => {
    // Don't connect if unmounted or already connected
    if (isUnmountedRef.current) {
      return;
    }
    
    if (wsRef.current?.readyState === WebSocket.OPEN || wsRef.current?.readyState === WebSocket.CONNECTING) {
      return;
    }

    try {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        if (isUnmountedRef.current) {
          ws.close();
          return;
        }
        console.log('WebSocket connected');
        setIsConnected(true);
        reconnectAttemptsRef.current = 0;
        onConnectRef.current?.();
      };

      ws.onclose = (event) => {
        if (isUnmountedRef.current) {
          return;
        }
        console.log('WebSocket disconnected:', event.code, event.reason);
        setIsConnected(false);
        onDisconnectRef.current?.();

        // Attempt reconnect if not closed intentionally
        if (event.code !== 1000 && reconnectAttemptsRef.current < maxReconnectAttempts) {
          reconnectTimeoutRef.current = setTimeout(() => {
            if (!isUnmountedRef.current) {
              reconnectAttemptsRef.current++;
              console.log(`Reconnecting... Attempt ${reconnectAttemptsRef.current}`);
              connect();
            }
          }, reconnectInterval);
        }
      };

      ws.onerror = (error) => {
        console.error('WebSocket error:', error);
      };

      ws.onmessage = (event) => {
        try {
          const message: WSMessage = JSON.parse(event.data);
          onMessageRef.current?.(message);

          // Handle specific message types
          switch (message.type) {
            case 'PRICE_UPDATE':
              onPriceUpdateRef.current?.(message.data as unknown as PriceUpdate);
              break;
            case 'STRATEGY_STATE':
              onStrategyStateRef.current?.(message.data as unknown as StrategyState);
              break;
            case 'ORDER_UPDATE':
              onOrderUpdateRef.current?.(message.data as unknown as OrderUpdate);
              break;
          }
        } catch (error) {
          console.error('Error parsing WebSocket message:', error);
        }
      };
    } catch (error) {
      console.error('Error creating WebSocket:', error);
    }
  }, [reconnectInterval, maxReconnectAttempts]);

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }
    if (wsRef.current) {
      // Close with normal closure code
      wsRef.current.close(1000, 'Client disconnect');
      wsRef.current = null;
    }
  }, []);

  const subscribe = useCallback((symbols: string[]) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'SUBSCRIBE', symbols }));
    }
  }, []);

  const unsubscribe = useCallback((symbols: string[]) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'UNSUBSCRIBE', symbols }));
    }
  }, []);

  const sendMessage = useCallback((message: Record<string, unknown>) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(message));
    }
  }, []);

  const reconnect = useCallback(() => {
    disconnect();
    reconnectAttemptsRef.current = 0;
    connect();
  }, [connect, disconnect]);

  useEffect(() => {
    isUnmountedRef.current = false;
    connect();
    
    return () => {
      isUnmountedRef.current = true;
      disconnect();
    };
  }, [connect, disconnect]);

  return {
    isConnected,
    subscribe,
    unsubscribe,
    sendMessage,
    reconnect,
  };
}

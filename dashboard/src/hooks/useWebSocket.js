import { useCallback, useEffect, useRef, useState } from "react";

export function useWebSocket(url) {
  const [data, setData] = useState(null);
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState(null);
  const [lastMessageAt, setLastMessageAt] = useState(null);
  const wsRef = useRef(null);
  const retryRef = useRef(null);
  const mountedRef = useRef(false);

  const connect = useCallback(() => {
    if (!mountedRef.current) {
      return;
    }

    try {
      wsRef.current = new WebSocket(url);

      wsRef.current.onopen = () => {
        if (!mountedRef.current) {
          return;
        }
        setIsConnected(true);
        setError(null);
      };

      wsRef.current.onmessage = (event) => {
        if (!mountedRef.current) {
          return;
        }

        try {
          setData(JSON.parse(event.data));
          setLastMessageAt(new Date());
        } catch {
          setError("Received a live payload that could not be parsed.");
        }
      };

      wsRef.current.onerror = () => {
        if (!mountedRef.current) {
          return;
        }
        setError("WebSocket connection failed. Verify that FastAPI is running on port 8000.");
      };

      wsRef.current.onclose = () => {
        if (!mountedRef.current) {
          return;
        }
        setIsConnected(false);
        retryRef.current = window.setTimeout(connect, 3000);
      };
    } catch (caughtError) {
      setError(caughtError.message);
    }
  }, [url]);

  useEffect(() => {
    mountedRef.current = true;
    connect();

    return () => {
      mountedRef.current = false;
      window.clearTimeout(retryRef.current);
      if (wsRef.current) {
        wsRef.current.close(1000, "component-unmounted");
      }
    };
  }, [connect]);

  return { data, isConnected, error, lastMessageAt };
}

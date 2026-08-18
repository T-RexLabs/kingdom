import { useCallback, useReducer, type ReactNode } from "react";
import {
  NotificationContext,
  notificationReducer,
  type Notification,
} from "./notification-context";

export function NotificationProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(notificationReducer, { notifications: new Map() });

  const addNotification = useCallback((notification: Notification) => {
    dispatch({ type: "add", notification });
  }, []);

  const updateNotification = useCallback((id: string, partial: Partial<Omit<Notification, "id">>) => {
    dispatch({ type: "update", id, partial });
  }, []);

  const removeNotification = useCallback((id: string) => {
    dispatch({ type: "remove", id });
  }, []);

  return (
    <NotificationContext.Provider value={{ notifications: state.notifications, addNotification, updateNotification, removeNotification }}>
      {children}
    </NotificationContext.Provider>
  );
}

import { createContext, useCallback, useContext, useReducer, type ReactNode } from "react";

export type NotificationStatus = "loading" | "success" | "error" | "info";

export type NotificationAction = {
  label: string;
  onClick: () => void;
};

export type Notification = {
  id: string;
  title: string;
  message?: string;
  instanceKey?: string;
  status: NotificationStatus;
  autoDismissMs?: number;
  action?: NotificationAction;
  onDismiss?: () => void;
};

type NotificationState = {
  notifications: Map<string, Notification>;
};

type NotificationDispatch =
  | { type: "add"; notification: Notification }
  | { type: "update"; id: string; partial: Partial<Omit<Notification, "id">> }
  | { type: "remove"; id: string };

function notificationReducer(state: NotificationState, action: NotificationDispatch): NotificationState {
  switch (action.type) {
    case "add": {
      const next = new Map(state.notifications);
      next.set(action.notification.id, action.notification);
      return { notifications: next };
    }
    case "update": {
      const existing = state.notifications.get(action.id);
      if (!existing) return state;
      const next = new Map(state.notifications);
      next.set(action.id, { ...existing, ...action.partial });
      return { notifications: next };
    }
    case "remove": {
      if (!state.notifications.has(action.id)) return state;
      const next = new Map(state.notifications);
      next.delete(action.id);
      return { notifications: next };
    }
  }
}

type NotificationContextValue = {
  notifications: Map<string, Notification>;
  addNotification: (notification: Notification) => void;
  updateNotification: (id: string, partial: Partial<Omit<Notification, "id">>) => void;
  removeNotification: (id: string) => void;
};

const NotificationContext = createContext<NotificationContextValue | null>(null);

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

export function useNotifications() {
  const ctx = useContext(NotificationContext);
  if (!ctx) throw new Error("useNotifications must be used within NotificationProvider");
  return ctx;
}

import { createContext, useContext } from "react";

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

export type NotificationState = {
  notifications: Map<string, Notification>;
};

export type NotificationDispatch =
  | { type: "add"; notification: Notification }
  | { type: "update"; id: string; partial: Partial<Omit<Notification, "id">> }
  | { type: "remove"; id: string };

export function notificationReducer(state: NotificationState, action: NotificationDispatch): NotificationState {
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

export type NotificationContextValue = {
  notifications: Map<string, Notification>;
  addNotification: (notification: Notification) => void;
  updateNotification: (id: string, partial: Partial<Omit<Notification, "id">>) => void;
  removeNotification: (id: string) => void;
};

export const NotificationContext = createContext<NotificationContextValue | null>(null);

export function useNotifications() {
  const context = useContext(NotificationContext);
  if (!context) throw new Error("useNotifications must be used within NotificationProvider");
  return context;
}

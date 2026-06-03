import { Component, type ErrorInfo, type ReactNode } from "react";
import { Shield, RefreshCw, Home } from "lucide-react";
import { Button } from "@/components/ui/button";

interface ErrorBoundaryProps {
  children: ReactNode;
  fallback?: ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

/**
 * Top-level error boundary that catches unhandled rendering errors
 * and shows a themed fallback UI instead of a white screen.
 */
export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return { hasError: true, error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("[Kingdom] Unhandled render error:", error, info.componentStack);
  }

  handleReset = () => {
    this.setState({ hasError: false, error: null });
  };

  handleHome = () => {
    this.setState({ hasError: false, error: null });
    window.location.href = "/";
  };

  render() {
    if (this.state.hasError) {
      if (this.props.fallback) return this.props.fallback;
      return <ErrorFallback error={this.state.error} onReset={this.handleReset} onHome={this.handleHome} />;
    }
    return this.props.children;
  }
}

function ErrorFallback({
  error,
  onReset,
  onHome,
}: {
  error: Error | null;
  onReset: () => void;
  onHome: () => void;
}) {
  return (
    <div className="flex h-screen w-screen items-center justify-center bg-background">
      <div className="mx-4 max-w-lg text-center">
        <div className="mx-auto mb-6 flex h-20 w-20 items-center justify-center rounded-full border-2 border-status-failed/30 bg-status-failed/10">
          <Shield className="h-10 w-10 text-status-failed" />
        </div>
        <h1 className="mb-2 font-display text-2xl font-bold text-foreground">
          The Kingdom Has Fallen
        </h1>
        <p className="mb-6 text-foreground-muted">
          An unexpected error disrupted the realm. The council has been notified.
        </p>
        {error && (
          <div className="mb-6 rounded-lg border border-border bg-surface p-4 text-left">
            <p className="mb-1 text-xs font-medium uppercase tracking-wider text-foreground-subtle">
              Error Details
            </p>
            <p className="font-mono text-sm text-status-failed">{error.message}</p>
          </div>
        )}
        <div className="flex items-center justify-center gap-3">
          <Button variant="outline" onClick={onReset}>
            <RefreshCw className="h-4 w-4" />
            Try Again
          </Button>
          <Button onClick={onHome}>
            <Home className="h-4 w-4" />
            Return to Throne Room
          </Button>
        </div>
      </div>
    </div>
  );
}

/**
 * Not Found page shown for unmatched routes.
 */
export function NotFoundPage() {
  return (
    <div className="flex h-full items-center justify-center">
      <div className="mx-4 max-w-lg text-center">
        <div className="mx-auto mb-6 flex h-20 w-20 items-center justify-center rounded-full border-2 border-gold/30 bg-gold/10">
          <Shield className="h-10 w-10 text-gold" />
        </div>
        <h1 className="mb-2 font-display text-2xl font-bold text-foreground">
          Uncharted Territory
        </h1>
        <p className="mb-2 text-4xl font-bold text-foreground-subtle">404</p>
        <p className="mb-6 text-foreground-muted">
          This path leads beyond the borders of the kingdom. No decree governs these lands.
        </p>
        <Button onClick={() => (window.location.href = "/")}>
          <Home className="h-4 w-4" />
          Return to Throne Room
        </Button>
      </div>
    </div>
  );
}

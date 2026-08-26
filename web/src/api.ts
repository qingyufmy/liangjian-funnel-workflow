import { ApiError } from "./types";

export const TOKEN_STORAGE_KEY = "liangjian-dashboard-token";

export function getStoredToken(): string {
  return sessionStorage.getItem(TOKEN_STORAGE_KEY) ?? "";
}

export function saveToken(token: string): void {
  if (token.trim()) {
    sessionStorage.setItem(TOKEN_STORAGE_KEY, token.trim());
  } else {
    sessionStorage.removeItem(TOKEN_STORAGE_KEY);
  }
}

export async function apiFetch<T>(path: string, signal?: AbortSignal): Promise<T> {
  const token = getStoredToken();
  const response = await fetch(path, {
    signal,
    headers: token ? { Authorization: `Bearer ${token}` } : undefined,
    cache: "no-store",
  });

  if (!response.ok) {
    let message = `请求失败（HTTP ${response.status}）`;
    try {
      const body = (await response.json()) as { error?: string; message?: string };
      message = body.message ?? body.error ?? message;
    } catch {
      // Keep the status-based message when the body is not JSON.
    }
    throw new ApiError(message, response.status);
  }

  return (await response.json()) as T;
}

export function withQuery(path: string, values: Record<string, string | number | undefined>): string {
  const query = new URLSearchParams();
  Object.entries(values).forEach(([key, value]) => {
    if (value !== undefined && value !== "") query.set(key, String(value));
  });
  const suffix = query.toString();
  return suffix ? `${path}?${suffix}` : path;
}

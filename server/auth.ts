import { timingSafeEqual } from "node:crypto";

import type { NextFunction, Request, RequestHandler, Response } from "express";

function suppliedToken(request: Request): string | null {
  const header = request.header("authorization");
  if (!header) return null;
  const match = /^Bearer\s+(.+)$/i.exec(header.trim());
  return match?.[1] ?? null;
}

export function tokenMatches(request: Request, expected: string): boolean {
  const supplied = suppliedToken(request);
  if (!supplied) return false;
  const expectedBytes = Buffer.from(expected, "utf8");
  const suppliedBytes = Buffer.from(supplied, "utf8");
  if (expectedBytes.length !== suppliedBytes.length) return false;
  return timingSafeEqual(expectedBytes, suppliedBytes);
}

export function dashboardAuth(token: string | null): RequestHandler {
  return (request: Request, response: Response, next: NextFunction): void => {
    if (!token || tokenMatches(request, token)) {
      next();
      return;
    }
    response.status(401).json({ error: "UNAUTHORIZED" });
  };
}

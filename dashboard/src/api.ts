import axios from "axios";

const baseURL = import.meta.env.VITE_API_URL ?? "";

export const http = axios.create({ baseURL });

export function wsURL(path: string): string {
  const root = import.meta.env.VITE_API_URL;
  if (root) {
    const u = new URL(root);
    u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
    u.pathname = u.pathname.replace(/\/$/, "") + path;
    u.search = "";
    u.hash = "";
    return u.toString();
  }
  const loc = window.location;
  const proto = loc.protocol === "https:" ? "wss:" : "ws:";
  return `${proto}//${loc.host}${path}`;
}

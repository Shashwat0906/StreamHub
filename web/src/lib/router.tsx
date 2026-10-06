// A tiny hash router: #/brokers/2 -> route "brokers", params ["2"].
// (No router package is available offline; this is all the app needs.)
import { useEffect, useState } from 'react';
import type { ReactNode } from 'react';

export interface Route {
  name: string;
  params: string[];
  query: URLSearchParams;
}

function parse(): Route {
  const raw = window.location.hash.replace(/^#\/?/, '');
  const [path, qs] = raw.split('?');
  const parts = path.split('/').filter(Boolean).map(decodeURIComponent);
  return { name: parts[0] || 'overview', params: parts.slice(1), query: new URLSearchParams(qs || '') };
}

export function useRoute(): Route {
  const [route, setRoute] = useState(parse);
  useEffect(() => {
    const on = () => setRoute(parse());
    window.addEventListener('hashchange', on);
    return () => window.removeEventListener('hashchange', on);
  }, []);
  return route;
}

export function navigate(to: string) {
  window.location.hash = to.startsWith('#') ? to : '#' + to;
}

export function Link({ to, className, children, title }: { to: string; className?: string; children?: ReactNode; title?: string }) {
  return (
    <a href={'#' + to} className={className} title={title}>
      {children}
    </a>
  );
}

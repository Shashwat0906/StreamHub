// Minimal React 19 type declarations, used ONLY when @types/react is not
// installed (see typecheck.mjs / tsconfig.offline.json). They cover the
// APIs this app uses; with a normal `npm install` the real @types/react
// are used instead.
export type Key = string | number | bigint;
export interface ReactElement<P = unknown> {
  type: unknown;
  props: P;
  key: string | null;
}
export type ReactNode = ReactElement | string | number | bigint | boolean | null | undefined | Iterable<ReactNode>;
export type FC<P = object> = (props: P) => ReactNode;
export type PropsWithChildren<P = object> = P & { children?: ReactNode };
export type CSSProperties = { [key: string]: string | number | undefined };

export type SetStateAction<S> = S | ((prev: S) => S);
export type Dispatch<A> = (value: A) => void;
export function useState<S>(initial: S | (() => S)): [S, Dispatch<SetStateAction<S>>];
export function useState<S = undefined>(): [S | undefined, Dispatch<SetStateAction<S | undefined>>];
export function useReducer<S, A>(reducer: (state: S, action: A) => S, initial: S): [S, Dispatch<A>];
export type EffectCallback = () => void | (() => void);
export function useEffect(effect: EffectCallback, deps?: readonly unknown[]): void;
export function useLayoutEffect(effect: EffectCallback, deps?: readonly unknown[]): void;
export function useMemo<T>(factory: () => T, deps: readonly unknown[]): T;
export function useCallback<T extends (...args: never[]) => unknown>(fn: T, deps: readonly unknown[]): T;
export interface RefObject<T> {
  current: T;
}
export function useRef<T>(initial: T): RefObject<T>;
export function useRef<T>(initial: T | null): RefObject<T | null>;
export function useRef<T = undefined>(): RefObject<T | undefined>;
export function useId(): string;
export interface Context<T> {
  Provider: FC<{ value: T; children?: ReactNode }>;
}
export function createContext<T>(defaultValue: T): Context<T>;
export function useContext<T>(context: Context<T>): T;
export function memo<P>(component: FC<P>): FC<P>;
export const Fragment: FC<{ children?: ReactNode }>;
export const StrictMode: FC<{ children?: ReactNode }>;
export function startTransition(fn: () => void): void;

export interface SyntheticEvent<T = Element> {
  currentTarget: T;
  target: EventTarget;
  preventDefault(): void;
  stopPropagation(): void;
}
export interface ChangeEvent<T = Element> extends SyntheticEvent<T> {
  target: EventTarget & T;
}
export type FormEvent<T = Element> = SyntheticEvent<T>;
export interface MouseEvent<T = Element> extends SyntheticEvent<T> {
  clientX: number;
  clientY: number;
}
export interface KeyboardEvent<T = Element> extends SyntheticEvent<T> {
  key: string;
  metaKey: boolean;
  ctrlKey: boolean;
}

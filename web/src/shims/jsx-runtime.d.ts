// JSX namespace for the offline React shim (see react.d.ts). Intrinsic
// elements are loosely typed (any attributes); component props are checked.
import type { Key, ReactElement, ReactNode } from 'react';

export namespace JSX {
  type Element = ReactElement;
  type ElementType = string | ((props: never) => ReactNode);
  interface ElementAttributesProperty {
    props: object;
  }
  interface ElementChildrenAttribute {
    children: object;
  }
  interface IntrinsicAttributes {
    key?: Key;
  }
  interface IntrinsicElements {
    [name: string]: { key?: Key; children?: ReactNode; [attr: string]: unknown };
  }
}
export function jsx(type: unknown, props: unknown, key?: Key): ReactElement;
export function jsxs(type: unknown, props: unknown, key?: Key): ReactElement;
export const Fragment: unknown;

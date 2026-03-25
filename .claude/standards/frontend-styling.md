# Frontend Styling Preferences

## Theme Direction
- Use a **dark navy** palette for app surfaces — NOT charcoal/gray. All backgrounds should have a blue undertone.
- Map to Tailwind's `slate-*` scale (NOT `neutral-*` or `gray-*`).
- Use soft white text (`slate-200` / `#e2e8f0`), not harsh pure white (`#fff` / `#f0f0f0`).
- Reserve color accents for data semantics (positive/negative values, metric colors), not for general layout chrome.

## CSS Custom Properties
```css
--background: #0a0f1a     /* deep navy, between slate-950 and slate-900 */
--foreground: #e2e8f0     /* slate-200, soft white */
--surface:    #111827     /* slate-900, sidebar/card backgrounds */
--surface-strong: #1e293b /* slate-800, hover/elevated surfaces */
```

## Color Mapping (Tailwind classes)
| Role | Class | Hex |
|---|---|---|
| Deepest background | `bg-[var(--background)]` | `#0a0f1a` |
| Card / sidebar | `bg-[var(--surface)]` or `bg-slate-900` | `#0f172a` |
| Elevated / hover | `bg-slate-800` | `#1e293b` |
| Borders | `border-slate-700/50` | `rgba(51,65,85,0.5)` |
| Primary text | `text-slate-200` | `#e2e8f0` |
| Secondary text | `text-slate-400` | `#94a3b8` |
| Muted text | `text-slate-500` | `#64748b` |
| Section headers | `text-slate-600` or `text-slate-500` | `#475569` / `#64748b` |

## Chart Styling
- Dark navy chart backgrounds (`bg-slate-900/80`).
- Grid lines: `rgba(51,65,85,0.25)` (slate-700 tinted).
- Axis strokes: `rgba(51,65,85,0.4)`.
- Tick / legend text: `#cbd5e1` (slate-300).
- Tooltips: `#0f172a` background, `rgba(51,65,85,0.5)` border.

## Active / Selected States
- Active sidebar item: `bg-blue-500/15` with `text-white`.
- Selected date button: `border-white/40 bg-white/10 text-white`.
- Inactive buttons: `border-slate-600/50 bg-slate-800`.

## UX Constraints
- Preserve existing calculations and data logic unless a UI request explicitly requires changes.
- Preserve responsive behavior on desktop and mobile.
- Always use `slate-*` classes. Never use `neutral-*`, `gray-*`, or `zinc-*`.

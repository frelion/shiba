# Shiba Ops Dashboard

## Product context

- Product type: database runtime observability console.
- Primary users: engineers operating Shiba dataflows and diagnosing lag.
- Primary tasks: decide whether Runtime is healthy, locate the slow stage, and
  understand how a source reaches each materialized result table.
- Devices: desktop-first operations screen, usable down to a tablet width.
- Density: information-dense, with progressive disclosure for stage details.

## Design direction

Shiba Ops is a quiet dark operations console: neutral surfaces, monospaced
numbers, precise cyan topology lines, and restrained semantic status colors. It
should feel closer to a flight recorder than a generic SaaS admin panel.

Avoid decorative gradients, oversized hero cards, and color-only status. The
graph and current health state are the visual focus; secondary catalog metadata
recedes until a node is selected.

## Principles

- System health first, topology second, diagnosis third.
- Use structure, labels, and status text alongside color.
- Keep recurring controls compact and align metrics to a shared grid.
- Animate only live refresh and graph continuity; honor reduced motion.

## Semantic tokens

- Surfaces: ink, panel, raised, inset.
- Content: primary, secondary, muted, inverse.
- State: healthy, active, warning, danger, info.
- Accent: cyan for topology and selected navigation.
- Numbers: monospace with tabular figures.

## Accessibility

- Every graph node is a keyboard-focusable button.
- Status chips include text, not color alone.
- Focus rings remain visible on the dark surface.
- The layout collapses to a single column below 1080px.
- Reduced motion disables live pulse and graph transitions.

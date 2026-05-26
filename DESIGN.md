# Design System — Harmful Algal Bloom Predictor

## Product Context
- **What this is:** A machine learning system that predicts harmful algal bloom risk along California's coast, outputting weekly station-level risk classifications (Low / Medium / High) from NDBC buoy data.
- **Who it's for:** Researchers and policymakers (primary); general public, aquaculture industry (secondary).
- **Space/industry:** Environmental monitoring, ocean science, public health.
- **Project type:** Landing page (marketing/education) + live geospatial risk dashboard.

## Aesthetic Direction
- **Direction:** Dark-to-light editorial — deep navy atmospheric hero on the landing page, transitioning to a clean light reading experience. Dashboard stays light with a dark navy top bar for visual continuity.
- **Decoration level:** Intentional — SVG tide chart data visualization as hero background texture; otherwise typography does the work. No decorative blobs, gradients, or icon grids.
- **Mood:** The urgency and authority of serious science journalism. Not a government data portal. Not a startup SaaS tool. Something that makes a policymaker feel the stakes before they see the data.
- **Memorable thing:** "The bloom arrives before the warning does." — everything serves this.

## Typography
- **Display/Hero:** [Fraunces](https://fonts.google.com/specimen/Fraunces) (variable, Google Fonts) — optical-size variable serif. Used for hero headlines, section anchors, and key risk numerals in the dashboard. Signals science journalism, not government forms.
- **Body:** [DM Sans](https://fonts.google.com/specimen/DM+Sans) (Google Fonts) — humanist grotesque, warm and clean. Approachable for the public, technically clean for researchers.
- **Data/Metrics/Code:** [DM Mono](https://fonts.google.com/specimen/DM+Mono) (Google Fonts) — strict monospace for risk percentages, station IDs, buoy readings, timestamps, model metrics, and all sensor data labels.
- **Loading:** Google Fonts CDN via `<link>` preconnect + stylesheet
  ```html
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,200..700&family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet" />
  ```
- **Scale:**
  | Role | Size | Font | Weight |
  |------|------|------|--------|
  | Hero headline | clamp(48px, 8vw, 96px) | Fraunces | 200–300 |
  | Section heading | clamp(28px, 4vw, 42px) | Fraunces | 600 |
  | Dashboard risk % | 24–32px | Fraunces | 700 |
  | Body paragraph | 15–16px | DM Sans | 400 |
  | UI label | 13–14px | DM Sans | 500 |
  | Data value | 11–13px | DM Mono | 400–500 |
  | Meta/eyebrow | 10–11px | DM Mono | 400, tracked +0.08–0.18em |

## Color
- **Approach:** Extended from the existing dashboard palette — identical risk colors, coherent handoff when the user navigates from landing page to dashboard.

| Name | Hex | Usage |
|------|-----|-------|
| Void | `#0b1920` | Landing page hero background |
| Navy | `#10202a` | Dashboard topbar; body text; dark surfaces |
| Grid | `#1e3340` | Borders on dark surfaces; SVG grid lines |
| Hero text | `#e8ece8` | Text on dark backgrounds |
| Muted dark | `#8fa3ad` | Secondary text on dark |
| Water | `#dceff4` | Links/accents on dark; map water color |
| Land | `#f4efe5` | Editorial warm-interrupt section background |
| Panel | `#ffffff` | Card and panel backgrounds |
| Page bg | `#f7fafb` | App background |
| Ink | `#10202a` | Body text on light |
| Muted | `#5d6b73` | Secondary text on light |
| Line | `#d9e2e7` | Borders on light |
| Teal | `#1b8fa8` | Interactive elements, links on light |
| Low risk | `#2f8f68` | Low bloom probability — green |
| Medium risk | `#c4881a` | Elevated probability — amber |
| High risk | `#c63d3d` | Harmful threshold exceeded — red |
| High glow | `rgba(198,61,61,0.18)` | Pulsing shadow on high-risk elements |

- **Dark mode:** The landing page hero IS the dark mode. Dashboard remains light — do not add a dark toggle to the dashboard; it would create visual noise around the risk colors.

## Spacing
- **Base unit:** 8px
- **Density:** Comfortable (research-grade products breathe)
- **Scale:** 4 / 8 / 12 / 16 / 20 / 24 / 32 / 40 / 48 / 56 / 64 / 80 / 96px
- **Dashboard sidebar padding:** 12–16px internal, 8px gaps between cards
- **Landing page section padding:** `clamp(64px, 8vw, 100px)` vertical

## Layout
- **Landing page:** Creative-editorial — first viewport is a full-bleed poster (dark, asymmetric, left-aligned). Below fold: alternating full-width and grid sections. No nav bar on hero load — fades in after 80px scroll.
- **Dashboard:** Fixed-height split layout — dark topbar (52px) + full-height row: Leaflet map (flex:1) + fixed-width sidebar (380px). Sidebar scrolls internally.
- **Grid:** 12-column for landing page content sections. Dashboard is flex, not grid.
- **Max content width (landing page):** 1100px centered
- **Border radius:** 4px (chips/badges) / 6px (buttons, form inputs) / 8px (cards, panels) / 12px (modal frames)

## Map (Dashboard)
- **Library:** Leaflet.js 1.9.x from CDN
- **Tile source:** OpenStreetMap (`https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png`)
- **Default view:** Center `[37.5, -122.0]`, zoom 6
- **Station markers:** Custom `L.divIcon` — 20×20px colored circle, 2.5px white border, drop shadow
- **Popup style:** Dark navy (`#10202a`) background, `#1e3340` border — matches topbar
- **Zoom control:** Bottom-right, custom dark styles
- **Active station:** Marker scales 1.4×, white border; sidebar card scrolls into view; map flyTo

## Motion
- **Approach:** Intentional — entrance animations that aid comprehension, no decorative motion
- **Landing page hero stat:** Count-up from 0 to current value on load
- **Landing page hero SVG:** Tide chart path draws in over 1.2s on load (stroke-dasharray animation)
- **Nav bar:** Fades in after 80px scroll (opacity + translateY transition, 200ms)
- **Station list cards:** Scroll-triggered fade-in (Intersection Observer), staggered 50ms
- **Map flyTo:** 0.8s duration when activating a station from the sidebar
- **Easing:** enter `ease-out` / exit `ease-in` / move `ease-in-out`
- **Duration:** micro 80ms / short 150–200ms / medium 250–400ms / long 700–1200ms

## Page Structure — Landing Page

```
[HERO — full viewport, #0b1920]
  SVG tide chart background (historical bloom event data as oscillating lines)
  Eyebrow: "California Coastal Monitoring / Harmful Algal Bloom Prediction" [DM Mono]
  Headline: "The bloom arrives before the warning does." [Fraunces 200]
  Sub: "Weekly bloom risk prediction... powered by NDBC..." [DM Sans 300]
  Live stat: "● 3 HIGH-RISK STATIONS THIS WEEK" [DM Mono, pulsing red dot]
  CTA: "→ View live risk dashboard" [DM Mono text link]
  Ghost buoy: NDBC ID / SST / Last ping [DM Mono, 40% opacity, bottom-right]

[STAKES — 3 cards, white background]
  "$100M+ aquaculture losses" / "7 days advance warning" / "9 stations monitored"
  [Fraunces numerals, DM Sans descriptions]

[HOW IT WORKS — asymmetric grid, light background]
  Left: NDBC data → Random Forest → weekly risk [DM Sans body]
  Right: California map screenshot / embed [showing current stations]

[BLOOM AS PROTAGONIST — full-width, #f4efe5 warm sand]
  No icons. No bullet points.
  Fraunces italic pull quote about Alexandrium catenella, saxitoxins, domoic acid.
  The facts stated plainly. One green border-left accent.

[THRESHOLD CTA — full viewport, #0b1920]
  California map loading at 30% opacity behind content
  Headline: "Right now." [Fraunces 200 italic, 72–80px]
  Live station counts [DM Mono]
  "Click anywhere to open the map"
  Click resolves map opacity to 100%

[FOOTER — #10202a]
  Data sources / Model info / Team [DM Mono, 3-column]
```

## Dashboard Structure

```
[TOPBAR — #10202a, 52px fixed]
  ← Overview link | HAB Predictor [Fraunces] | subtitle [DM Mono]
  RIGHT: Last updated timestamp | ● N HIGH-RISK STATIONS alert badge

[APP — flex row, full height below topbar]
  [MAP — flex:1]
    Leaflet OSM tiles
    Station dots (colored by risk, clickable)
    Popup: dark navy, station name + risk % + sensor data
    Zoom controls: bottom-right, dark navy styled
    Toolbar overlay: legend + "California HAB Monitoring Stations" chip

  [SIDEBAR — 380px fixed]
    Metrics strip (4 cells): [Fraunces numerals] stations / high / elevated / clear
    Filter tabs: All / High / Elevated / Low [DM Mono]
    Station cards (scrollable):
      Left border by risk color
      Station name [DM Sans 600] + buoy ID + week [DM Mono]
      Risk % [Fraunces 700, risk color] + label [DM Mono]
      Feature grid: SST / NDBC temp / wind / chlorophyll [DM Mono]
    Model performance: AUC-ROC / F1 / Recall / Precision [DM Mono]
    Disclaimer [DM Mono, 10px]
```

## Implementation Notes

**Landing page:** New file — `website/index.html` or `index.html` at repo root. Self-contained HTML + CSS + minimal JS. No framework needed.

**Dashboard:** Modify `dashboard/build_dashboard.py`:
1. Add Leaflet CSS/JS CDN links to `HTML_TEMPLATE`
2. Add Google Fonts link (Fraunces + DM Sans + DM Mono)
3. Replace static tile CSS/HTML with Leaflet map div
4. Update all CSS custom properties and font-family declarations
5. Keep `dashboard_json` Jinja2 injection — Leaflet reads it for station positions
6. Remove `map_tiles` Jinja2 template loop (no longer needed)
7. Update `build_map_tiles()` function — can be removed or kept as fallback

## Decisions Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-05-18 | Fraunces as display serif | No HAB monitoring site uses a display serif. Signals science journalism, not government portal. Accessible via Google Fonts. |
| 2026-05-18 | Dark hero for landing page | Differentiates from the all-light dashboard. Creates visual separation and urgency before the user enters data mode. |
| 2026-05-18 | SVG tide chart background | Data-as-decoration. Proves the system is real before a single word is read. Historical bloom events rendered as oscillating lines. |
| 2026-05-18 | Map-as-threshold CTA | Dashboard map loads at 30% opacity as the page ending. User realizes they're already at its edge. The CTA is the content. |
| 2026-05-18 | Bloom-as-protagonist section | Frames the bloom as a natural phenomenon before a threat. Warm sand section, long-form prose, no bullet points. Move a government portal wouldn't make. |
| 2026-05-18 | Leaflet.js for dashboard map | Replaces static OSM tile image positioning. Real geo navigation (zoom, pan, click). Adds popup interaction without changing the data pipeline. |
| 2026-05-18 | Risk colors unchanged | `#2f8f68` / `#c4881a` / `#c63d3d` preserved exactly from existing dashboard. Visual coherence when navigating between landing page and dashboard. |

# Presentation Script — HAB Prediction Pipeline
*Read time: ~6–7 minutes*

---

## Slide 1 — Introduction

Harmful algal blooms are a growing threat along the California coastline. When certain
species of phytoplankton multiply rapidly, they produce toxins — domoic acid and
saxitoxin — that accumulate in shellfish and move up the food chain, causing illness in
humans and mass die-offs in sea lions, seabirds, and whales.

Beyond the environmental toll, bloom events force emergency closures of commercial and
recreational fisheries, costing the state millions of dollars per incident. And as ocean
temperatures rise, blooms are becoming more frequent and harder to predict.

Our goal with this project was to build a machine learning pipeline that can flag a
likely harmful bloom before it's confirmed by a water sample — giving agencies a
one-week lead time to act.

---

## Slide 2 — Data

We pulled data from three sources.

First, **CalHABMAP** — a statewide monitoring program that collects weekly water samples
at ten stations along the California coast, from Trinidad Pier in the north down to
Scripps Pier in San Diego. Each sample records nutrient levels — nitrate, silicate,
chlorophyll — and a binary flag for whether a harmful bloom was observed.

Second, **NOAA OISST** — a satellite-derived sea surface temperature product. We used it
to compute rolling 14-day SST averages, anomalies relative to the historical baseline,
and warm degree-days — all features that capture ocean heat accumulation.

Third, **NOAA NDBC buoy 46011**, moored off Point Arguello. This gives us wind speed,
wave height, atmospheric pressure, and air temperature at weekly resolution.

After merging and cleaning, we had just over 3,000 weekly observations spanning 2017 to
2026. About 6 percent of those observations are labeled as harmful blooms — so the
dataset is highly imbalanced, which turned out to be the central modelling challenge.

---

## Slide 3 — Feature Engineering

Before modelling, we engineered two additional features.

The first is **season** — winter, spring, summer, or fall — derived from the sample
month. Upwelling patterns on the California coast are strongly seasonal, and so is the
nutrient availability that drives bloom conditions.

The second is **rolling bloom rate** — for each station, we computed the 52-week
trailing average of bloom occurrences, shifted forward by one week so the current
observation is never included. This gives the model a sense of whether a station has
been in a bloom-prone state recently, without leaking any future information.

After feature engineering we had 29 input features per observation.

---

## Slide 4 — Train / Validation / Test Split

We used a sliding window split rather than a random shuffle, because bloom prediction is
fundamentally a time-series problem — you can only use the past to predict the future.

- **Training set**: 2021 and 2022 — 711 rows, 3% bloom rate
- **Validation set**: 2023 — 357 rows, 4.5% bloom rate — used exclusively for tuning
- **Test set**: 2024 onward — 716 rows, 11.6% bloom rate — held out until final evaluation

That jump in bloom rate — from 3% in training to nearly 12% in the test set — is
something we'll come back to, because it turns out to be the defining challenge of the
whole pipeline.

---

## Slide 5 — Models & Threshold Tuning

We trained three models: Logistic Regression with balanced class weights, a Random
Forest with 200 trees and balanced class weights, and XGBoost with a scale parameter
set to roughly 33 — the ratio of non-bloom to bloom rows in the training set.

One thing we learned early is that the default 0.5 decision threshold doesn't work here.
Every model trained on this dataset predicts zero positives at 0.5, because the training
bloom rate is so low that the model never reaches 50% confidence. So for each model, we
swept thresholds from 0.10 to 0.90 on the validation set and picked the one that
maximised F1 score. That threshold was then locked in before touching the test set.

---

## Slide 6 — Results

Here's what we found. [Point to comparison table]

Every model trained on the full history with no threshold tuning predicted zero blooms.
Accuracy looked fine — around 88% — but precision, recall, and F1 were all zero. That's
the class imbalance problem in action.

With threshold tuning on the sliding window split, Logistic Regression came out on top
with a test F1 of 0.247 and a recall of 0.578. That means it correctly flagged 48 out
of 83 actual bloom events in the test set.

The Random Forest and XGBoost models did not perform as well. Their validation F1 scores
were zero regardless of threshold — the 2023 validation year had only 16 bloom events,
which wasn't enough signal to tune the non-linear models reliably. Both defaulted to the
minimum threshold of 0.10 and still couldn't recover strong recall on the test set.

---

## Slide 7 — Best Model Deep Dive

Looking at the best model in more detail. [Point to the three diagnostic plots]

The confusion matrix shows the tradeoff the model makes at threshold 0.35: it catches
48 of 83 blooms, but at the cost of 258 false alarms. In a real early-warning context,
false alarms are costly but a missed bloom is worse — so we deliberately tuned toward
recall.

The ROC curve gives us an AUC of 0.62. That's above random but modest, which reflects
how difficult the temporal shift makes this task. The precision-recall curve tells a
similar story — average precision of 0.185, well above the no-skill baseline of 0.116
but with significant room to improve.

---

## Slide 8 — Feature Importance

The Random Forest gives us a window into which variables matter most. [Point to bar chart]

The top feature is **14-day rolling sea surface temperature** — the ocean's thermal
memory over two weeks is the single strongest signal. Close behind is **lagged nitrate**
— specifically, the nitrate level from one week prior. When nitrate is depleted, harmful
species that can subsist on trace nitrogen have a competitive advantage.

**Rolling bloom rate** comes in at number five, which confirms the intuition that
bloom-prone sites stay bloom-prone. Wave **dominant period** also appears, likely as a
proxy for offshore swell and the mixing conditions it creates.

The picture these features paint is physically coherent: warm, stratified, nutrient-poor
water with a history of prior blooms is the danger zone.

---

## Slide 9 — Key Findings & Next Steps

Let me summarise what we found and where we'd go next.

**What worked**: threshold tuning on a held-out validation year was essential — without
it, every model predicted zero blooms. Logistic Regression generalised better than the
more complex models under a distributional shift, because its simpler decision boundary
degrades more gracefully when the test environment differs from training.

**The core challenge**: bloom frequency nearly quadrupled between our training window
and the test period. We believe this reflects a structural shift in ocean conditions,
possibly linked to the 2023-24 El Niño. Any model calibrated on pre-2024 data will
systematically under-predict the current regime.

**Three things we'd do next**:

One — add the NOAA Coastal Upwelling Transport Index. Upwelling is the primary mechanism
that brings cold, nutrient-rich deep water to the surface, and its absence or reversal
is what sets the stage for bloom conditions. We don't have it in this feature set and
it's the most obvious gap.

Two — calibrate the Random Forest and XGBoost probabilities using isotonic regression
on the validation set. These models compress their predicted probabilities, which is why
threshold tuning failed for them. Calibration would fix that.

Three — explore sequence models. We have weekly observations at ten stations over a
decade. That's a natural fit for an LSTM or Transformer architecture that can learn
temporal patterns across multiple sites simultaneously — something a tabular model
simply can't do.

The bottom line is that this pipeline demonstrates the problem is learnable — we can
catch more than half of harmful blooms from environmental data alone — but closing the
gap to operational reliability will require richer features and models that adapt to a
shifting ocean.

Thank you.

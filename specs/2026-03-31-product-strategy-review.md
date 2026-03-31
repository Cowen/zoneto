# Product Strategy Review — 2026-03-31

**Role:** Product Owner
**Date:** 2026-03-31

---

## Executive Summary

Two weeks ago, this product was a prediction pipeline writing to Parquet files
that no human could read. Today it is a functioning web application with comps
search, address geocoding, scenario modelling, SHAP explanations, and a Docker
image. The March 17 strategy review called for a pivot from "predict approval" to
"development intelligence" — and the team executed it decisively. 43 commits
shipped across Phases 1–3 of the recommended roadmap.

The product has crossed the threshold from "interesting data pipeline" to "usable
tool." The strategic question is no longer "what should we build?" — it is "who
can actually use this, and how do we keep it alive?"

Three specific recommendations follow:

1. **Get it deployed.** The product exists only on localhost. Until it runs on a
   URL someone can bookmark, it has zero users and zero feedback.
2. **Automate data freshness.** The AIC scraper and OLT scraper exist but run
   manually. Stale data kills a comps product — a developer who finds their
   neighbor's application missing will never come back.
3. **Double down on "What could I build here?"** The scenario comparison panel is
   the most differentiated feature in the product. Comps are commodity; scenario
   modelling is not.

---

## What Got Built Since March 17

The March 17 review proposed three phases. Here is what actually shipped:

### Phase 1 (Make data accessible) — Complete

| Deliverable | Status |
|---|---|
| FastAPI serving layer (`/health`, `/ready`, `/comps`, `/score`) | Shipped |
| DuckDB comps query builder with spatial filtering | Shipped |
| Vanilla HTML frontend with comps search | Shipped |
| `just serve` command | Shipped |
| Docker image and `just docker-build/run` | Shipped |
| Address geocoding (`GET /geocode` via Nominatim) | Shipped (exceeded plan) |

### Phase 2 (Fix data source crisis) — Complete

| Deliverable | Status |
|---|---|
| AIC scraper expanded to full application records (`fetch_aic_applications`) | Shipped |
| `AICSource` registered in SOURCES replacing retired CKAN dataset | Shipped |
| OLT decision scraper (`fetch_olt_decisions`) | Shipped |
| OLT fuzzy matching to dev_applications (`match_olt_to_dev`) | Shipped |

### Phase 3 (Model improvements) — Complete

| Deliverable | Status |
|---|---|
| MTSA spatial feature (`in_mtsa`) | Shipped |
| SHAP explanations (`explain_one`, `?explain=true` on `/score`) | Shipped |
| TF-IDF + SVD NLP features (`desc_svd_0..19`) | Shipped |
| COA and permit scoring retired (dev_applications only) | Shipped |

### Beyond the plan

| Feature | Notes |
|---|---|
| "What could I build here?" scenario panel | Scores 5 hypothetical development scenarios against nearest comp's spatial context |
| Folderrsn deduplication in comps | Prevents duplicate rows from confusing results |
| AIC portal links in comps results | Direct link to source of truth for each application |

**Assessment:** The roadmap is complete. Every P0 and P1 item from the March 17
review has shipped. The product exceeded the plan by adding geocoding, scenario
modelling, and AIC deep links. This is a successful execution sprint.

---

## Current State (As of 2026-03-31)

### What Exists

**A working web application** at `localhost:8000` that:
- Accepts an address, geocodes it, and returns comparable development applications
  within a configurable radius
- Shows application outcomes (approved, appealed, active), timelines, proposed
  scale (storeys, units), and spatial context (heritage, secondary plan, MTSA)
- Links each comp to the AIC portal for verification
- Scores applications for appeal risk with SHAP explanations
- Models 5 "What could I build here?" scenarios using the nearest comp's spatial
  context fields
- Runs in Docker

**Two production-ready ML models:**
- `dev_applications_appealed` (CalibratedClassifierCV, AUC ~0.67–0.69)
- `dev_days_to_decision` (Survival, C-index ~0.74)

**A live data pipeline** that can ingest from AIC ArcGIS (replacing the retired
CKAN dataset) and OLT published decisions.

### What Does Not Exist

1. **No deployment.** The product runs on localhost only. No hosted URL, no DNS,
   no TLS, no auth. Zero external users.
2. **No automated data refresh.** The AIC and OLT scrapers run manually via CLI.
   There is no cron job, no scheduled pipeline, no freshness monitoring.
3. **No user feedback mechanism.** No analytics, no usage tracking, no way to
   know if the comps results are useful or the scenario panel is being used.
4. **No data quality monitoring.** No alerts if a scrape fails, if row counts
   drop unexpectedly, or if model metrics degrade.
5. **No multi-user support.** No accounts, no saved searches, no query history.

---

## Step Back: Three Strategic Questions

### 2a. Should we consider new data sources?

**No — not right now.** The data source crisis identified in March is resolved.
The product now has:
- AIC live application records (replacing the retired CKAN dataset)
- OLT decisions (enriching appeal labels)
- Zoning, heritage, secondary plans, MTSA, ward demographics (spatial context)
- NLP features from application descriptions

The data sources that *would* matter next are:
1. **Provincial planning policy changes** (ministerial zoning orders, as-of-right
   MTSA densification). These change the rules mid-game and make historical comps
   misleading for transit-adjacent sites. But this is policy data that doesn't
   exist in a structured, scrapeable form.
2. **Community engagement signals** (objection counts, deputations at committee).
   These predict appeals better than any structural feature. But they're buried
   in meeting agendas and minutes PDFs — extraction is a major NLP project.
3. **OLT decision full text** for appeal *outcome* prediction (does the appellant
   win?). This is more valuable than appeal *filing* prediction. The OLT scraper
   already captures case metadata; the decision text is a natural extension.

None of these should be pursued before the product has real users generating real
feedback on what's missing. **Adding data sources without user signal is the same
trap as improving models without a serving layer — optimizing in a vacuum.**

The one data investment worth making now is not a new source but **keeping existing
sources fresh.** An automated weekly pipeline (sync AIC, enrich, retrain, redeploy)
is worth more than any new dataset.

### 2b. Should we consider new users or optimize targeting?

**Yes — but the answer is "get the first user" before debating segments.**

The March 17 review identified four segments and recommended targeting Segment 1
(development firms doing site acquisition). That recommendation stands. But the
product has had exactly zero users from any segment. The theoretical segmentation
is untested.

The more productive framing: **who is the easiest first user?**

The easiest first user is not a development firm. Development firms have existing
workflows (paid UrbanToronto subscriptions, internal research teams, Bousfields
reports). Breaking into their workflow requires credibility, sales, and integration
with their existing tools.

The easiest first user is **the product owner himself.** If the developer building
Zoneto is also doing site acquisition due diligence in Toronto, the product's first
test is: does it actually help *you* make a decision you're facing right now?
Dog-fooding on a real parcel evaluation would:
- Reveal UX gaps that no amount of simulated critique can find
- Generate a concrete case study for showing to potential users
- Test whether the data is current enough to be useful (or whether the manual
  pipeline is already stale)

If the developer is *not* doing site acquisition, the next easiest user is **a
planning consultant who owes someone a favor.** One real user session — watching
someone query the tool for a case they're actually working on — is worth more
than ten strategy reviews.

**Recommendation: Before any new user segment work, get one real human to use the
product on a real decision and watch what happens.**

### 2c. Should we change the goals themselves?

**The goal pivot from March 17 was correct and should be sharpened, not changed.**

The current goal (from the README):
> Help Toronto development professionals make informed decisions by providing
> structured intelligence on comparable planning applications, outcome patterns,
> and expected timelines.

This is accurate but generic. It could describe any planning data product. What
makes Zoneto distinctive is now visible in the product itself:

1. **Spatial comps** — "show me what happened within 500m of this address" is
   something no existing tool does well for Toronto planning applications
2. **Scenario modelling** — "what would happen if I proposed a 12-storey OZ vs a
   6-storey SA?" is a genuinely novel question the product can answer (roughly)
3. **Appeal risk quantification** — even at AUC 0.67, an evidence-based appeal
   probability with SHAP explanations is more rigorous than consultant intuition

The goal should sharpen around the scenario modelling angle:

**Proposed refined goal:**
> Help Toronto development professionals evaluate what they could build on a
> specific site — using comparable applications, scenario modelling, and ML-based
> risk assessment to inform site acquisition and pre-application strategy.

This shifts the frame from "intelligence platform" (passive) to "site evaluation
tool" (active). It centers the "What could I build here?" feature as the hero,
with comps and predictions as supporting evidence.

**Why this matters:** "Development application intelligence" is a category that
established players (UrbanToronto, Bousfields, planning consultants) already
serve. "Scenario modelling for site acquisition" is a gap. Nobody offers
"enter an address, see 5 development scenarios scored for appeal risk and
timeline" in the Toronto market. This is the feature that could create a
category, not just enter one.

---

## What I'd Actually Build Next

### Phase 4: Get it in front of a human (Weeks 1–2)

The product works locally. It needs to work for someone else.

**1. Deploy to a cloud VM behind a domain.**

A single `$5-20/month` VPS (DigitalOcean, Fly.io, Railway) running the Docker
image behind a reverse proxy with TLS. No auth for now — the data is all public
(City of Toronto open data). Ship a URL that can be shared.

Scope: DNS, TLS cert, `docker-compose` with caddy/nginx, health check endpoint
already exists. This is a weekend project, not an engineering effort.

**2. Automated weekly pipeline.**

A cron job (on the VM or via GitHub Actions) that runs:
```
zoneto sync --source aic_applications
zoneto aic
zoneto olt
zoneto enrich --fetch-ref --fetch-aic --fetch-olt
zoneto train
zoneto score
```

This keeps the data current without manual intervention. The `just status` command
already exists to verify freshness. Add a `/status` API endpoint that returns
data timestamps so staleness is visible in the UI.

**3. One real user session.**

Find one person — friend, colleague, planning consultant, anyone — who is
evaluating a real Toronto development site. Sit with them (or watch a screen
share) as they use the product. Take notes on:
- What they search for first
- What questions the tool can't answer
- Whether they trust the comps results
- Whether the scenario panel is useful or confusing
- Whether they'd come back

This session will generate more actionable product direction than any spec.

### Phase 5: Sharpen the scenario tool (Weeks 2–4)

The "What could I build here?" panel is the most interesting thing in the
product, but it's currently a prototype — it scores 5 hardcoded scenarios using
the nearest comp's spatial context. To make it compelling:

**1. Let users define custom scenarios.**

Instead of 5 fixed scenarios, let the user specify: application type, proposed
storeys, proposed units. Score that specific scenario against the site's spatial
context. This turns the tool from "here are some possibilities" to "here is
*your* proposal evaluated."

**2. Add timeline estimates to scenarios.**

The survival model already produces p25/p50/p75 percentiles. Show these alongside
appeal risk: "A 12-storey OZ at this location: 18% appeal risk, estimated
1.8–3.2 years to decision." This is the single most valuable sentence the product
can generate.

**3. Show the comparable evidence behind each scenario.**

For each scored scenario, show the 3–5 most similar historical applications. This
lets users verify the prediction against real outcomes: "The model says 18% appeal
risk; here are 4 similar OZ applications nearby — 1 was appealed (25%), consistent
with the estimate."

### Phase 6: Decide on a business model (Weeks 4–6)

This is premature until Phase 4's user session happens, but the options are:

| Model | Fit | Risk |
|---|---|---|
| **Free tier + paid reports** | One-off comps queries free; detailed scenario reports with SHAP explanations behind a paywall ($50–200/report) | Requires payment integration, low conversion without brand trust |
| **Monthly subscription** | Unlimited queries for $99–299/month | Requires enough ongoing value to justify recurring payment; suits planning consultants better than one-off developers |
| **API-as-a-service** | Developers integrate Zoneto comps/scoring into their own tools via API keys | Requires API stability, rate limiting, documentation; higher LTV but longer sales cycle |
| **White-label for consultancies** | Bousfields, Goldberg, Urban Strategies embed Zoneto data in client reports | Highest revenue potential; requires enterprise sales capability |

**Recommendation: Don't build payment infrastructure yet.** Deploy free, get
users, learn what they value, then monetize. Premature monetization gates
feedback.

---

## What to Stop Doing

### 1. Stop the model improvement loop

The appeal model is at AUC ~0.67–0.69. The survival model is at C-index ~0.74.
Both are "good enough" for a product where models supplement comps, not replace
them. Further model engineering (new features, new architectures, hyperparameter
tuning) will produce single-digit AUC improvements that no user will notice.

The models will improve naturally when:
- More OLT decisions are scraped and matched (better appeal labels)
- More AIC records accumulate over time (larger training set)
- User feedback reveals which predictions are wrong (targeted fixes vs. shotgun features)

**Do not start another feature engineering cycle without a specific user complaint
motivating it.**

### 2. Stop writing strategy specs without user data

This is the tenth strategy/critique document in the `specs/` folder. Each one has
been written by AI playing the role of users who don't exist. The feedback loop is:
AI builds → AI critiques → AI recommends → AI builds. This loop has been
productive (it produced a working product!) but it has reached diminishing returns.

The next spec should be written *by a human user* or *based on observation of a
human user.* No more simulated user feedback rounds until there is a real user to
simulate.

### 3. Stop treating localhost as "shipped"

The product is Docker-ready but not deployed. Every day it sits on localhost is a
day of zero user learning. The deployment is not a "nice to have" — it is the
blocker for every strategic question (who are our users? what do they value? would
they pay?).

---

## Risks and Counter-Arguments

**"We need more model work before showing this to users."** No. The models are
supplementary to the comps, which require no ML at all. A user who finds useful
comps will tolerate imperfect predictions. A user who never sees the product
because it's not deployed will never tolerate anything.

**"Deployment is ops work, not product work."** Deployment *is* product work when
the product doesn't exist without it. A tool that only runs on the developer's
machine is a personal utility, not a product. The gap between localhost and a URL
is the gap between zero users and potential users.

**"The data might go stale and embarrass us."** This is the strongest argument for
delay — and it's an argument for automated refresh (Phase 4, step 2), not for
staying on localhost. Stale data on a deployed product is fixable. Zero users on
a localhost product is not.

**"We should validate the business model before investing in deployment."** The
business model cannot be validated without users. Users cannot be acquired without
deployment. This is circular. Deploy free, learn, then monetize.

**"The scenario panel is too rough to ship."** Ship it rough. The five hardcoded
scenarios are better than nothing. Real user feedback on which scenarios matter
will guide Phase 5 much better than internal speculation.

---

## Summary

| Decision | Reasoning |
|---|---|
| **Deploy to a public URL** | Zero users on localhost = zero learning; deployment is the blocker for all strategic progress |
| **Automate weekly data refresh** | Stale comps kill trust; manual pipeline won't survive contact with real usage patterns |
| **Get one real user session** | Ten AI-written strategy specs < one hour watching a real human use the tool |
| **Sharpen around scenario modelling** | "What could I build here?" is the differentiated feature; comps are commodity |
| **Stop model improvement loop** | AUC 0.67 + comps evidence > AUC 0.72 with no users to see it |
| **Stop simulated user feedback** | The AI-critiquing-AI loop produced a product; now it needs real signal |
| **Defer monetization** | Deploy free → get users → learn what they value → then charge |

The product is better than it has any right to be after three weeks of development.
The risk is not that it's bad — it's that it stays invisible. Ship it.

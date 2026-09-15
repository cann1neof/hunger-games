# Job Radar — execution plan

A daily job-collection pipeline and market-intelligence dashboard for the Israeli
data / AI / engineering market. Built as a working personal tool first and a
portfolio artifact second — in that order, always.

This document is the spec. Work through it phase by phase. Do not skip ahead.

### Status — this is not the final version

This plan is a living draft and is expected to change as the tool meets real
data. Treat it accordingly:

- **Parameters are starting points, not settled values.** Every weight,
  threshold, window size, score, taxonomy entry and query term in this document
  is a first guess. They will be revised — some of them heavily — once there are
  real postings to test against. Do not treat any number here as tuned.
- **The schema will move.** Phase 1 deliberately ends with a week of real usage
  before phase 2 begins, precisely because that week will expose fields that are
  missing, unused, or wrongly typed. Expect migrations.
- **Structural decisions in section 2 are stable but not frozen.** Don't swap
  them silently, but do raise it if implementation reveals one of them is wrong.
- **Later phases are sketched, not specified.** Phases 1 and 2 are detailed
  enough to build from. Phases 3 and 4 will be refined before they start, with
  what was learned from the phases before them.

When something in this plan conflicts with what the data actually shows, the
data wins — flag the conflict and propose the revision rather than implementing
the stale instruction.

---

## 0. Context and goal

**Who this is for.** A data/product analyst in Israel running an active job
search, who also wants the repo to demonstrate engineering ability to hiring
managers.

**What it must do.**

1. Collect job postings daily from multiple boards, keyed off a configurable
   query matrix.
2. Deduplicate them across boards and over time, so the same role seen on
   LinkedIn and Indeed is one row.
3. Enrich free-text descriptions into structured, analyzable columns.
4. Surface new postings in a triage inbox that can be cleared in ten minutes.
5. Track the market over time — what titles recur, which companies hire, what
   the stack demands look like.
6. Export selected postings as clean markdown for analysis in a chat.

**What "done" means for each phase** is written as acceptance criteria. A phase
is not finished until every criterion passes.

---

## 1. Non-negotiable conventions

Read this section before writing any code. These are not suggestions.

- **Python 3.11+**, dependency management with `uv`. No poetry, no bare pip.
- **Raw data is immutable.** Every source response is written to
  `data/raw/<source>/<date>/<query_hash>.parquet` and never modified or deleted
  by any downstream step. All parsing is replayable from raw.
- **Every stage is idempotent.** Running the pipeline twice on the same day must
  produce the same end state, not duplicates. Upserts everywhere, no blind
  inserts.
- **User-owned columns are sacred.** `status`, `my_notes`, `my_rating` are
  written only by the dashboard. No pipeline step may ever overwrite them.
- **No secrets in the repo.** `.env` for API keys, `config/profile.yaml` for
  anything personal (salary targets, personal keyword weights) and both in
  `.gitignore`. Ship `config/profile.example.yaml` instead.
- **Type hints on every function signature.** `ruff` for lint and format,
  config in `pyproject.toml`, line length 100.
- **Structured logging** via `structlog` to both stdout and
  `logs/run-<date>.jsonl`. No bare `print` outside the CLI.
- **Ask before inventing.** If a decision isn't specified here and it's
  load-bearing, stop and ask rather than guessing. Small implementation details
  are yours to choose.

---

## 2. Locked technical decisions

These were decided deliberately. Do not substitute alternatives. If you think a
decision is wrong, say so and wait — don't silently swap.

| Concern | Choice | Reason |
|---|---|---|
| Collection | `python-jobspy` | Multi-board in one library; LinkedIn, Indeed, Glassdoor, Google |
| Storage | DuckDB, single file `data/jobradar.duckdb` | Columnar, zero-setup, great pandas interop, good analytics story |
| Transformation | `dbt-duckdb` | Real staging→mart layering with tests, run locally |
| Validation | `pandera` | DataFrame contracts between pipeline stages |
| Dashboard | Streamlit | Already a known tool; fastest path to a usable triage UI |
| Orchestration | Dagster (phase 4 only) | Asset graph + lineage UI; phases 1–3 run from a CLI |
| LLM | Anthropic API, Claude Haiku for extraction | Cheap, fast, sufficient for structured extraction |
| Tests | pytest + frozen fixtures | Board schema changes must break a test, not a Tuesday |
| CI | GitHub Actions — lint + tests only | Never run the scraper in CI (datacenter IP, geo-skewed results) |
| Scheduling | macOS `launchd` on the user's own machine | Residential Israeli IP is an asset; avoids blocks and geo skew |

---

## 3. Repo layout

Build this structure in phase 1 and keep it stable.

```
jobradar/
├── README.md
├── pyproject.toml
├── .env.example
├── .gitignore
├── config/
│   ├── queries.yaml              # the search matrix
│   ├── scoring.yaml              # rule-based fit score weights
│   ├── profile.example.yaml      # personal config template (real one gitignored)
│   └── taxonomy.yaml             # controlled vocabularies for extraction
├── src/jobradar/
│   ├── __init__.py
│   ├── cli.py                    # typer CLI: collect, enrich, build, serve, doctor
│   ├── config.py                 # pydantic settings + yaml loading
│   ├── models.py                 # pydantic models: RawPosting, Posting, RunRecord
│   ├── schemas.py                # pandera dataframe schemas
│   ├── sources/
│   │   ├── base.py               # abstract Source
│   │   ├── jobspy_source.py
│   │   └── registry.py
│   ├── store/
│   │   ├── duckdb_store.py       # connection, migrations, upserts
│   │   └── migrations/           # numbered .sql files
│   ├── dedup.py                  # job_key computation + cross-board resolution
│   ├── enrich/
│   │   ├── extractor.py          # LLM structured extraction
│   │   ├── prompts/
│   │   └── cache.py              # content-hash keyed, never re-extract
│   ├── scoring.py                # rule-based fit score
│   ├── observability.py          # run manifest, freshness checks, alerts
│   └── export.py                 # markdown export for chat analysis
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       ├── intermediate/
│       └── marts/
├── app/
│   ├── main.py                   # Streamlit entrypoint
│   └── pages/
├── evals/
│   ├── labeled.jsonl             # 50 hand-labeled postings
│   ├── run_eval.py
│   └── results/
├── tests/
│   ├── fixtures/                 # frozen JobSpy responses
│   ├── test_dedup.py
│   ├── test_schemas.py
│   └── test_sources.py
├── docs/
│   ├── architecture.md
│   └── decisions/                # ADRs, numbered
├── scripts/
│   └── com.jobradar.daily.plist  # launchd template
└── data/                         # gitignored
    ├── raw/
    └── jobradar.duckdb
```

---

## 4. Phase 1 — collector and store

**Goal: real postings flowing into DuckDB daily, viewable in a minimal UI.**
This is the only phase that is load-bearing for the actual job search. Ship it
before touching anything else.

### 4.1 Source interface

Define the abstraction first, then implement JobSpy against it. This matters:
later Israeli boards must plug in without downstream changes.

```python
class Source(Protocol):
    name: str
    def fetch(self, query: Query) -> list[RawPosting]: ...
    def enrich_descriptions(self, postings: list[RawPosting]) -> list[RawPosting]: ...
    def health(self) -> SourceHealth: ...
```

`JobSpySource` wraps `scrape_jobs`. Key parameters:

- `country_indeed="israel"` for Indeed and Glassdoor
- `hours_old=48` on a daily run — overlap is free after dedup and survives a
  missed run
- `results_wanted` 25–50 per query
- `site_concurrency=1`, jittered delays between queries
- `linkedin_fetch_description=False` on the search pass

**Two-pass design, and this is important.** The search pass is cheap and
descriptionless. A second enrichment pass fetches full descriptions *only* for
`job_key` values not already present in the database. New postings per day will
be 10–40, so this stays fast and you never re-fetch a description you have.

Google is a separate, fiddly query type — its `google_search_term` must be the
literal string copied from a real Google Jobs URL after applying filters. Treat
it as an optional query entry and don't block on it.

### 4.2 Query matrix

`config/queries.yaml`. Two tracks, both active from day one. The whole point of
collecting daily is to learn which track actually has volume at this level.

```yaml
defaults:
  location: "Israel"
  country_indeed: "israel"
  results_wanted: 40
  hours_old: 48

queries:
  # --- analyst track ---
  - term: "product analyst"
    sites: [linkedin, indeed, glassdoor]
    track: analyst
  - term: "data analyst"
    sites: [linkedin, indeed, glassdoor]
    track: analyst
  - term: "business intelligence developer"
    sites: [linkedin, indeed]
    track: analyst
  - term: "analytics engineer"
    sites: [linkedin, indeed]
    track: analyst
  - term: "AI analyst"
    sites: [linkedin]
    track: analyst
  - term: "growth analyst"
    sites: [linkedin]
    track: analyst

  # --- builder track ---
  - term: "software engineer intern"
    sites: [linkedin, indeed]
    track: builder
  - term: "junior software engineer"
    sites: [linkedin, indeed]
    track: builder
  - term: "junior backend developer"
    sites: [linkedin]
    track: builder
  - term: "AI engineer"
    sites: [linkedin]
    track: builder
```

The `track` field flows all the way to the marts. It's what makes the
career-pivot question answerable with data.

### 4.3 Schema

Migrations as numbered SQL files under `src/jobradar/store/migrations/`.

```sql
CREATE TABLE postings (
  job_key           TEXT PRIMARY KEY,
  canonical_url     TEXT NOT NULL,
  title             TEXT NOT NULL,
  company           TEXT NOT NULL,
  location_raw      TEXT,
  city              TEXT,
  is_remote         BOOLEAN,
  job_type          TEXT,
  date_posted       DATE,
  min_amount        DOUBLE,
  max_amount        DOUBLE,
  currency          TEXT,
  salary_interval   TEXT,
  description       TEXT,
  description_hash  TEXT,
  first_seen_at     TIMESTAMP NOT NULL,
  last_seen_at      TIMESTAMP NOT NULL,
  times_seen        INTEGER NOT NULL DEFAULT 1,
  is_active         BOOLEAN NOT NULL DEFAULT TRUE,
  closed_at         TIMESTAMP,
  matched_queries   TEXT[],
  tracks            TEXT[],
  fit_score         DOUBLE,
  -- user-owned, never written by the pipeline
  status            TEXT NOT NULL DEFAULT 'new',
  my_notes          TEXT,
  my_rating         INTEGER
);

CREATE TABLE posting_sources (
  job_key       TEXT NOT NULL,
  site          TEXT NOT NULL,
  site_job_id   TEXT,
  url           TEXT NOT NULL,
  first_seen_at TIMESTAMP NOT NULL,
  PRIMARY KEY (job_key, site, site_job_id)
);

CREATE TABLE runs (
  run_id         TEXT PRIMARY KEY,
  started_at     TIMESTAMP NOT NULL,
  finished_at    TIMESTAMP,
  source         TEXT NOT NULL,
  query_term     TEXT,
  site           TEXT,
  rows_returned  INTEGER,
  rows_new       INTEGER,
  status         TEXT NOT NULL,      -- ok | partial | error
  error_message  TEXT,
  duration_ms    INTEGER
);
```

`status` values: `new | reading | shortlist | applied | interviewing | pass |
rejected`.

**Posting closure detection.** A posting not seen in a collection run whose
query previously returned it gets `is_active = FALSE` and `closed_at` set after
three consecutive misses. Three, not one — boards are flaky. This gives you
time-to-close as a derived metric later, which is genuinely interesting market
data and costs almost nothing to capture.

### 4.4 Deduplication

`job_key = sha1(norm(company) + "|" + norm(title) + "|" + norm(city))`

Normalization, in `dedup.py`, thoroughly unit-tested:

- lowercase, strip accents, collapse whitespace
- strip trailing location suffixes from titles (`- Tel Aviv`, `, Israel`)
- strip gender/inclusion markers (`(m/f/d)`, `m/f`)
- strip recruiting noise (`hiring now`, `urgent`, `!!!`, emoji)
- strip legal-entity suffixes from company (`ltd`, `inc`, `בע"מ`)
- **do not** strip seniority words — `senior data analyst` and `data analyst`
  are different roles and must stay different keys

When the same `job_key` arrives from a second board: keep the existing
`canonical_url`, append a row to `posting_sources`, and take the longest
non-null description. Update `last_seen_at`, increment `times_seen`.

Write the tests for this before the implementation. Hard cases to cover: same
role on three boards with different title punctuation; two genuinely different
roles at the same company with similar titles; a reposted job after a gap.

### 4.5 Fit scoring engine

This is the most interesting component in phase 1 and the one worth spending
extra time on. It is a port of a pattern-scoring approach the user built at Wix
for operational site categorization, and the design constraints are the same:
a bare keyword match is nearly worthless, because *where* a term appears and
*what surrounds it* carry most of the signal.

The motivating example from that system: searching all sites for "donations"
returns mostly false positives — sites saying "we do not collect donations", or
boilerplate legal text in terms and conditions. A page *titled* "Donate to us"
is worth far more than the word appearing once in a footer. The same is true
here: "SQL" in a benefits paragraph, "SQL" in a must-have requirement, and "no
SQL experience needed — we'll teach you" are three completely different signals
that naive keyword counting collapses into one.

The engine has six stages. Implement them in order, each independently testable.

**The configuration below is a v1 starting point and will be optimized.** The
pattern set, the base scores, the zone weights, the context cues and the
saturation ceiling are all first guesses written before any real posting has
been scored. The engine's *structure* is the deliverable; the numbers in it are
expected to be reworked substantially through the calibration loop described at
the end of this section. Build the config so that reworking it is cheap — no
weights hardcoded in Python, everything in `scoring.yaml`, every score row
stamped with the config version that produced it.

#### Stage 1 — zone segmentation

Split each description into zones by header detection, then weight matches by
the zone they land in. A skill under "requirements" means something very
different from the same skill under "nice to have" or in the equal-opportunity
boilerplate.

```yaml
# config/scoring.yaml
version: 1

zones:
  title:            {weight: 3.0}
  requirements:     {weight: 2.0}
  responsibilities: {weight: 1.6}
  tech_stack:       {weight: 1.8}
  nice_to_have:     {weight: 0.6}
  about_company:    {weight: 0.3}
  benefits:         {weight: 0.2}
  boilerplate:      {weight: 0.0}   # EEO, privacy, legal — scored at zero
  body:             {weight: 1.0}   # fallback for unsegmented text

segmenters:
  requirements: '(?im)^\W*(requirements?|qualifications?|what you.{0,15}(need|bring)|must[- ]have|who you are)\W*$'
  responsibilities: '(?im)^\W*(responsibilities|what you.{0,15}(do|own)|the role|day[- ]to[- ]day)\W*$'
  tech_stack: '(?im)^\W*(tech(nology)? stack|our stack|tools we use)\W*$'
  nice_to_have: '(?im)^\W*(nice[- ]to[- ]have|advantage|bonus points|preferred|a plus)\W*$'
  about_company: '(?im)^\W*(about (us|the company|<company>)|who we are)\W*$'
  benefits: '(?im)^\W*(benefits|what we offer|perks)\W*$'
  boilerplate: '(?im)^\W*(equal opportunity|diversity|privacy|legal|by applying)\W*'
```

A zone runs from its header to the next detected header. Text before the first
header falls into `body`. Unsegmentable descriptions — a single wall of prose,
which is common — degrade gracefully to all-`body` at weight 1.0, and this must
be tested explicitly.

#### Stage 2 — pattern matching

Regex patterns, not substrings. Each pattern carries a score, a group, and
optional zone overrides.

```yaml
patterns:
  - id: ab_testing
    regex: '\b(a[/\s-]?b tests?(ing)?|split tests?|online experiments?|experimentation (platform|culture|framework))\b'
    score: 6
    group: experimentation

  - id: experimentation_generic
    regex: '\bexperiment(s|ation)?\b'
    score: 2
    group: experimentation

  - id: product_analytics
    regex: '\bproduct analytics\b'
    score: 6
    group: analytics

  - id: analytics_generic
    regex: '\banalytics\b'
    score: 1
    group: analytics

  - id: event_tracking
    regex: '\b(event (tracking|schema|taxonomy)|tracking plan|instrumentation)\b'
    score: 5
    group: tracking

  - id: airflow
    regex: '\b(airflow|dagster|prefect|orchestrat(ion|or))\b'
    score: 3
    group: orchestration

  - id: llm_product
    regex: '\b(llm|genai|generative ai|ai[- ]first product|prompt engineering)\b'
    score: 5
    group: ai
    zone_overrides:
      about_company: 0.1     # "we are an AI company" is nearly meaningless

  - id: years_senior
    regex: '\b([7-9]|1[0-9])\+?\s*years?\b'
    score: -8
    group: experience_bar

  - id: hebrew_native
    regex: '\b(native|mother.?tongue)\s+hebrew\b|\bhebrew\s+(at\s+)?(native|mother.?tongue)\b'
    score: -6
    group: language
```

Every pattern must be accompanied by at least one positive and one negative test
case in `tests/test_scoring.py`. A pattern without tests does not get merged.

#### Stage 3 — negation and context handling

This is the stage that kills the false positives, and it is the reason the
engine exists.

For each match, examine a context window — the enclosing sentence, plus a
bounded token window on either side — for cues that invert or nullify the match:

```yaml
context_rules:
  negation:
    cues: ['\bno\b', '\bnot\b', '\bwithout\b', "\bdon.t\b", '\bnever\b',
           '\bnot required\b', '\bno need\b', '\bnot expected\b']
    window_tokens: 8
    effect: zero            # zero | invert | scale
    scale: 0.0

  will_teach:
    cues: ["we.{0,5}ll teach", '\byou.{0,5}ll learn\b', '\bwilling to learn\b',
           '\btraining provided\b', '\bno prior\b']
    window_tokens: 10
    effect: scale
    scale: 0.3              # still mildly positive — it signals the domain

  softener:
    cues: ['\ba plus\b', '\badvantage\b', '\bbonus\b', '\bpreferred\b',
           '\bnice to have\b']
    window_tokens: 8
    effect: scale
    scale: 0.4              # inline equivalent of the nice_to_have zone
```

The `softener` rule matters because many postings don't use headers at all —
they write "Python is a must, dbt an advantage" in a single line. Zone
segmentation can't catch that; inline context rules can.

Sentence boundary detection should be simple and dependency-free — split on
`.`, `;`, newline, and bullet markers. Do not add spaCy or NLTK for this.

#### Stage 4 — span overlap resolution

Matches frequently nest. "We run A/B tests to drive product analytics" fires
`ab_testing`, `experimentation_generic`, `product_analytics` and
`analytics_generic`, with the generic patterns matching *inside* the specific
ones. Counting all four triple-counts a single signal.

Resolution: collect all matches as `(start, end, pattern_id, group,
effective_score)` character spans, then apply greedy interval selection —

1. sort by `abs(effective_score)` descending, tie-broken by longer span
2. walk the list, keeping a match only if its span does not overlap any span
   already kept
3. matches in different zones are always independent, since spans can't cross
   zone boundaries

This means the specific, high-scoring pattern always wins over the generic one
it contains, which is the behavior you want. The `group` field is retained for
diagnostics and for stage 5.

#### Stage 5 — repetition saturation

A posting that says "SQL" nine times is not nine times better than one that says
it once — it's usually just a verbose posting. Apply diminishing returns per
group:

```
effective_group_score = group_max_score * (1 + ln(n_matches_in_group))
```

where `n` is the count of surviving matches in that group after stage 4. Cap `n`
at a configurable ceiling (default 5). This preserves "mentioned repeatedly is
somewhat stronger than mentioned once" without letting verbosity dominate.

#### Stage 6 — normalization

Sum the per-group scores into a raw score, then normalize.

**Length bias check first.** Raw scores correlate with description length by
construction. Compute the Spearman correlation between raw score and token count
across the corpus and log it. If it exceeds 0.3, divide raw by
`sqrt(tokens / median_tokens)` before z-scoring, and record that you did.

**Z-score, computed per track.** Analyst and engineering postings have different
pattern densities and must not be pooled — a z-score against a mixed corpus
would systematically rank one track above the other for reasons that have
nothing to do with fit.

```
fit_score = (raw_normalized - mean(track, window)) / std(track, window)
```

Use a rolling 90-day window with a minimum sample of 50 postings per track;
below that, fall back to the raw normalized score and flag it. Persist the
`(mean, std, window_end, n)` per track into a `scoring_stats` table on every run,
so any historical score is reproducible and drift is visible.

Store both `fit_score_raw` and `fit_score` (the z-score). The raw one is for
debugging; the z-score is for sorting. Display the z-score in the UI as a
percentile, since "this is in the top 8% of analyst postings I've seen" is
readable in a way that "1.41" is not.

#### Explainability — required, not optional

Every score must be fully traceable. Persist a `score_breakdown` JSON per
posting containing every surviving match: pattern id, matched text, zone, base
score, zone weight, context rule applied and its factor, final contribution.

The dashboard renders this as a "why this score" expander on each row. Two
reasons this is non-negotiable. First, tuning the pattern file is impossible
without seeing which patterns actually fired on which text. Second, it's the
thing that makes the component demonstrable to a reviewer — a score with no
explanation is a magic number, and the whole point here is that it isn't one.

This mirrors the debug tooling the user built at Wix, which exposed the
reasoning and sub-process breakdown behind each generated site. Same principle,
same value.

#### Calibration

The engine is tunable, so make the tuning measurable rather than a matter of
taste.

The dashboard's triage buttons already capture judgment — `shortlist` versus
`pass` is a label. Once roughly 80 postings have been triaged, add
`scripts/calibrate.py` reporting:

- Spearman correlation between `fit_score` and the user's own `my_rating`
- precision@10 and precision@25 — of the top N by score, how many were
  shortlisted
- the patterns most overrepresented in false positives (scored high, marked
  `pass`) and false negatives (scored low, marked `shortlist`)

That last output is the tuning loop. It tells you which patterns to reweight
instead of guessing.

#### Versioning

`scoring.yaml` carries a `version` field. Every score row stores the version
that produced it. Add `jobradar score --rescore-all` to recompute historical
scores after a config change, so the corpus stays internally comparable.

#### Scope boundary

This is for **sort order in the triage inbox**, and for a measurable, explainable
signal to put in the README. It is not judgment and not a recommendation engine.
Keep it pure regex and configuration in phase 1 — no LLM in this loop. In
phase 3 the extracted structured fields become *additional* inputs to the same
engine (a verified `min_years_experience` of 8 is a far more reliable
disqualifier than a regex hit on "8+ years"), but the rule engine stays
independently runnable and independently testable.

#### Hard gates, separate from the score

Some conditions should not be negative points — they should be a flag, because a
sufficiently keyword-rich posting can outscore a large penalty. Keep these
apart:

```yaml
hard_gates:
  - id: clearance
    regex: '\b(security clearance|סיווג ביטחוני)\b'
  - id: decade_plus
    regex: '\b(1[0-9]|[2-9][0-9])\+?\s*years?\b'
    zones: [requirements, title]
```

A gate hit sets `gate_flags` and drops the posting out of the default inbox view
behind a "show gated" toggle. It never touches `fit_score`.

### 4.6 CLI

`typer`, entry point `jobradar`:

```
jobradar collect [--dry-run] [--query TERM] [--site SITE]
jobradar enrich-descriptions
jobradar score
jobradar doctor          # freshness + config validation, exits non-zero on failure
jobradar serve           # launches Streamlit
jobradar export --status shortlist --out exports/
```

`jobradar daily` chains collect → enrich-descriptions → score → digest, and is
what the scheduler calls.

### 4.7 Minimal dashboard

Single Streamlit page. No polish yet.

- Triage inbox: `status = 'new'`, sorted by `fit_score` descending, gated
  postings hidden behind a toggle
- Each row an expander with full description, the score as a percentile, and a
  "why this score" breakdown table from `score_breakdown`
- Four buttons per row: shortlist / pass / applied / reading, plus a 1–5
  `my_rating` control that feeds calibration later
- Sidebar filters: track, site, city, remote, date range, min score
- An "export selected as markdown" button producing title, company, location,
  salary, fit score, full description — formatted for pasting into a chat

### 4.8 Daily digest

Write `digests/YYYY-MM-DD.md` on every daily run: top 10 new postings by score,
one line each, plus a health footer showing per-source row counts. This means
even on days the dashboard goes unopened there's something skimmable.

### 4.9 Scheduling

Generate `scripts/com.jobradar.daily.plist` targeting roughly 07:30 local, with
`StandardOutPath` and `StandardErrorPath` set. Include install and uninstall
instructions in the README. Make `jobradar daily` safe to run repeatedly — if it
already ran today, it should still work and not duplicate.

### Phase 1 acceptance criteria

- [ ] `jobradar collect` returns real Israeli postings from at least two boards
- [ ] Running it twice in a row creates zero duplicate rows
- [ ] `posting_sources` shows at least one job found on two different boards
- [ ] `runs` has one row per query/site combination with accurate counts
- [ ] Killing the process mid-run leaves the database in a valid state
- [ ] Dedup unit tests pass, including the three hard cases above
- [ ] Every scoring pattern has a passing positive and negative test case
- [ ] "We don't expect you to know dbt" scores at or near zero for dbt, while
      "strong dbt experience required" scores full weight
- [ ] A description with no detectable headers scores without error, all-`body`
- [ ] Overlapping matches resolve to one contribution — verified on a sentence
      firing four nested patterns
- [ ] `score_breakdown` reproduces the final score exactly when its
      contributions are summed
- [ ] Length-bias correlation is computed and logged on every scoring run
- [ ] Streamlit inbox loads, status buttons persist across restart
- [ ] The "why this score" expander renders every surviving match
- [ ] Markdown export produces clean, pasteable output
- [ ] launchd job fires and writes a digest

**Stop here and use it for a week before starting phase 2.** Real usage will
change the schema, and it's cheaper to learn that now.

---

## 5. Phase 2 — dbt models and data quality

**Goal: the layer that turns a database into a data product.**

### 5.1 dbt project

`dbt-duckdb`, profile pointing at `data/jobradar.duckdb`.

```
models/
├── staging/
│   ├── _sources.yml          # source freshness: warn 36h, error 72h
│   ├── stg_postings.sql      # typing, normalization, salary to monthly NIS
│   └── stg_runs.sql
├── intermediate/
│   ├── int_postings_enriched.sql
│   └── int_company_activity.sql
└── marts/
    ├── mart_triage_inbox.sql
    ├── mart_pipeline.sql
    ├── mart_market_daily.sql
    └── mart_stack_demand.sql
```

Salary normalization matters and is fiddly: postings mix monthly and annual,
NIS and USD, gross and net, with most Israeli listings omitting salary entirely.
Normalize to monthly NIS gross where possible, keep `salary_confidence` as a
column, and never silently coerce. Postings without salary are the majority —
that itself is a finding worth surfacing.

### 5.2 dbt tests

- `unique` and `not_null` on `job_key` in every model
- `accepted_values` on `status`, `track`, `site`
- `relationships` from `posting_sources.job_key` to `postings.job_key`
- source freshness checks that fail the build when data goes stale
- one custom singular test: no posting may have `first_seen_at > last_seen_at`
- one custom singular test: no `job_key` may appear in two tracks with
  contradictory seniority

### 5.3 pandera contracts

A schema per pipeline boundary — `RawPostingSchema` after collection,
`ScoredPostingSchema` after scoring. Validation failures write a row to `runs`
with `status='error'` and a readable message, then abort that source without
killing the whole run. One bad board must never take down the others.

### 5.4 Observability

- `jobradar doctor` checks: database reachable, last successful run under 36h
  old, every configured source returned rows in the last 3 days, no failed dbt
  tests. Exits non-zero on any failure.
- Alerting on failure via Telegram bot (simplest) or SMTP. Configurable, off by
  default in the example config.
- A health panel in the dashboard rendering the last 14 days of `runs` as a
  grid — green/amber/red per source per day. This is a README screenshot.

### Phase 2 acceptance criteria

- [ ] `dbt build` passes clean
- [ ] Deliberately corrupting a row causes a specific, named test to fail
- [ ] Deliberately breaking one source leaves the others' data intact
- [ ] `jobradar doctor` exits non-zero when data is stale
- [ ] Health grid renders 14 days of history

---

## 6. Phase 3 — LLM enrichment and evaluation

**Goal: the part that differentiates this repo. Build it carefully.**

### 6.1 Extraction

Anthropic API, Claude Haiku, structured JSON output. Runs once per unique
`description_hash` and the result is cached — descriptions are never re-extracted
and re-running costs nothing.

Fields, all against a controlled vocabulary in `config/taxonomy.yaml`. Free-text
output here would be useless for aggregation, which is the entire point.

| Field | Type | Values |
|---|---|---|
| `seniority_band` | enum | intern, junior, mid, senior, lead, manager, unknown |
| `min_years_experience` | int or null | extracted, not guessed |
| `role_family` | enum | data_analyst, product_analyst, bi_developer, analytics_engineer, data_engineer, data_scientist, ml_engineer, software_engineer, other |
| `work_model` | enum | onsite, hybrid, remote, unknown |
| `hebrew_required` | bool or null | |
| `degree_required` | enum | none, bachelors, masters, unknown |
| `tools` | list | from taxonomy: sql, python, dbt, airflow, snowflake, bigquery, tableau, looker, power_bi, spark, streamlit, react, ... |
| `domain` | enum | fintech, adtech, security, gaming, ecommerce, healthtech, devtools, other |
| `disqualifiers` | list | free-form short strings, capped at 3 |
| `one_line_summary` | string | ≤ 20 words |

Rules for the prompt: extract only what is stated, return `unknown` or `null`
rather than inferring, and never invent a tool that isn't named in the text. An
extraction that says "unknown" is correct; one that guesses is a bug.

### 6.2 The eval set — do not skip this

1. Hand-label 50 postings sampled across tracks and boards. Commit as
   `evals/labeled.jsonl`. The user labels these personally; do not generate
   labels with a model, that defeats the purpose entirely.
2. `evals/run_eval.py` runs the current extraction prompt against all 50 and
   reports per-field metrics:
   - exact match accuracy for enums
   - within-±1 accuracy for `min_years_experience`
   - precision / recall / F1 for `tools`
   - a confusion matrix for `role_family`
3. Results written to `evals/results/<timestamp>.json`, with a small table
   rendered to markdown for the README.
4. A pytest test asserting overall accuracy stays above an agreed floor, so a
   prompt change that regresses quality fails CI.

This is the single most credible artifact in the repo. The accuracy table goes
in the README above the fold.

### 6.3 Cost control

Batch requests, cap spend per run, log token usage per run into `runs`. Add a
`--max-cost` flag that aborts when exceeded. Expect this to cost cents per day.

### Phase 3 acceptance criteria

- [ ] Extraction runs on new postings only; re-running costs nothing
- [ ] 50 labeled examples committed
- [ ] `run_eval.py` produces a per-field metrics table
- [ ] The accuracy floor test passes in CI
- [ ] Malformed LLM output is caught and retried once, then logged as a failure
      rather than crashing the run

---

## 7. Phase 4 — orchestration, dashboard, publication

### 7.1 Dagster

Convert the CLI steps into assets: `raw_postings` → `deduped_postings` →
`enriched_postings` → `dbt_models` → `daily_digest`. A daily schedule. Asset
checks wrapping the existing freshness and pandera validations.

Keep the CLI working. Dagster orchestrates it; it does not replace it. A
reviewer who doesn't want to run Dagster should still be able to run
`jobradar daily`.

### 7.2 Dashboard, full version

Three pages.

**Triage inbox** — as phase 1, plus filters on the extracted fields. Filtering
by `seniority_band` and `min_years_experience` is what makes the inbox genuinely
fast to clear.

**Pipeline** — everything shortlisted or beyond, with status, days since
application, and notes. Simple, but it stops applications getting lost.

**Market view** — the part that serves the career question:

- postings per week by track, over time
- top hiring companies by volume
- tool mention frequency, and its trend
- seniority distribution per track
- share of postings requiring Hebrew
- salary distribution where disclosed, plus what share disclose at all
- median days-to-close by track

### 7.3 Publication

Collector runs locally on a residential IP. A `jobradar publish` command writes
an **aggregated** snapshot to `public/snapshot.parquet`, and Streamlit Community
Cloud serves the public dashboard from that file.

**Publish aggregates and links only — never full scraped descriptions.**
Counts, distributions, tool frequencies, title trends, and a link back to the
original posting. Republishing other people's posting text at scale is the thing
that turns a portfolio project into a problem, and the aggregates are the
interesting part anyway. Enforce this in code: `publish` must strip
`description` and assert the column is absent before writing.

### 7.4 README

Reviewers spend ninety seconds. In order:

1. One sentence on what it does, one screenshot of the market view
2. Live demo link
3. Architecture diagram
4. The extraction accuracy table
5. What's interesting in the data — two or three actual findings, with numbers
6. Setup, under ten lines
7. Link to `docs/decisions/`

CI badge for lint and tests. No badge farming beyond that.

### 7.5 ADRs

Short — a paragraph of context, the decision, the alternatives rejected and why.
Write these as you go, not at the end:

- `001-duckdb-over-postgres.md`
- `002-local-scheduling-over-github-actions.md`
- `003-llm-on-new-rows-only.md`
- `004-controlled-vocabulary-extraction.md`
- `005-publish-aggregates-only.md`
- `006-dagster-over-airflow.md`

Nothing reads as engineering maturity faster than a written record of a tradeoff
considered and rejected.

### Phase 4 acceptance criteria

- [ ] Dagster daily schedule runs the full graph green
- [ ] CLI still works standalone
- [ ] All three dashboard pages populated with real data
- [ ] Public snapshot contains no description text, asserted in a test
- [ ] Live demo reachable
- [ ] README complete, six ADRs written

---

## 8. Optional stretch — MCP server

Only after phase 4 is green. A small MCP server over the marts exposing
`search_postings`, `get_market_summary`, `get_posting_detail`, so the pipeline
can be queried conversationally instead of by pasting exports.

Read-only. No write tools, no status mutation through MCP.

---

## 9. Non-goals — do not build these

Actively resist all of the following. Each adds reviewer friction and
demonstrates nothing not already shown:

- Docker Compose with Postgres, Redis, or any service container
- A FastAPI or REST layer that nothing calls
- Authentication, user accounts, multi-tenancy
- Kafka, Celery, or any message queue
- Kubernetes, Terraform, or cloud infrastructure of any kind
- An auto-apply or auto-message feature — do not build this, at all
- A recommendation model, embeddings search, or a fine-tune
- Running the scraper in GitHub Actions
- A frontend framework; Streamlit is the frontend

If the plan seems to call for one of these, it doesn't. Ask.

---

## 10. Open questions — ask before assuming

1. Repo name and whether it will be public from day one or opened later.
2. Anthropic API key availability and an acceptable monthly spend ceiling.
3. Whether Telegram alerting is wanted, and if so the bot setup.
4. Whether the machine running this is always on around 07:30, or whether the
   schedule should be a login-triggered catch-up instead.
5. Which 50 postings to label, once phase 1 has collected enough to sample from.

---

## 11. Working order, briefly

| Phase | Scope | Rough effort |
|---|---|---|
| 1 | Collector, store, dedup, minimal UI, scheduling | 2–3 sessions |
| 2 | dbt, pandera, observability | 2 sessions |
| 3 | LLM extraction, eval set | 2 sessions |
| 4 | Dagster, full dashboard, publish, README | 2–3 sessions |

Only phase 1 is required for the tool to be useful. Everything after is
portfolio work layered onto something that already runs — which is also the
right way to build it, and worth saying in the README.

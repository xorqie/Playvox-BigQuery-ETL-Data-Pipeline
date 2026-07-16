# BigQuery Schema

All tables live in a single dataset (default name: `playvox`). Playvox's
own `_id` field is used as the primary/merge key throughout, so it stays
consistent across every table.

## Load strategy by table

| Table | Strategy | Why |
|---|---|---|
| `users` | Incremental `MERGE` | Roster is mostly stable; only sync what changed |
| `teams` | Incremental `MERGE` | Same as users |
| `roles` | Incremental `MERGE` | Changes rarely; kept for completeness/audit |
| `coachings` | Incremental `MERGE` | Enriched with names looked up from `users`/`teams` |
| `evaluations` | Incremental `MERGE` | The core QA fact table; enriched the same way |
| `interactions` | Full refresh (`WRITE_TRUNCATE`) | Nested/variable-shape base contact log |
| `scorecards` | Full refresh (`WRITE_TRUNCATE`) | Low-volume rubric definitions, deeply nested |
| `calibrations` | Full refresh (`WRITE_TRUNCATE`) | Low-volume, deeply nested (multi-analyst results) |

## `users`

| Column | Description |
|---|---|
| `_id` | Playvox user ID (merge key) |
| `name` / `last_name` | Display name, used to build `agent_name` / `coach_name` / `evaluator_name` in other tables |
| `email` | User's email |
| `type` | Playvox user type (agent, team leader, etc.) |
| `roles` | Assigned role(s) |
| `status` | Active/inactive status |
| `last_login_date` | Last login timestamp |
| `created_at` / `updated_at` | Lifecycle timestamps |

## `teams`

| Column | Description |
|---|---|
| `_id` | Playvox team ID (merge key) |
| `name` | Team name |
| `team_leader_id` / `team_leader_info` | Assigned leader(s) |
| `users_info` | Nested list of team members |
| `total_users` | Member count reported by Playvox |
| `created_at` / `updated_at` | Lifecycle timestamps |

## `roles`

| Column | Description |
|---|---|
| `_id` | Playvox role ID (merge key) |
| `code` / `name` | Role identifier and display name |
| `is_default` | Whether this is the account's default role (renamed from Playvox's `default` field, which is a reserved word in most SQL dialects) |
| `read` / `write` | Permission scopes granted |
| `role_inherit` | Parent role this one inherits permissions from |

## `interactions`

Base contact log - one row per interaction (call, chat, ticket) routed
through Playvox.

| Column | Description |
|---|---|
| `_id` / `interaction_id` | Playvox interaction identifiers |
| `assignee_id` | Agent assigned to the interaction |
| `priority` | Interaction priority |
| `tags` | Tags applied to the interaction |
| `csat` | Customer satisfaction score, if collected |
| `interaction_date` | When the interaction occurred |
| `created_at` / `updated_at` | Record lifecycle timestamps |

## `scorecards`

QA rubric definitions - the question sets used to grade interactions.

| Column | Description |
|---|---|
| `_id` | Scorecard ID |
| `name` / `description` | Scorecard identity |
| `max_score` / `effectiveness_goal` / `target` | Scoring configuration |
| `sections` | Nested: sections → questions → answer options, each with its own scoring weight |
| `team_ids` | Teams this scorecard applies to |

## `calibrations`

Inter-rater agreement sessions: multiple analysts independently score the
same interaction to check scoring consistency.

| Column | Description |
|---|---|
| `_id` | Calibration session ID |
| `agent_id` / `agent_info` | Agent whose interaction is being calibrated |
| `scorecard_id` / `scorecard_info` | Scorecard used |
| `analysts` | Nested: each analyst's info + their individual scoring result |
| `status` / `due_date` / `completed_at` | Session lifecycle |

## `coachings`

1:1 coaching sessions, typically triggered by a low evaluation score.

| Column | Description |
|---|---|
| `_id` | Coaching session ID |
| `coach_id` / `coach_name` | Who ran the session (name resolved from `users`) |
| `trainee_id` / `trainee_name` | Agent being coached |
| `team_ids` / `team_name` | Associated team(s) |
| `feedback` | Coaching notes (HTML stripped) |
| `achieved` | Whether the coaching goal was met |
| `evaluation_ids` | Linked evaluations that triggered or relate to this coaching |
| `kpis_threshold_value` | KPI threshold tied to this coaching |

## `evaluations`

The core QA fact table - a completed scorecard graded against a specific
interaction. This is what most agent-performance and QA-trend dashboards
should be built on.

| Column | Description |
|---|---|
| `_id` | Evaluation ID |
| `agent_id` / `agent_name` | Agent being evaluated (name resolved from `users`) |
| `team_id` / `team_name` | Agent's team (name resolved from `teams`) |
| `monitor_id` / `evaluator_name` | QA analyst who performed the evaluation |
| `scorecard_id` | Scorecard used (joins to `scorecards._id`) |
| `score` / `score_avg` | Overall score(s) |
| `passed` | Whether the evaluation passed |
| `sections` | Nested: section-by-section answers, scores, and comments |
| `feedback` | Evaluator's written feedback (HTML stripped) |
| `coaching` | Linked coaching session(s) triggered by this evaluation |
| `interaction_id` | Joins to `interactions._id` / `interaction_id` |

## Entity relationships

```
evaluations.agent_id      --> users._id
evaluations.team_id       --> teams._id
evaluations.monitor_id    --> users._id
evaluations.scorecard_id  --> scorecards._id
evaluations.interaction_id --> interactions._id / interaction_id
coachings.trainee_id      --> users._id
coachings.coach_id        --> users._id
coachings.evaluation_ids  --> evaluations._id
calibrations.agent_id     --> users._id
calibrations.scorecard_id --> scorecards._id
```

This is a small star schema in disguise: `evaluations` and `coachings`
are the fact tables; `users`, `teams`, and `scorecards` are the
dimensions everything else joins against.

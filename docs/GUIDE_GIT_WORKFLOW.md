Awesome. I’ll draft two deliverables for you right now:

1. `docs/git_workflow.md` – human-facing doc with diagrams and captions
2. `docs/assets/git_flow.svg` – an SVG you can commit (this will just be inline SVG markup you can save as a file)

You can paste these directly into your repo.

---

## 1. `docs/git_workflow.md`

For a visual overview of the Git + Deploy process, see:
[docs/git_workflow.md](docs/git_workflow.md)


````markdown
# CTV BookedBiz Git + Deploy Flow

This doc explains how code moves from a feature branch on the Pi to `dev`, then to `main`, and finally into production. It also shows which database/environment each step is using.

---

## 1. High-level branch flow

```mermaid
flowchart LR
    subgraph Feature["Feature Branch (feat/<owner>/<slug>)"]
        A1[Local changes<br/>ruff check / ruff format] --> A2[Commit + push]
        A2 --> A3[Open PR → dev]
    end

    subgraph Dev["dev branch"]
        B1[Merge PR] --> B2[Pi dev service on :5100<br/>ENV=.env.dev<br/>DB=production_dev.db]
        B2 --> B3[Validate on Pi]
        B3 --> B4[Open PR dev → main]
    end

    subgraph Main["main branch"]
        C1[Squash merge] --> C2[Pull on prod box<br/>ENV=.env.prod<br/>DB=production.db]
        C2 --> C3[Restart prod service]
    end

    Feature --> Dev --> Main
````

**Read it like this:**

* You do work on a short-lived branch named `feat/<owner>/<slug>`.
* That work is merged into `dev` first and tested on the Pi’s dev service (port `5100`, using the dev DB).
* Once dev looks good, `dev` is merged into `main` via **Squash merge**.
* `main` is then pulled on the production instance and the prod service is restarted.

---

## 2. Day-to-day loop (timeline)

```mermaid
flowchart TD
    S1["1. Sync dev\n git switch dev && git pull --ff-only"]
    S2["2. Create feature branch\n git switch -c feat/<you>/<slug>"]
    S3["3. Code / test locally\n uvx ruff check .\n uvx ruff format ."]
    S4["4. Commit + push\n git add -A && git commit -m '...'\n git push -u origin HEAD"]
    S5["5. Open PR: base=dev, compare=feat/..."]
    S6["6. After merge, pull dev on Pi\n git switch dev && git pull --ff-only"]
    S7["7. Restart dev service\n systemctl --user restart ctv-dev.service"]
    S8["8. Health check\n curl -sf http://localhost:5100/health/ && echo DEV_OK"]
    S9["9. If OK, open PR dev → main (Squash merge)"]
    S10["10. On prod box\n git switch main && git pull --ff-only"]
    S11["11. Restart prod service"]

    S1 --> S2 --> S3 --> S4 --> S5 --> S6 --> S7 --> S8 --> S9 --> S10 --> S11

```

This is basically:
**sync dev → branch → code → PR to dev → verify on :5100 → PR dev→main → pull main for prod.**

---

## 3. Environment / DB mapping

```mermaid
flowchart LR
    subgraph DevService["Pi dev service (:5100)"]
        D1["systemd user unit\nctv-dev.service"]
        D2[".env.dev"]
        D3["DB_PATH = data/database/production_dev.db"]
    end

    subgraph ProdService["Prod service"]
        P1["systemd unit (prod)"]
        P2[".env.prod"]
        P3["DB_PATH = data/database/production.db"]
    end

    D1 --> D2 --> D3
    P1 --> P2 --> P3
```

**Key idea:**
The service that runs on port `5100` is *not allowed to touch* `production.db`.
It should always point at `production_dev.db`.

To refresh `production_dev.db` from prod safely:

```bash
systemctl --user stop ctv-dev.service
sqlite3 data/database/production.db ".backup 'data/database/production_dev.db'"
systemctl --user start ctv-dev.service
```

---

## 4. Glossary

**feat/<owner>/<slug>**
Short-lived feature branch. Example: `feat/jenna/report-cleanup`.

**dev**
Integration branch. Anything here should be runnable on the Pi using the dev DB (`production_dev.db`) through `ctv-dev.service` on port `5100`.

**main**
Production branch. Squash-merged from `dev`. This is what gets pulled to the production environment and run against the real DB (`production.db`).

**Squash merge**
GitHub turns a whole PR into one clean commit instead of a pile of tiny commits. This keeps `main` history readable.

**git pull --ff-only**
“Fast-forward only.” This refuses to create merge commits on the Pi. If it fails, you need to rebase instead of creating weird local merges.

---

## 5. Operator checklist

### After merging a feature branch into `dev`:

```bash
git switch dev
git pull --ff-only
systemctl --user restart ctv-dev.service
curl -sf http://localhost:5100/health/ && echo DEV_OK
```

### After merging `dev` → `main`:

```bash
git switch main
git pull --ff-only
# then restart prod service on the box that serves prod
```

If you follow that, `main` always represents “what prod is actually running,” and `dev` always represents “what we’re testing on :5100”.

````

---

## 2. `docs/assets/git_flow.svg`

![CTV Git Workflow Diagram](assets/git_flow.svg)


---

How to use this:

* Commit `docs/git_workflow.md` and `docs/assets/git_flow.svg`.
* Link to both from `GUIDE_DEV_WORKFLOW.md` near the top with one line like:
  `For a visual map of branches and promotion, see docs/git_workflow.md.`

That gives you:

* human steps (already in GUIDE_DEV_WORKFLOW.md),
* dev-facing visuals in `docs/git_workflow.md`,
* a clean SVG for onboarding decks / sending to someone new.

```mermaid
gitGraph
    commit id: "main init"
    branch dev
    checkout dev
    commit id: "dev setup"

    branch feat/sector-filter
    checkout feat/sector-filter
    commit id: "feat: sector filter logic"
    commit id: "fix: edge case"
    checkout dev
    merge feat/sector-filter id: "merge feat → dev"

    branch feat/dashboard-ui
    checkout feat/dashboard-ui
    commit id: "feat: dashboard UI"
    checkout dev
    merge feat/dashboard-ui id: "merge feat → dev"

    checkout main
    merge dev id: "release dev → main" 
    ```

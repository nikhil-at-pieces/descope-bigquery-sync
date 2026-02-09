# Push this repo to your GitHub

## 1. Create the repo on GitHub

1. Go to [https://github.com/new](https://github.com/new) (logged in as **nikhil-at-pieces**).
2. Repository name: `descope-bigquery-sync` (or any name you like).
3. Choose **Public**, leave "Add a README" **unchecked**.
4. Create repository.

## 2. Push from this folder

From the **descope-bigquery-sync** folder (this folder), run:

```bash
cd /path/to/Beam\ Test/descope-bigquery-sync

git init
git add .
git commit -m "Descope to BigQuery sync Cloud Function and deploy scripts"
git branch -M main
git remote add origin https://github.com/nikhil-at-pieces/descope-bigquery-sync.git
git push -u origin main
```

Use your actual repo URL if you chose a different name (e.g. `https://github.com/nikhil-at-pieces/YOUR-REPO-NAME.git`).

## 3. Optional: CI/CD

To deploy from GitHub Actions you need **Workload Identity Federation** (or a service account key in secrets). See the main Beam Test repo’s `.github/SETUP.md` for WIF. Otherwise deploy manually with `./deploy_secure.sh` from your machine.

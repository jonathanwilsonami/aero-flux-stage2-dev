# AeroFlux — Stage 2 Development Prototype

![AeroFlux flight map](images/aero-flux/flight-map-app_v2.png)

## About AeroFlux Stage 2

AeroFlux Stage 2 is a development prototype for a scalable aviation-data platform focused on real-time flight-delay intelligence. The project expands on the historical machine-learning work completed during Stage 1 by introducing live data ingestion, data fusion, streaming analytics, and an extensible architecture for machine learning and agentic AI.

This repository contains the source code, documentation, project proposal, architecture, and development environment for the Stage 2 prototype.

## Project Resources

- [AeroFlux Stage 2 Project Site](https://jonathanwilsonami.github.io/aero-flux-stage2-dev/)
- [Project Proposal](https://jonathanwilsonami.github.io/aero-flux-stage2-dev/)

## Contributing and Repository Access

### Requesting Write Access

Contact the project owner to request write access to the repository. Contributors without direct write access may fork the repository and submit a pull request with their proposed changes.

You may also need to configure SSH keys for secure GitHub access. See the [GitHub SSH documentation](https://docs.github.com/en/authentication/connecting-to-github-with-ssh) for setup instructions.

### Clone the Repository

Use the green **Code** button on the GitHub repository page to copy the appropriate HTTPS or SSH clone URL.

```bash
# Navigate to your desired project directory
git clone <REPO_URL>

# Enter the project root
cd <REPO_FOLDER>
````

Additional setup and development instructions will be added as the Stage 2 environment evolves.

### Setups
The following setup assumes you already have your IDE of choice installed (RStudio, JupyterLab or VSCode) and that you are already in your working directory of the project.

If you need a different version of Python we can update the dependencies in the environment.yml. file. See the section on working with conda.

### Environment Setup (Conda + Python)

This project uses **Conda** to manage a unified environment for **Python** dependencies. Follow the steps below to get started.

---

#### 1. Install Conda

If you do not already have Conda installed, install the following:

- Miniconda: https://docs.conda.io/en/latest/miniconda.html  

---

#### 2. Create the Environment

Create the environment using the provided `environment.yml` file:

```bash
conda env create -f environment.yml
conda activate aeroflux2
```

### Installing New Packages

If you need to install new packages, update the environment in a way that keeps it reproducible for the team.

#### Preferred Method (Recommended)

Install packages using Conda:

```bash
# Python packages
conda install -c conda-forge <package_name>

# R packages (via conda)
conda install -c conda-forge r-<package_name>
```

Examples:

```bash
conda install -c conda-forge polars
```

#### Using pip (Python only)

Only use pip if the package is not available in Conda:

```bash
pip install <package_name>
```

### Updating the Environment File (IMPORTANT)

After installing new packages, update the shared `environment.yml` file:

```bash
conda env export --from-history --no-builds | grep -v "^prefix:" > environment.yml
```

---

Summary notes:
- Prefer conda install when possible for compatibility
- Use pip only when a package is not available via Conda
- Keep environment.yml updated for team reproducibility

### Installing and running Quarto

Quarto is used to build our project site mentioned above. You will need Quarto if you want to make edits to any documents on the site pages.

To install Qaurto see [Quarto installation guide](https://quarto.org/docs/get-started/)

The following are useful Quarto commands:
```bash
# To render entire site - Note: Need to do this anytime you want your changes to be reflected on the site.
quarto render
# To see the site in your local browser. Make sure you do this to check for any issues.
quarto preview

# To render and view a single notebook
quarto preview <notebook>.qmd
```

## Project Overview

### Website Structure and Key Pages (Quarto Overview)

The main site is built using **Quarto**, which converts `.qmd` (and `.ipynb`) files into a static website. The overall structure and navigation are defined in the `_quarto.yml` file at the root of the project. This file controls the **navbar (top menu)**, theme, and where the rendered site is output (`docs/` folder for GitHub Pages).

- **Home Page** → `index.qmd`  
  Main landing page of the site

- **About Page** → `about.qmd`  
  Team bios and project context

- **Project Proposal** → `aeroflux_stage2_proposal.qmd`  
  Project Proposal

More pages will be added as the project progresses.

#### Other Useful Quarto Docs

- **Images** → `images/`  
  Where the site grabs images.

- **docs** → `docs/`  
  When the site is built or rendered (quarto render) it will place all the html, css, js etc. code into this folder. This folder is basically the site. It's what the git workflow will pick up (part of the CI/CD) and what github pages will deploy on github. The GitHub Action (Workflow) is responsible for **publishing the rendered Quarto site to GitHub Pages**. It does **not build the site**—it simply takes the already-rendered files in the `docs/` folder and pushes them to the `gh-pages` branch, which GitHub uses to host the website.

### How Quarto Works (High-Level)

- Each `.qmd` or `.ipynb` file = **one page on the site**
- Quarto renders everything into the `docs/` folder (this is what GitHub Pages serves)
- The `_quarto.yml` file defines:
  - Navigation (navbar + sidebar)
  - Site layout and structure
  - Rendering behavior

---

## Git Workflow for Maintaining and Contributing to the Quarto Site

To keep the Quarto site stable and organized, all work should be done through feature branches rather than directly on `main`.

### Recommended Step-by-Step Workflow

Note that you can also use the IDE extensions to do all of the following.

```bash
# BEFORE making changes
# 1. Move to main and get the latest updates
git checkout main # This is the default branch and you may already be on it
git pull origin main # Get latest updates

# 2. Create a new branch for your work
git checkout -b your-branch-name
# If you have already made changes you can move them to this new branch.

# 3. Make changes to the Quarto site files
#    Example: .qmd files, _quarto.yml, scripts, images, etc.

# 4. Preview locally to verify the site builds
quarto render # Builds the site. You only need to run this once before pushing or opening the PR to confirm a clean full build. Otherwise your IDE will usually build your site.
quarto preview # view changes locally. Make sure everything works before pushing!

# 5. Stage and commit your changes
# Add anything you do not want in git to .gitignore before you run the commands below.

# IMPORTANT: Before committing, sync your branch with latest changes from main
git fetch origin
git merge origin/main
or
git pull
# Resolve any conflicts if they appear before continuing

git status # Shows you what things are tracked or untracked. Can use this to know what you need to track or commit.

git add path/to/file1 path/to/file2
# or you can add everything like this. Caution: Make sure you know what you are pushing if you use git add .
git add .

git commit -m "update descriptive message here"
# You can also just run git commit and it will enter you into an editor to write the comment.
# If you prefer using an IDE you can do the same thing using buttons.

# 6. Push your branch to GitHub
git push -u origin your-branch-name

# 7. Open a Pull Request into main on GitHub
#    Review changes, discuss if needed, and merge after approval

# 8. After merge, update local main
git checkout main
git pull origin main

# 9. Delete the old branch locally only once the change has been made
git branch -d feature/your-branch-name
```

#### Best Practices
- Always preview the Quarto site locally before committing
- Do not commit large raw datasets, cached files, or environment-specific files
- If multiple people are editing, pull from main often to reduce merge conflicts.

This is a general workflow. You may have to do some additional things if you get stuck.

## Publish To Github Pages

I added a github workflow ci-cd to automatically push to Github pages. So when you add your changes and push it should automatically push the quarto site too. Note: This will only work if you are working directly on main. If you are working on your own branch your work will show up once your branch has been merged into main. Make sure you run quarto render to render before pushing your changes.  

If you need to manually push to Github Pages use the following command:

```bash
quarto publish gh-pages
```

This will push the quarto site to Github.
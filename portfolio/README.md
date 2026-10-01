# Portfolio

Static, no-build portfolio. Black/white theme with a light/dark toggle.

## Files
- `index.html` — content (edit text here)
- `style.css` — design tokens at the top (`:root`) control all colors/fonts
- `script.js` — theme toggle, mobile menu, scroll reveals, active nav
- `resume.pdf` — add your own (linked from the hero button)

## Deploy to GitHub Pages
1. Create a repo named `<your-username>.github.io` (gives you the root URL) or any name (gives `/<repo>/`).
2. Push these files to the `main` branch root.
3. Repo → **Settings → Pages** → Source: *Deploy from a branch* → `main` / `/ (root)` → Save.
4. Live in ~1 minute at `https://<your-username>.github.io/`.

## Editing tips
- **Images:** replace a `<span class="mono">IMAGE …</span>` inside `.card-media` with `<img src="img/project.jpg" alt="…">`. Images render grayscale and go full color on hover.
- **Add a project:** duplicate an `<article class="card">` block. Add `card-wide` to make it span full width.
- **Placeholders to replace:** `20XX` dates, referee association, email, social handles, project titles/links.
- **Custom domain:** add a `CNAME` file containing your domain, then set it in Settings → Pages.
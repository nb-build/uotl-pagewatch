# Up On The Ladder page watch

A small, honest page watcher for Tools of Record.

**What it does.** Every night at about 11:35pm Pacific, GitHub runs `tools/snapshot.py` on a fresh machine with a normal internet connection. It does four things:

1. Fetches every public vendor page listed in `urls.txt`, about 770 help, pricing, release-notes and status pages.
2. Turns each page into plain text and takes a SHA-256 fingerprint of it.
3. Compares each page with the last capture and writes `latest/changed_pages.json`. Each change is labeled:
   - **new_change:** seen for the first time
   - **confirmed:** still there at least 20 hours later
   - **flip_back:** the page went back to an older version
   - **reverted_before_confirm:** the change disappeared before it could be confirmed
4. Commits the results here. The 2:13am nightly writer and the fleet merger read `latest/` before they do anything else.

**Rules it follows.**
- Public pages only. It never logs in or submits forms.
- It obeys robots.txt and crawl-delay.
- It identifies itself as `UOTL-PageWatch` and fetches at a low rate: at least 4 seconds between requests to the same site, with a random extra wait on top.
- It visits pages in a different order each night.
- It keeps no personal data.

**Why it exists.** The agents' own workspaces can't open vendor sites directly. Their fetch tool often returns cached copies, so it can't prove a page changed on a given night. This watcher can.

**Setup (one time, about 10 minutes).**
1. Create a GitHub repository named `uotl-pagewatch`. Public is fine, since it only holds public vendor pages; private also works and uses about 1,200 of the free 2,000 Action minutes a month.
2. Upload these files, keeping the folder structure.
3. In the repository, open **Actions** and enable workflows. Run "Nightly page watch" once by hand (**Run workflow**) to take the first baseline.
4. Tell Claude the repository name (owner/uotl-pagewatch), so the nightly tasks read from it.

**Adding pages.** Add one URL per line to `urls.txt`. Nightly agents may propose new lines; a person adds them.

# SharePoint Clean-Up Toolkit

Free space, speed syncs, and lighten *.docx* and *.pptx* files in your SharePoint Online sites.

---

## Why this repo?

You waste storage and bandwidth on stale file versions and oversized thumbnails.  
These six scripts help you **delete**, **compress**, and **optimise** at scale—no manual clicks, no broken links.

---

## What’s inside?

| File | What it does | Typical use |
|---|---|---|
| **DeleteOldVersionsPPT.ps1** | Removes historic versions of *.pptx* files, keeps the latest *n* | Weekly trim of busy slide libraries |
| **DeleteOldVersionsWord.ps1** | Same as above for *.docx* | Clear audit clutter in large authoring sites |
| **OldVersionsPPT_Readme** | How-to for the PPT clean-up script | Quick copy-paste into ticket replies |
| **OldVersionsWord_Readme** | How-to for the DOCX clean-up script | Hand-off to ops teams |
| **ReduceThumbnailSizeInWord.ps1** | Downloads heavy Word files, swaps SVG thumbnails for compressed PNG, re-uploads | Cut file size before project archive |
| **ReduceThumbnailinWord_Readme** | Usage guide for thumbnail reducer | Onboard new admins fast |

---

## When to use what?

* **Space crunch** – run the *DeleteOldVersions* pair first  
* **Large design docs** – follow up with *ReduceThumbnailSizeInWord.ps1* to shrink images  
* **Routine hygiene** – schedule weekly or monthly runs in **-TestMode** first, then live  

---

## Quick start

```powershell
# Example: Dry-run delete on a slide library
.\DeleteOldVersionsPPT.ps1 `
    -SiteUrl "https://contoso.sharepoint.com/sites/Marketing" `
    -LocalTempPath "C:\Temp\PPTCleanup" `
    -ClientId "<AAD-App-ID>" `
    -TenantId "<Tenant-ID>" `
    -TestMode

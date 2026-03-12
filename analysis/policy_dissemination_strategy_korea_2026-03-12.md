# Policy Dissemination Strategy — Korea
**Project:** kr-health-monitor
**Date:** 2026-03-12
**Context:** Pathways for publishing independent research and submitting findings to relevant Korean authorities, specifically 보건복지부, ahead of or during the next 고시 revision cycle.

---

## The practice exists and has precedent

Korean civil society organizations, patient advocacy groups, and independent researchers
regularly submit policy documents directly to 보건복지부 and the National Assembly's
보건복지위원회. The mechanism is called **정책 제안** or **의견서 제출**, and there are
formal intake channels for it. 보건복지부 runs a public comment system through its
website, and the National Assembly accepts written submissions from non-members during
committee deliberations on related legislation.

The more effective route historically has been through **건강보험심사평가원 (HIRA)**
itself, since it directly administers the 기준금액 review process. When the ceiling gets
reconsidered — which happens on a rolling basis through 고시 amendments — external data
submissions can formally enter the deliberation record.

---

## What makes this specific work unusually credible for this

Most patient advocacy submissions are qualitative — personal testimony, cost burden
descriptions. What this project has is different:

- A reproducible, public pipeline with a verifiable methodology (anyone can clone and re-run)
- Administrative claims data from NHIS's own public datasets, not surveys or estimates
- A 22-year longitudinal utilization rate series showing the ceiling has been frozen while the patient base grew 3.6×
- A quantified cost-containment argument (CGM program = 6% of insulin claim costs)
- 시군구-level regional equity data showing the double-jeopardy districts

The fact that the data source is NHIS's own public releases means 보건복지부 cannot
dispute the underlying numbers — they published them.

---

## The realistic path

GitHub alone reaches developers, not policymakers. The repos that actually get read by
government audiences in Korea tend to get there through one of these routes:

**1. Patient organization co-authorship.**
The Korea Diabetes Association (대한당뇨병학회) or the T1D patient group
(한국1형당뇨병환우회) carry institutional weight. A joint submission with this analysis
attached as a technical appendix lands differently than an individual submission.

**2. Medical press.**
청년의사, 메디칼업저버, 히트뉴스 cover NHIS policy closely. A data-driven story with a
reproducible GitHub repo is genuinely unusual and newsworthy in that space. Journalists
there actively look for quantified critiques of 기준금액 policy.

**3. Academic preprint or letter.**
Submitting a short data note to a Korean health policy journal (보건경제와 정책연구,
한국보건경제학회지) or even as a letter to a diabetes journal creates a citable document.
Once it has a DOI, it can be formally referenced in a 의견서.

**4. National Assembly petition.**
The 국민동의청원 system allows any citizen to submit a petition; if it reaches 50 votes
it gets referred to the relevant committee (보건복지위원회). A petition backed by a
quantified technical report with a public GitHub link is stronger than a text-only
request.

**5. Direct 고시 comment period.**
When 보건복지부 publishes a draft 고시 revision, there is a mandatory public comment
window (usually 20–40 days). The next revision to 고시 2024-226 is the clearest formal
entry point.

---

## The practical first step

The GitHub repo establishes credibility and gives any co-author, journalist, or patient
group something concrete to point to. But the repo alone does nothing. The move that
actually creates pressure is getting the findings in front of someone with standing —
the **한국1형당뇨병환우회** is the obvious first contact, since they have direct patient
testimony to pair with the quantitative case and they already engage with 보건복지부 on
기준금액 issues.

The findings document (`analysis/findings_dataset_integration_2026-03-12.md`) is already
written. It would take one afternoon to reformat it as a two-page **정책 제안서** in
Korean and attach the GitHub link as the technical appendix.

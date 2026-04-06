# Analyst SOP

## Purpose
Standard operating procedure for analysts to produce consistent, client-ready environmental location overview reports.

## Scope
Applies to all analyst work from order intake through submission to admin review.

## Daily Workflow
1. Log in and open assigned orders.
2. Validate subject location inputs.
3. Run location screening and confirm database coverage.
4. Review map output and address-level findings.
5. Generate report and perform acceptance checks.
6. Submit to admin review with notes.

## Step-by-Step Procedure

### 1) Intake And Assignment
1. Open assigned order in analyst dashboard.
2. Confirm project name, client name/company, address, and coordinates are present.
3. If location data is missing or invalid, stop and escalate to admin.

### 2) Location Validation
1. In workbench, verify subject marker is on correct parcel/location.
2. Adjust subject marker only if obvious placement error exists.
3. Keep radius at standard unless order explicitly requires a different scope.
4. If polygon is provided, ensure it reflects subject footprint reasonably.

### 3) Screening Execution
1. Refresh GIS search in workbench.
2. Confirm database layers are enabled for full screening.
3. Use filters only for analysis; avoid excluding records in final run unless justified.
4. Confirm database catalog is populated and not empty.

### 4) Findings Review
1. Review records table for obvious duplicates, malformed coordinates, or blank addresses.
2. Review address-level grouping for location context.
3. Confirm flood, receptor, and major regulatory layers are represented when available.
4. Add custom point only when source evidence supports it.
5. If adding custom point, include clear site name and source context.

### 5) Map Quality Check
1. Ensure subject property marker is visible.
2. Ensure nearby points are visible and not clipped.
3. Ensure map exhibits reflect current radius and location.
4. If map tiles fail, refresh and retry before escalation.

### 6) Report Generation
1. Generate report from workbench.
2. Verify report completes without API errors.
3. Download and open generated PDF.
4. Confirm all major sections are present and populated.
5. Confirm report reads as location overview and database coverage summary.

### 7) Acceptance Criteria (Must Pass)
1. Order and client identifiers are correct.
2. Subject address and coordinates are correct.
3. Maps render correctly in report.
4. Database coverage section is present and readable.
5. No placeholder artifacts like "N/A" in core title fields.
6. Report downloads successfully.

### 8) Submission To Admin Review
1. Move order stage to admin review.
2. Add analyst note with:
   - Any manual adjustments made.
   - Any data limitations observed.
   - Any recommended follow-up checks.

## Escalation Rules
Escalate to admin immediately when:
1. Missing or conflicting subject address/coordinates.
2. Report generation fails after 2 retries.
3. Map exhibits fail to render after refresh/retry.
4. Catalog unexpectedly empty for multiple known active locations.
5. Payload or API errors persist (413, 500, auth failures).

## Retry Policy
1. First failure: retry once with same inputs.
2. Second failure: reduce payload complexity (fewer optional extras) and retry.
3. Third failure: escalate with error details and order ID.

## Evidence And Notes Standard
Each submitted order should include analyst note containing:
1. Order ID and timestamp.
2. Radius used.
3. Total records found.
4. Any custom points added and why.
5. Known caveats or data limitations.

## QA Spot-Check Checklist
1. Address and coordinates match order.
2. Database names are legible and non-empty.
3. Distances use expected units and look reasonable.
4. Report branding and logo appear correctly.
5. Download link works from order workflow.

## Handover Message Template
Use this in admin note:

"Analyst review complete. Location validated at [address]. Radius used: [value]. Records found: [count]. Report generated and downloaded successfully. Notable caveats: [none/list]. Ready for admin QA."

## Time Targets
1. Standard order: 20 to 35 minutes.
2. High-density urban order: 35 to 55 minutes.
3. Rework pass: 10 to 20 minutes.

## Change Control
1. If workflow or section logic changes, update this SOP the same day.
2. Keep one source of truth in this file.

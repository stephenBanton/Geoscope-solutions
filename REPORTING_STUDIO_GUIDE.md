# 📋 Custom Reporting Studio

A professional visual interface for designing environmental reports with real-time customization.

## Features

### 🎯 Report Sections (15 Total)
Toggle individual sections or use category "Enable All / Disable All" buttons:

**Structure:**
- 📄 Cover Page
- 📑 Table of Contents

**Core Analysis:**
- 🏢 Property Information
- 📊 Executive Summary

**Maps & Imagery:**
- 🗺️ Location Maps
- 🧭 Topographic Maps

**Environmental Data:**
- 🔍 Environmental Records

**Detailed Analysis:**
- 📍 Proximity Analysis
- ⛏️ Geological Analysis
- 💧 Wetlands & Flood Analysis
- 🏫 Sensitive Receptors

**Historical Context:**
- 📰 Historical Land Use

**Closing & References:**
- ✅ Recommendations
- 📖 Database Descriptions
- ⚠️ Disclaimer

### 🎨 Branding Presets
Choose from 5 pre-designed color schemes:
- **GeoScope Standard** — Teal/Blue (default)
- **Professional Blue** — Navy/Cyan
- **Eco Green** — Forest/Fresh
- **Corporate Grey** — Neutral/Formal
- **Clean Minimal** — White/Black

### 📊 Data Density Levels
Customize the depth of analysis in your report:

| Level | Records | Use Case |
|---|---|---|
| 📄 **Concise** | ~10 | Quick executive summary |
| 📋 **Standard** | ~25 | Most client reports ⭐ |
| 📚 **Comprehensive** | ~50 | Detailed Phase I/II |
| 🔬 **Expert** | All | Maximum coverage |

### 💾 Save & Load Templates
- **Save Template** — Create custom templates you use frequently
- **Load Template** — Apply saved configurations instantly
- **Delete Template** — Remove templates no longer needed
- Stored locally via browser storage

### 📄 Real-Time Preview
Live report card showing:
- Selected branding colors
- All enabled sections grouped by category
- Estimated page count
- Data density indicator

## How to Access

### From Dashboard
1. **Analyst** → Click "📋 Report Studio" button in hero
2. **Admin** → Same button available in workbench
3. **GIS** → Not yet included (report gen is analyst role)

### URL
Direct access: `/reporting-studio`

## Workflow

1. **Select Sections** — Check/uncheck report components
   - Use category toggles for bulk changes
   - Search to filter sections
   
2. **Choose Branding** — Select color scheme matching client needs
   
3. **Set Density** — Pick data depth (Standard recommended for most)
   
4. **Preview** — See estimated page count and structure
   
5. **Save Template** — Store config for future reuse
   
6. **Generate** → Use these settings when creating report (next integration step)

## Technical Details

- **State Management:** React hooks (sections, branding, density, templates)
- **Storage:** Browser localStorage (survives page refresh)
- **Responsive:** Sidebar collapses on smaller screens
- **No API calls** yet (future: POST template configurations to backend)

## Future Enhancements

- [ ] Export template configuration to JSON file
- [ ] Import template from file
- [ ] Backend persistence of templates (shared team library)
- [ ] Section reordering (drag-and-drop)
- [ ] Custom header/footer text
- [ ] Logo upload & placement settings
- [ ] Default data density per template
- [ ] Integration: Open report studio from order context
- [ ] Apply template directly to order report generation

## Tips

✅ **Best Practices:**
- Save your most-used configurations as templates
- Use **Standard** density for balanced, professional reports
- Create templates per client type (e.g., "Quick Review", "Full ESA")
- Branding can be changed per report without losing section selections

❌ **Avoid:**
- Disabling all sections in a category (enable at least one from each used category)
- Switching layouts mid-review (save first!)

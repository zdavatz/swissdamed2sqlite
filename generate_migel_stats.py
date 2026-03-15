#!/usr/bin/env python3
"""Generate MiGeL matching stats PNG for swissdamed2sqlite, reading from the latest migel DB."""

import glob
import sqlite3
import sys
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec

# --- Find latest migel database ---
db_files = sorted(glob.glob('db/swissdamed_migel_*.db'))
if not db_files:
    print("No swissdamed_migel_*.db found in db/", file=sys.stderr)
    sys.exit(1)
DB_PATH = db_files[-1]
print(f"Reading from {DB_PATH}")

# --- Find latest full swissdamed database for total product count ---
full_db_files = sorted(glob.glob('db/swissdamed_[0-9]*.db'))
total_products = 0
if full_db_files:
    try:
        full_conn = sqlite3.connect(full_db_files[-1])
        total_products = full_conn.execute("SELECT COUNT(*) FROM swissdamed").fetchone()[0]
        full_conn.close()
        print(f"Total products from {full_db_files[-1]}: {total_products}")
    except Exception:
        pass

# --- Read stats from migel database ---
try:
    conn = sqlite3.connect(DB_PATH)
except Exception as e:
    print(f"Error opening {DB_PATH}: {e}", file=sys.stderr)
    sys.exit(1)

total_matched = conn.execute("SELECT COUNT(*) FROM swissdamed").fetchone()[0]
num_migel_codes = conn.execute("SELECT COUNT(DISTINCT migel_code) FROM swissdamed").fetchone()[0]
num_companies = conn.execute("SELECT COUNT(DISTINCT companyName) FROM swissdamed").fetchone()[0]

pct_mapped = f'{total_matched / total_products * 100:.1f}%' if total_products > 0 else 'N/A'

# Company breakdown
company_rows = conn.execute(
    "SELECT companyName, COUNT(*) as cnt FROM swissdamed "
    "GROUP BY companyName ORDER BY cnt DESC"
).fetchall()

# Top MiGeL codes
migel_rows = conn.execute(
    "SELECT migel_bezeichnung, COUNT(*) as cnt FROM swissdamed "
    "GROUP BY migel_code ORDER BY cnt DESC LIMIT 8"
).fetchall()

conn.close()

# --- Color scheme: dark background ---
bg_color = '#1a1a2e'
card_color = '#16213e'
title_color = '#e0e0e0'
text_color = '#b0b0b0'
accent = '#4caf50'
accent_light = '#81c784'
bar_color = '#43a047'
edge_color = '#1a1a2e'
company_colors = [
    '#2e7d32', '#43a047', '#66bb6a', '#81c784', '#a5d6a7',
    '#c8e6c9', '#e8f5e9', '#fff59d', '#ffcc80', '#ef9a9a',
    '#ce93d8',
]

# --- Build chart ---
fig = plt.figure(figsize=(16, 12), facecolor=bg_color)
gs = GridSpec(2, 2, figure=fig, hspace=0.55, wspace=0.3,
             left=0.08, right=0.95, top=0.91, bottom=0.06)

now = datetime.now()
timestamp_display = now.strftime('%Hh%M-%d.%m.%Y')
timestamp_file = now.strftime('%Hh%M.%d.%m.%Y')

fig.suptitle('swissdamed MiGeL Matching Results',
             fontsize=25, fontweight='bold', color=accent, y=0.96)
fig.text(0.95, 0.02, timestamp_display, ha='right', fontsize=13, color=text_color)

# --- Top left: Key metrics ---
ax1 = fig.add_subplot(gs[0, 0])
ax1.set_facecolor(bg_color)
ax1.axis('off')

metrics = [
    (f'{total_products:,}', 'Total swissdamed products'),
    (f'{total_matched:,}', f'MiGeL matched ({pct_mapped})'),
    (f'{num_migel_codes}', 'Distinct MiGeL codes'),
    (f'{num_companies}', 'Companies with matches'),
    (f'786', 'Total MiGeL items in XLSX'),
]

for i, (value, label) in enumerate(metrics):
    y = 0.88 - i * 0.19
    ax1.text(0.05, y, value, fontsize=25, fontweight='bold',
             color=accent, transform=ax1.transAxes, va='center')
    ax1.text(0.40, y, label, fontsize=15, color=text_color,
             transform=ax1.transAxes, va='center')

ax1.text(0.05, 1.05, 'Key Metrics', fontsize=17, fontweight='bold',
         color=title_color, transform=ax1.transAxes)

# --- Top right: Company donut chart ---
ax2 = fig.add_subplot(gs[0, 1])
ax2.set_facecolor(bg_color)

company_names = [r[0] for r in company_rows]
company_values = [r[1] for r in company_rows]
colors = company_colors[:len(company_names)]

def short_name(name):
    if len(name) > 25:
        return name[:22] + '...'
    return name

wedges, texts, autotexts = ax2.pie(
    company_values, labels=None, autopct='%1.0f%%',
    colors=colors, startangle=90,
    pctdistance=0.78, wedgeprops=dict(width=0.45, edgecolor=bg_color, linewidth=2)
)
for t in autotexts:
    t.set_fontsize(12)
    t.set_fontweight('bold')
    t.set_color('#1a1a2e')
# Hide small percentages
for t, v in zip(autotexts, company_values):
    if v / total_matched < 0.03:
        t.set_text('')

ax2.text(0, 0, f'{total_matched}\nmatches', ha='center', va='center',
         fontsize=17, fontweight='bold', color=accent)

ax2.set_title('Matches by Company', fontsize=17, fontweight='bold',
              color=title_color, pad=12)

legend = ax2.legend(
    [mpatches.Patch(facecolor=c, edgecolor=bg_color) for c in colors],
    [f'{short_name(n)}  ({v:,})' for n, v in zip(company_names, company_values)],
    loc='lower center', bbox_to_anchor=(0.5, -0.30),
    ncol=2, fontsize=12, frameon=False,
)
for t in legend.get_texts():
    t.set_color(text_color)

# --- Bottom: Top MiGeL codes bar chart ---
ax3 = fig.add_subplot(gs[1, :])
ax3.set_facecolor(bg_color)

migel_labels = [r[0] for r in migel_rows]
migel_values = [r[1] for r in migel_rows]

def short_migel(name):
    if len(name) > 40:
        return name[:37] + '...'
    return name

bar_positions = [i * 1.3 for i in range(len(migel_labels))]
bars = ax3.barh(bar_positions, migel_values[::-1],
                color=bar_color, edgecolor=bg_color, height=0.7, alpha=0.9)

max_val = max(migel_values) if migel_values else 1
for i, (bar, val) in enumerate(zip(bars, migel_values[::-1])):
    label = short_migel(migel_labels[::-1][i])
    y_center = bar.get_y() + bar.get_height() / 2
    y_top = bar.get_y() + bar.get_height() + 0.05
    ax3.text(0, y_top, label, va='bottom', ha='left',
             fontsize=12, fontweight='bold', color=text_color)
    if bar.get_width() > max_val * 0.08:
        ax3.text(bar.get_width() * 0.5, y_center, f'{val:,}',
                 va='center', ha='center',
                 fontsize=14, fontweight='bold', color='#1a1a2e')
    else:
        ax3.text(bar.get_width() + max_val * 0.02, y_center, f'{val:,}',
                 va='center', fontsize=14, fontweight='bold', color=text_color)

ax3.set_xlim(0, max_val * 1.15)
ax3.set_title('Top MiGeL Categories', fontsize=17, fontweight='bold',
              color=title_color, pad=12)
ax3.set_yticks([])
ax3.spines['top'].set_visible(False)
ax3.spines['right'].set_visible(False)
ax3.spines['bottom'].set_color('#333')
ax3.spines['left'].set_visible(False)
ax3.xaxis.set_visible(False)

output_ts = f'swissdamed_migel_stats_{timestamp_file}.png'
output_stable = 'swissdamed_migel_stats.png'
plt.savefig(output_ts, dpi=150, facecolor=fig.get_facecolor())
plt.savefig(output_stable, dpi=150, facecolor=fig.get_facecolor())
plt.close()
print(f'Saved {output_ts}')
print(f'Saved {output_stable}')

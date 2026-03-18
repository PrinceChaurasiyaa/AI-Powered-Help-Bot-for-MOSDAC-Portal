"""
Extracts clean text from MOSDAC pages (Drupal 7 CMS).

MOSDAC-specific adaptations (from real HTML analysis)
  • Targets div.region-content / div#content (Drupal content region)
  • Strips Drupal chrome: nav menus, superfish, footer, sticky bar,
    lang selector, contrast switcher, quicktabs widget chrome
  • Strips div#site-map list navigation items (they're just link lists)
  • Extracts Drupal Views row content (div.views-row)
  • Preserves figcaption text (service thumbnails have useful descriptions)
  • Extracts Announcements block (title + date + PDF link)
  • Page classification uses MOSDAC-specific domain vocabulary
"""


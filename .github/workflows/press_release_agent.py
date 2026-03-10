#!/usr/bin/env python3
"""
Press Release Agent for 20 Leading Asset Managers
- Scrapes press releases daily
- Analyzes with Claude API
- Sends 8am digest + real-time alerts via email
- Tracks sent releases to avoid duplicates
"""

import os
import json
import smtplib
import hashlib
import sqlite3
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import anthropic

# ============================================================================
# CONFIG
# ============================================================================

ASSET_MANAGERS = [
    "Alliance Bernstein",
    "Allianz",
    "Baillie Gifford",
    "BNY Mellon",
    "Capital Group",
    "Columbia Threadneedle",
    "Deutsche",
    "FERI",
    "Fidelity",
    "First Trust",
    "Franklin Templeton",
    "Goldman Sachs",
    "Invesco",
    "JP Morgan",
    "Jupiter",
    "M&G",
    "Morgan Stanley",
    "NINETY ONE",
    "PIMCO",
    "Principal"
]

# Press release URLs
PRESS_RELEASE_URLS = {
    "Alliance Bernstein": "https://www.alliancebernstein.com/corporate/en/investor-relations/news-center.html",
    "Allianz": "https://www.allianzlife.com/about/newsroom",
    "Baillie Gifford": "https://www.bailliegifford.com/en/uk/individual-investors/media-hub/",
    "BNY Mellon": "https://www.bnymellon.com/us/en/newsroom.html",
    "Capital Group": "https://www.capitalgroup.com/advisor/insights/articles.html",
    "Columbia Threadneedle": "https://www.columbiathreadneedle.com/en/media-centre/",
    "Deutsche": "https://www.dws.com/media-center/press-releases/",
    "FERI": "https://www.feri.de/en/latest-press-releases",
    "Fidelity": "https://www.fidelity.com/about-fidelity/newsroom/overview",
    "First Trust": "https://www.ftportfolios.com/Retail/NewsRoom/NewsRoom.aspx",
    "Franklin Templeton": "https://www.franklintempleton.com/press-releases",
    "Goldman Sachs": "https://www.gsam.com/content/gsam/global/en/market-insights/gsam-insights.html",
    "Invesco": "https://www.invesco.com/corporate/news/press-releases",
    "JP Morgan": "https://www.jpmorgan.com/news",
    "Jupiter": "https://www.jupiteram.com/global/en/corporate/press-releases/",
    "M&G": "https://www.mandg.com/news-and-media/press-releases",
    "Morgan Stanley": "https://www.morganstanley.com/about-us/press-releases",
    "NINETY ONE": "https://www.ninetyone.com/en/newsroom",
    "PIMCO": "https://www.pimco.com/en-us/about-us/press-release",
    "Principal": "https://investors.principal.com/news-releases",
}

DB_PATH = Path("press_releases.db")
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_FROM = os.getenv("EMAIL_FROM")  # Set as env var
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")  # Gmail app password
EMAIL_TO = os.getenv("EMAIL_TO", "your-email@citywire.co.uk")

# ============================================================================
# DATABASE
# ============================================================================

def init_db():
    """Initialize SQLite database to track sent releases."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS releases (
            id TEXT PRIMARY KEY,
            firm TEXT,
            title TEXT,
            url TEXT,
            published_date TEXT,
            scraped_date TEXT,
            claude_analysis TEXT,
            sent_as_alert BOOLEAN DEFAULT 0,
            sent_in_digest BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn

def release_hash(firm: str, title: str) -> str:
    """Generate unique ID for a release."""
    return hashlib.md5(f"{firm}|{title}".encode()).hexdigest()

def is_duplicate(conn, release_id: str) -> bool:
    """Check if release already processed."""
    c = conn.cursor()
    c.execute("SELECT id FROM releases WHERE id = ?", (release_id,))
    return c.fetchone() is not None

def save_release(conn, firm: str, title: str, url: str, analysis: Optional[str] = None):
    """Save release to database."""
    release_id = release_hash(firm, title)
    if is_duplicate(conn, release_id):
        return None
    
    c = conn.cursor()
    c.execute("""
        INSERT INTO releases 
        (id, firm, title, url, published_date, scraped_date, claude_analysis)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        release_id,
        firm,
        title,
        url,
        datetime.now().isoformat(),
        datetime.now().isoformat(),
        analysis
    ))
    conn.commit()
    return release_id

# ============================================================================
# SCRAPING
# ============================================================================

def scrape_press_releases(firm: str, url: str) -> list[dict]:
    """
    Scrape press releases from a firm's news page.
    Returns list of dicts with 'title' and 'url' keys.
    Tries multiple selector strategies to handle different site structures.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        releases = []
        seen_titles = set()
        
        # Strategy 1: Look for article/press-release containers
        selectors = [
            ('article', None),
            ('div', lambda x: x and any(kw in x.lower() for kw in ['article', 'press', 'news', 'release'])),
            ('li', lambda x: x and any(kw in x.lower() for kw in ['press', 'news'])),
        ]
        
        for tag, class_filter in selectors:
            if len(releases) >= 15:
                break
            
            for container in soup.find_all(tag, class_=class_filter, limit=20):
                if len(releases) >= 15:
                    break
                
                # Try to find title and link
                title_elem = container.find(['h2', 'h3', 'h4', 'a'])
                if not title_elem:
                    continue
                
                title = title_elem.get_text(strip=True)
                if not title or len(title) < 10 or title in seen_titles:
                    continue
                
                # Look for link
                link_elem = container.find('a', href=True)
                link = link_elem.get('href', '') if link_elem else ''
                
                if not link:
                    continue
                
                # Normalize URL
                if not link.startswith('http'):
                    # Handle relative URLs
                    link = urljoin(url, link)
                
                # Filter: only keep if looks like a press release
                title_lower = title.lower()
                is_press_release = any(kw in title_lower for kw in [
                    'fund', 'launch', 'appoint', 'merger', 'partnership', 'esg', 
                    'regulatory', 'aum', 'acquisition', 'promotes', 'announces',
                    'invest', 'award', 'compliance', 'expansion', 'closes',
                    'secures', 'names', 'welcomes', 'teams up'
                ])
                
                if is_press_release:
                    releases.append({
                        'title': title[:250],
                        'url': link,
                        'firm': firm
                    })
                    seen_titles.add(title)
        
        return releases[:15]  # Cap at 15 per firm
    except Exception as e:
        print(f"  ✗ Error scraping {firm}: {str(e)[:100]}")
        return []

# ============================================================================
# CLAUDE ANALYSIS
# ============================================================================

def analyze_release(client: anthropic.Anthropic, firm: str, title: str) -> dict:
    """
    Analyze press release with Claude.
    Extract: entity types, sentiment, competitive relevance.
    Focus on competitive intelligence value.
    """
    prompt = f"""You are a competitive intelligence analyst for a financial media company.

Analyze this asset manager press release:
Firm: {firm}
Title: {title}

Return ONLY valid JSON (no markdown, no preamble, no extra text):
{{
    "entity_type": "one of: fund_launch, leadership_change, m_and_a, partnership, regulatory, esg_initiative, aum_milestone, technology, market_commentary",
    "sentiment": "positive, neutral, or negative",
    "relevance_score": 1-10 (how important for competitive intelligence),
    "key_insight": "one sentence summary of what matters",
    "competitive_angle": "how this positions {firm} vs peers, or null if not applicable"
}}"""
    
    try:
        msg = client.messages.create(
            model="claude-opus-4-20250805",
            max_tokens=350,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = msg.content[0].text.strip()
        
        # Clean up markdown if present
        if "```" in response_text:
            response_text = response_text.split("```")[1]
            if response_text.startswith("json"):
                response_text = response_text[4:]
        
        analysis = json.loads(response_text)
        return analysis
    except json.JSONDecodeError as e:
        print(f"  ⚠️  JSON parse error: {e}")
        return {
            "entity_type": "unknown",
            "sentiment": "neutral",
            "relevance_score": 5,
            "key_insight": title[:100],
            "competitive_angle": None
        }
    except Exception as e:
        print(f"  ⚠️  Claude API error: {str(e)[:80]}")
        return {
            "entity_type": "unknown",
            "sentiment": "neutral",
            "relevance_score": 5,
            "key_insight": title[:100],
            "competitive_angle": None
        }

# ============================================================================
# EMAIL
# ============================================================================

def send_email(subject: str, body: str, is_html: bool = True):
    """Send email via Gmail SMTP."""
    print(f"DEBUG: EMAIL_FROM={EMAIL_FROM}, EMAIL_PASSWORD={'***' if EMAIL_PASSWORD else 'NOT SET'}")
    if not EMAIL_FROM or not EMAIL_PASSWORD:
        print("⚠️  Email not configured. Set EMAIL_FROM and EMAIL_PASSWORD env vars.")
        return False
    
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO
        
        mime_type = "html" if is_html else "plain"
        msg.attach(MIMEText(body, mime_type))
        
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_FROM, EMAIL_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        server.quit()
        
        print(f"✓ Email sent: {subject}")
        return True
    except Exception as e:
        print(f"✗ Email failed: {e}")
        return False

def format_digest_email(releases: list[dict]) -> str:
    """Format daily digest email."""
    html = f"""
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; color: #333; }}
        .header {{ background: #1a3a52; color: white; padding: 20px; }}
        .release {{ margin: 15px 0; padding: 15px; border-left: 4px solid #0066cc; background: #f9f9f9; }}
        .title {{ font-weight: bold; font-size: 16px; }}
        .firm {{ color: #666; font-size: 12px; }}
        .sentiment {{ display: inline-block; padding: 3px 8px; border-radius: 3px; font-size: 11px; margin-top: 5px; }}
        .positive {{ background: #d4edda; color: #155724; }}
        .neutral {{ background: #e2e3e5; color: #383d41; }}
        .negative {{ background: #f8d7da; color: #721c24; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📰 Asset Manager Press Release Digest</h1>
        <p>{datetime.now().strftime('%A, %B %d, %Y at %I:%M %p')}</p>
    </div>
"""
    
    for rel in releases:
        analysis = json.loads(rel['claude_analysis']) if isinstance(rel['claude_analysis'], str) else rel['claude_analysis']
        sentiment_class = analysis['sentiment'].lower()
        
        html += f"""
    <div class="release">
        <div class="firm">{rel['firm']}</div>
        <div class="title">{rel['title']}</div>
        <div style="margin-top: 8px; color: #666; font-size: 14px;">{analysis['key_insight']}</div>
        <div class="sentiment {sentiment_class}">
            {analysis['sentiment'].capitalize()} | Type: {analysis['entity_type']} | Relevance: {analysis['relevance_score']}/10
        </div>
    </div>
"""
    
    html += """
    <div style="margin-top: 30px; padding-top: 15px; border-top: 1px solid #ddd; font-size: 12px; color: #999;">
        <p>Real-time alerts sent as releases detected. Daily digest sent at 8:00 AM UTC.</p>
    </div>
</body>
</html>
"""
    return html

def format_alert_email(release: dict) -> str:
    """Format real-time alert email."""
    analysis = json.loads(release['claude_analysis']) if isinstance(release['claude_analysis'], str) else release['claude_analysis']
    
    html = f"""
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        .alert {{ background: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; }}
    </style>
</head>
<body>
    <div class="alert">
        <h2>🚨 Press Release Alert</h2>
        <p><strong>{release['firm']}</strong></p>
        <h3>{release['title']}</h3>
        <p><strong>Insight:</strong> {analysis['key_insight']}</p>
        <p><strong>Type:</strong> {analysis['entity_type']} | <strong>Sentiment:</strong> {analysis['sentiment']} | <strong>Relevance:</strong> {analysis['relevance_score']}/10</p>
        {f'<p><strong>Competitive Context:</strong> {analysis["competitive_angle"]}</p>' if analysis.get('competitive_angle') else ''}
    </div>
</body>
</html>
"""
    return html

# ============================================================================
# MAIN AGENT FUNCTIONS
# ============================================================================

def run_scrape_and_analyze():
    """
    Main function: scrape all firms, analyze with Claude, store, send real-time alerts.
    Call this every 30-60 minutes.
    """
    print(f"\n{'='*70}")
    print(f"PRESS RELEASE AGENT - RUN START: {datetime.now().isoformat()}")
    print(f"{'='*70}")
    
    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    conn = init_db()
    
    new_releases = []
    
    for firm in ASSET_MANAGERS:
        url = PRESS_RELEASE_URLS.get(firm)
        if not url:
            print(f"⏭️  {firm}: No URL configured")
            continue
        
        print(f"\n🔍 Scraping {firm}...")
        releases = scrape_press_releases(firm, url)
        
        for rel in releases:
            release_id = release_hash(firm, rel['title'])
            
            if is_duplicate(conn, release_id):
                print(f"  ⊘ Duplicate: {rel['title'][:60]}")
                continue
            
            print(f"  ✓ Found: {rel['title'][:60]}")
            
            # Analyze with Claude
            analysis = analyze_release(client, firm, rel['title'])
            analysis_json = json.dumps(analysis)
            
            # Save to database
            save_release(conn, firm, rel['title'], rel['url'], analysis_json)
            
            new_releases.append({
                'firm': firm,
                'title': rel['title'],
                'url': rel['url'],
                'claude_analysis': analysis_json
            })
            
            # Send real-time alert if high relevance
            if analysis['relevance_score'] >= 7:
                print(f"  📧 Sending real-time alert...")
                send_email(
                    f"[ALERT] {firm}: {rel['title'][:70]}",
                    format_alert_email({
                        'firm': firm,
                        'title': rel['title'],
                        'claude_analysis': analysis_json
                    }),
                    is_html=True
                )
    
    conn.close()
    
    print(f"\n✓ Run complete. {len(new_releases)} new releases found.")
    return new_releases

def run_daily_digest():
    """
    Generate and send daily digest.
    Call this at 8:00 AM UTC.
    """
    print(f"\n{'='*70}")
    print(f"DAILY DIGEST - GENERATION START: {datetime.now().isoformat()}")
    print(f"{'='*70}")
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get releases from last 24 hours not yet sent in digest
    cutoff = datetime.now() - timedelta(days=1)
    c.execute("""
        SELECT firm, title, url, claude_analysis
        FROM releases
        WHERE scraped_date > ? AND sent_in_digest = 0
        ORDER BY scraped_date DESC
    """, (cutoff.isoformat(),))
    
    releases = [
        {
            'firm': row[0],
            'title': row[1],
            'url': row[2],
            'claude_analysis': row[3]
        }
        for row in c.fetchall()
    ]
    
    if releases:
        # Send digest
        html = format_digest_email(releases)
        send_email(
            f"📰 Daily Asset Manager Press Release Digest - {datetime.now().strftime('%Y-%m-%d')}",
            html,
            is_html=True
        )
        
        # Mark as sent
        for rel in releases:
            c.execute(
                "UPDATE releases SET sent_in_digest = 1 WHERE title = ? AND firm = ?",
                (rel['title'], rel['firm'])
            )
        conn.commit()
        
        print(f"✓ Digest sent with {len(releases)} releases.")
    else:
        print("⊘ No new releases for digest.")
    
    conn.close()

# ============================================================================
# CLI / DEPLOYMENT
# ============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "scrape":
            run_scrape_and_analyze()
        elif sys.argv[1] == "digest":
            run_daily_digest()
        else:
            print("Usage: python press_release_agent.py [scrape|digest]")
    else:
        # Default: run scrape
        run_scrape_and_analyze()

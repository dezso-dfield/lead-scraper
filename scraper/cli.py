from __future__ import annotations
import argparse
import signal
import sys
import time
import warnings
import urllib3
warnings.filterwarnings("ignore")
urllib3.disable_warnings()

from pathlib import Path
from datetime import datetime

from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TaskProgressColumn
from rich.text import Text
from rich import box

from scraper.models import Lead

console = Console()


def _check_playwright() -> bool:
    try:
        import subprocess
        result = subprocess.run(
            ["python", "-c", "from playwright.sync_api import sync_playwright"],
            capture_output=True, timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


def build_results_table(leads: list[Lead], max_rows: int = 30) -> Table:
    t = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style="bold white on navy_blue",
        row_styles=["", "on grey7"],
        expand=True,
    )
    t.add_column("#", style="dim", width=4, justify="right")
    t.add_column("Company", min_width=20, max_width=35)
    t.add_column("Email(s)", min_width=25, max_width=40)
    t.add_column("Phone(s)", min_width=16, max_width=24)
    t.add_column("Website", min_width=20, max_width=35)
    t.add_column("Source", width=14)
    t.add_column("Conf", width=6, justify="right")

    for i, lead in enumerate(leads[:max_rows], 1):
        from scraper.extractors.validators import format_phone_display
        conf = lead.confidence
        conf_color = "green" if conf >= 0.7 else "yellow" if conf >= 0.4 else "red"
        t.add_row(
            str(i),
            lead.company_name or "[dim]—[/dim]",
            "\n".join(lead.emails[:2]) if lead.emails else "[dim]—[/dim]",
            "\n".join(format_phone_display(p) for p in lead.phones[:2]) if lead.phones else "[dim]—[/dim]",
            lead.website[:35] + "…" if len(lead.website) > 35 else lead.website or "[dim]—[/dim]",
            ", ".join(lead.sources[:2]),
            f"[{conf_color}]{conf:.0%}[/{conf_color}]",
        )

    if len(leads) > max_rows:
        t.add_row("…", f"[dim]+{len(leads) - max_rows} more in output file[/dim]", "", "", "", "", "")

    return t


def run(args: argparse.Namespace) -> None:
    from scraper.scrapers.deep_search import DeepSearcher
    from scraper.extractors.contact import ContactExtractor
    from scraper.http.session import fetch, get_client
    import scraper.db as db
    from scraper.export.csv_exporter import export_csv
    from scraper.export.excel_exporter import export_excel

    query = args.query
    location = args.location
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    slug = f"{query.replace(' ', '_')}_{location.replace(' ', '_')}_{timestamp}"[:60]

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    stats_counts = {"discovered": 0, "saved_new": 0, "merged": 0, "with_email": 0, "with_phone": 0}

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    )
    discovery_task = progress.add_task("[cyan]Discovering…", total=None)
    enrich_task = progress.add_task("[green]Enriching…", total=None)

    interrupted = [False]
    def _signal_handler(sig, frame):
        interrupted[0] = True
        progress.stop()
        console.print("\n[yellow]Interrupted. Saving partial results…[/yellow]")
    signal.signal(signal.SIGINT, _signal_handler)

    console.print(Panel(
        f"[bold]Lead Scraper — Deep Mode[/bold]\n"
        f"[dim]Query:[/dim] [cyan]{query}[/cyan]  "
        f"[dim]Location:[/dim] [cyan]{location}[/cyan]  "
        f"[dim]Max:[/dim] [cyan]{args.max_leads}[/cyan]\n"
        f"[dim]DB:[/dim] [dim]{db.DB_PATH}[/dim]",
        border_style="navy_blue",
    ))

    client = get_client()
    extractor = ContactExtractor(default_region=args.region)

    def on_progress(msg: str):
        progress.update(discovery_task, description=f"[cyan]{msg[:70]}[/cyan]")

    with progress:
        searcher = DeepSearcher(
            niche=query,
            location=location,
            max_leads=args.max_leads,
            on_progress=on_progress,
        )
        stubs = searcher.run()
        stats_counts["discovered"] = len(stubs)

        progress.update(enrich_task, description=f"[green]Enriching {len(stubs)} URLs…[/green]")

        for stub in stubs:
            if interrupted[0]:
                break
            if not stub.website:
                continue
            if db.exists(stub.canonical_key()):
                db.upsert(stub)
                stats_counts["merged"] += 1
                continue

            enriched = extractor.enrich_lead(stub, lambda url: fetch(url, client=client))
            was_new, _ = db.upsert(enriched)
            if was_new:
                stats_counts["saved_new"] += 1
                if enriched.emails:
                    stats_counts["with_email"] += 1
                if enriched.phones:
                    stats_counts["with_phone"] += 1
            else:
                stats_counts["merged"] += 1

            progress.update(
                enrich_task,
                description=f"[green]Enriched {stats_counts['saved_new'] + stats_counts['merged']}[/green]  "
                            f"[cyan]new: {stats_counts['saved_new']}[/cyan]  "
                            f"email: {stats_counts['with_email']}  phone: {stats_counts['with_phone']}",
            )

    # Pull results from DB for display
    from scraper.models import Lead as LeadModel
    db_rows = db.fetch_all(niche=query)
    leads = []
    for r in db_rows:
        l = LeadModel(
            company_name=r["company_name"], website=r["website"],
            emails=r["emails"], phones=r["phones"], address=r["address"],
            city=r["city"], country=r["country"], niche=r["niche"],
            sources=r["sources"], confidence=r["confidence"],
        )
        leads.append(l)

    if args.email_only:
        leads = [l for l in leads if l.emails]
    if args.phone_only:
        leads = [l for l in leads if l.phones]
    if args.min_confidence > 0:
        leads = [l for l in leads if l.confidence >= args.min_confidence]
    leads.sort(key=lambda l: l.confidence, reverse=True)

    console.print()
    console.print(build_results_table(leads, max_rows=args.preview_rows))

    s = db.stats()
    console.print(Panel(
        f"[bold green]Session: +{stats_counts['saved_new']} new leads[/bold green]  "
        f"[dim]merged: {stats_counts['merged']}[/dim]\n"
        f"[bold]DB Total: {s.get('total', 0)}[/bold]  "
        f"[cyan]email: {s.get('with_email', 0)}[/cyan]  "
        f"[yellow]phone: {s.get('with_phone', 0)}[/yellow]  "
        f"[magenta]both: {s.get('with_both', 0)}[/magenta]\n"
        f"[dim]DB: {db.DB_PATH}[/dim]",
        border_style="green",
    ))

    # Export
    if not args.no_csv:
        csv_p = export_csv(leads, output_dir / f"{slug}.csv")
        console.print(f"[dim]CSV:[/dim]  {csv_p}")
    if not args.no_excel:
        xlsx_p = export_excel(leads, output_dir / f"{slug}.xlsx", query=query, location=location)
        console.print(f"[dim]Excel:[/dim] {xlsx_p}")
    console.print(f"\n[dim]Open TUI to manage all leads:[/dim] [bold]python -m scraper[/bold]")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="scraper",
        description="Ultimate lead scraping tool — find emails & phone numbers by niche and location.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  scraper "event organizers" --location "Budapest, Hungary"
  scraper "AI solution companies" --location "Hungary" --max 200 --serp-api-key YOUR_KEY
  scraper "wedding planners" --location "Budapest" --email-only --no-maps
  scraper "restaurants" --location "Budapest" --region HU --workers 8
        """,
    )
    p.add_argument("query", help="Niche/industry to search for (e.g. 'event organizers')")
    p.add_argument("--location", "-l", default="Budapest, Hungary", help="Target location/region (default: Budapest, Hungary)")
    p.add_argument("--max", "-m", dest="max_leads", type=int, default=100, help="Max leads to collect (default: 100)")
    p.add_argument("--workers", "-w", type=int, default=5, help="Enrichment worker threads (default: 5)")
    p.add_argument("--region", "-r", default="HU", help="Phone number default region code (default: HU)")
    p.add_argument("--output-dir", "-o", default="./leads_output", help="Output directory (default: ./leads_output)")
    p.add_argument("--serp-api-key", default=None, metavar="KEY", help="SerpAPI key for faster Google Search")
    p.add_argument("--no-maps", action="store_true", help="Skip Google Maps scraping (no Playwright needed)")
    p.add_argument("--no-csv", action="store_true", help="Skip CSV export")
    p.add_argument("--no-excel", action="store_true", help="Skip Excel export")
    p.add_argument("--email-only", action="store_true", help="Only output leads with email addresses")
    p.add_argument("--phone-only", action="store_true", help="Only output leads with phone numbers")
    p.add_argument("--min-confidence", type=float, default=0.0, metavar="0.0-1.0", help="Minimum confidence score filter (0.0-1.0)")
    p.add_argument("--preview-rows", type=int, default=30, help="Rows to preview in terminal (default: 30)")
    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()

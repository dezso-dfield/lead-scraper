"""
Lead Manager TUI — built with Textual.

Keybindings:
  n         New scrape
  /         Search / filter
  d         Delete selected
  e         Export visible leads
  Enter     Open lead detail
  o         Open website in browser
  c         Copy email to clipboard
  Tab       Cycle status (new → contacted → qualified → rejected)
  F5        Refresh table
  q / Esc   Quit (from detail) / Quit app
"""
from __future__ import annotations
import json
import subprocess
import threading
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Any

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, ScrollableContainer
from textual.reactive import reactive
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button, DataTable, Footer, Header, Input, Label,
    RichLog, Select, Static, TextArea,
)
from textual.widgets import Checkbox
from rich.text import Text

import scraper.db as db

STATUS_COLORS = {
    "new":        "cyan",
    "contacted":  "yellow",
    "qualified":  "green",
    "rejected":   "red",
}
STATUS_CYCLE = ["new", "contacted", "qualified", "rejected"]


# ─── Scrape Modal ─────────────────────────────────────────────────────────────

class ScrapeModal(ModalScreen):
    """Modal dialog to launch a new scrape job."""

    CSS = """
    ScrapeModal {
        align: center middle;
    }
    ScrapeModal > Vertical {
        width: 70;
        height: auto;
        max-height: 30;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }
    ScrapeModal Label { margin-bottom: 1; color: $text-muted; }
    ScrapeModal Input { margin-bottom: 1; }
    ScrapeModal #btn-row { margin-top: 1; height: 3; }
    ScrapeModal Button { width: 1fr; }
    """

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label("New Scrape Job", id="modal-title")
            yield Label("Niche / Query")
            yield Input(placeholder='e.g. event organizers, AI companies', id="in-niche")
            yield Label("Location")
            yield Input(placeholder='e.g. Budapest, Hungary', id="in-loc", value="Budapest, Hungary")
            yield Label("Max leads (deeper = more time)")
            yield Input(placeholder='100', id="in-max", value="100")
            with Horizontal(id="btn-row"):
                yield Button("Start Scrape", variant="primary", id="btn-start")
                yield Button("Cancel", id="btn-cancel")

    @on(Button.Pressed, "#btn-start")
    def start(self) -> None:
        niche = self.query_one("#in-niche", Input).value.strip()
        location = self.query_one("#in-loc", Input).value.strip()
        max_leads = self.query_one("#in-max", Input).value.strip()
        if not niche:
            return
        try:
            max_leads = int(max_leads)
        except ValueError:
            max_leads = 100
        self.dismiss({"niche": niche, "location": location, "max_leads": max_leads})

    @on(Button.Pressed, "#btn-cancel")
    def cancel(self) -> None:
        self.dismiss(None)

    def on_key(self, event) -> None:
        if event.key == "enter":
            self.start()
        elif event.key == "escape":
            self.cancel()


# ─── Lead Detail Modal ────────────────────────────────────────────────────────

class LeadDetailModal(ModalScreen):
    CSS = """
    LeadDetailModal { align: center middle; }
    LeadDetailModal > Vertical {
        width: 80;
        height: 40;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }
    LeadDetailModal .field-label { color: $text-muted; margin-top: 1; }
    LeadDetailModal .field-value { color: $text; margin-left: 2; }
    LeadDetailModal #status-row { height: 3; margin-top: 1; }
    LeadDetailModal Select { width: 25; }
    LeadDetailModal TextArea { height: 6; margin-top: 1; }
    LeadDetailModal #btn-row { height: 3; margin-top: 1; }
    LeadDetailModal Button { width: 1fr; }
    """

    def __init__(self, lead_id: int):
        super().__init__()
        self.lead_id = lead_id
        self._lead = db.fetch_by_id(lead_id)

    def compose(self) -> ComposeResult:
        lead = self._lead
        if not lead:
            yield Label("Lead not found")
            return

        emails_str = ", ".join(lead["emails"]) or "—"
        phones_str = ", ".join(lead["phones"]) or "—"
        sources_str = ", ".join(lead["sources"]) or "—"

        with Vertical():
            yield Label(f"[bold]{lead['company_name'] or lead['website']}[/bold]", id="detail-title")
            yield Label("Website", classes="field-label")
            yield Label(lead["website"] or "—", classes="field-value", id="det-website")
            yield Label("Emails", classes="field-label")
            yield Label(emails_str, classes="field-value", id="det-emails")
            yield Label("Phones", classes="field-label")
            yield Label(phones_str, classes="field-value")
            yield Label("Address", classes="field-label")
            yield Label(lead["address"] or "—", classes="field-value")
            yield Label("Sources", classes="field-label")
            yield Label(sources_str, classes="field-value")
            yield Label("Confidence", classes="field-label")
            yield Label(f"{lead['confidence']:.0%}", classes="field-value")

            with Horizontal(id="status-row"):
                yield Label("Status:", classes="field-label")
                yield Select(
                    [(s.title(), s) for s in STATUS_CYCLE],
                    value=lead["status"],
                    id="sel-status",
                )

            yield Label("Notes", classes="field-label")
            yield TextArea(lead["notes"], id="ta-notes")

            with Horizontal(id="btn-row"):
                yield Button("Save & Close", variant="primary", id="btn-save")
                yield Button("Open Website", id="btn-open")
                yield Button("Close", id="btn-close")

    @on(Button.Pressed, "#btn-save")
    def save(self) -> None:
        status = self.query_one("#sel-status", Select).value
        notes = self.query_one("#ta-notes", TextArea).text
        db.update_status(self.lead_id, str(status))
        db.update_notes(self.lead_id, notes)
        self.dismiss("saved")

    @on(Button.Pressed, "#btn-open")
    def open_website(self) -> None:
        lead = self._lead
        if lead and lead["website"]:
            webbrowser.open(lead["website"])

    @on(Button.Pressed, "#btn-close")
    def close(self) -> None:
        self.dismiss(None)

    def on_key(self, event) -> None:
        if event.key == "escape":
            self.dismiss(None)


# ─── Scrape Progress Screen ───────────────────────────────────────────────────

class ScrapeScreen(Screen):
    CSS = """
    ScrapeScreen {
        background: $surface;
    }
    ScrapeScreen #log {
        height: 1fr;
        border: round $primary;
        margin: 1;
    }
    ScrapeScreen #status-bar {
        height: 3;
        background: $primary;
        color: $text;
        padding: 0 2;
        content-align: left middle;
    }
    ScrapeScreen #btn-row {
        height: 3;
        margin: 0 1;
        dock: bottom;
    }
    ScrapeScreen Button { width: 1fr; }
    """
    BINDINGS = [Binding("escape", "cancel", "Cancel")]

    def __init__(self, niche: str, location: str, max_leads: int):
        super().__init__()
        self.niche = niche
        self.location = location
        self.max_leads = max_leads
        self._running = True
        self._counts = {"discovered": 0, "saved_new": 0, "merged": 0, "skipped": 0}

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static(
            f"Scraping: [bold]{self.niche}[/bold] in [bold]{self.location}[/bold]  |  Max: {self.max_leads}",
            id="status-bar",
        )
        yield RichLog(id="log", highlight=True, markup=True, wrap=True)
        with Horizontal(id="btn-row"):
            yield Button("Stop & Save", variant="error", id="btn-stop")
            yield Button("Done (Return to List)", variant="primary", id="btn-done", disabled=True)

    def on_mount(self) -> None:
        self._start_scrape()

    @work(thread=True)
    def _start_scrape(self) -> None:
        from scraper.scrapers.deep_search import DeepSearcher
        from scraper.extractors.contact import ContactExtractor
        from scraper.http.session import fetch, get_client

        log = self.query_one("#log", RichLog)
        client = get_client()
        extractor = ContactExtractor(default_region="HU")

        def log_msg(msg: str) -> None:
            self.call_from_thread(log.write, msg)

        def fetch_fn(url: str):
            return fetch(url, client=client)

        log_msg(f"[cyan]Starting deep search for '{self.niche}' in '{self.location}'[/cyan]")
        log_msg(f"[dim]Database: {db.DB_PATH}[/dim]\n")

        searcher = DeepSearcher(
            niche=self.niche,
            location=self.location,
            max_leads=self.max_leads,
            on_progress=log_msg,
        )

        leads = searcher.run()
        self._counts["discovered"] = len(leads)
        log_msg(f"\n[bold green]Discovery complete: {len(leads)} candidate URLs[/bold green]")
        log_msg("[cyan]Enriching — fetching each website for emails & phones…[/cyan]\n")

        for i, lead in enumerate(leads):
            if not self._running:
                break
            if not lead.website:
                continue

            # Fast DB dedup check
            key = lead.canonical_key()
            if db.exists(key):
                # Still upsert to merge new source info, but skip full enrichment
                db.upsert(lead)
                self._counts["merged"] += 1
                self.call_from_thread(self._update_status)
                continue

            # Full enrichment
            enriched = extractor.enrich_lead(lead, fetch_fn)
            was_new, _id = db.upsert(enriched)

            if was_new:
                self._counts["saved_new"] += 1
                if enriched.has_contacts():
                    email_str = enriched.emails[0] if enriched.emails else ""
                    phone_str = enriched.phones[0] if enriched.phones else ""
                    log_msg(
                        f"[green]+[/green] {enriched.company_name or enriched.website[:40]:40} "
                        f"[cyan]{email_str[:35]:35}[/cyan] [yellow]{phone_str}[/yellow]"
                    )
                else:
                    log_msg(f"[dim]  {enriched.website[:60]}[/dim]")
            else:
                self._counts["merged"] += 1

            self.call_from_thread(self._update_status)

            if (i + 1) % 10 == 0:
                log_msg(
                    f"\n[dim]Progress: {i+1}/{len(leads)} enriched | "
                    f"new: {self._counts['saved_new']} | "
                    f"merged: {self._counts['merged']}[/dim]\n"
                )

        log_msg(f"\n[bold green]═══ Scrape Complete ═══[/bold green]")
        log_msg(f"  Discovered:  {self._counts['discovered']}")
        log_msg(f"  New leads:   [green]{self._counts['saved_new']}[/green]")
        log_msg(f"  Merged:      [yellow]{self._counts['merged']}[/yellow]")
        log_msg(f"  DB total:    {db.stats().get('total', 0)}")

        self.call_from_thread(self.query_one, "#btn-done", Button).disabled = False
        self.call_from_thread(self.query_one, "#btn-stop", Button).disabled = True

    def _update_status(self) -> None:
        c = self._counts
        self.query_one("#status-bar", Static).update(
            f"Scraping: [bold]{self.niche}[/bold] in [bold]{self.location}[/bold]  |  "
            f"Found: {c['discovered']}  New: [green]{c['saved_new']}[/green]  "
            f"Merged: [yellow]{c['merged']}[/yellow]"
        )

    @on(Button.Pressed, "#btn-stop")
    def stop(self) -> None:
        self._running = False
        self.query_one("#btn-stop", Button).disabled = True
        self.query_one("#log", RichLog).write("[yellow]Stopping after current enrichment…[/yellow]")

    @on(Button.Pressed, "#btn-done")
    def done(self) -> None:
        self.app.pop_screen()

    def action_cancel(self) -> None:
        self._running = False
        self.app.pop_screen()


# ─── Export Modal ─────────────────────────────────────────────────────────────

class ExportModal(ModalScreen):
    CSS = """
    ExportModal { align: center middle; }
    ExportModal > Vertical {
        width: 60; height: auto;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }
    ExportModal Button { width: 1fr; margin-top: 1; }
    """

    def __init__(self, lead_ids: list[int]):
        super().__init__()
        self._ids = lead_ids

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label(f"Export {len(self._ids)} leads", id="export-title")
            yield Label("Output directory")
            yield Input(value="./leads_output", id="in-dir")
            with Horizontal():
                yield Button("CSV + Excel", variant="primary", id="btn-export")
                yield Button("Cancel", id="btn-cancel")

    @on(Button.Pressed, "#btn-export")
    def do_export(self) -> None:
        out_dir = self.query_one("#in-dir", Input).value.strip() or "./leads_output"
        rows = [db.fetch_by_id(i) for i in self._ids if db.fetch_by_id(i)]
        if not rows:
            self.dismiss(None)
            return

        from scraper.models import Lead
        from scraper.export.csv_exporter import export_csv
        from scraper.export.excel_exporter import export_excel

        leads = []
        for r in rows:
            l = Lead(
                company_name=r["company_name"],
                website=r["website"],
                emails=r["emails"],
                phones=r["phones"],
                address=r["address"],
                city=r["city"],
                country=r["country"],
                niche=r["niche"],
                sources=r["sources"],
                confidence=r["confidence"],
            )
            leads.append(l)

        from pathlib import Path
        from datetime import datetime
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        csv_p = export_csv(leads, out / f"leads_{ts}.csv")
        xlsx_p = export_excel(leads, out / f"leads_{ts}.xlsx")
        self.dismiss({"csv": str(csv_p), "xlsx": str(xlsx_p)})

    @on(Button.Pressed, "#btn-cancel")
    def cancel(self) -> None:
        self.dismiss(None)


# ─── Main Leads Screen ────────────────────────────────────────────────────────

class LeadsScreen(Screen):
    CSS = """
    LeadsScreen {
        layout: vertical;
    }
    #filter-bar {
        height: 3;
        background: $surface-darken-1;
        padding: 0 1;
        border-bottom: solid $primary;
    }
    #filter-bar Input {
        width: 1fr;
        margin-right: 1;
    }
    #filter-bar Select {
        width: 20;
        margin-right: 1;
    }
    #filter-bar Checkbox {
        margin-right: 2;
        height: 3;
        content-align: center middle;
    }
    #stats-bar {
        height: 1;
        background: $primary-darken-2;
        color: $text-muted;
        padding: 0 1;
        content-align: left middle;
    }
    #leads-table {
        height: 1fr;
        border: none;
    }
    """

    BINDINGS = [
        Binding("n", "new_scrape", "New Scrape"),
        Binding("slash", "focus_search", "Search"),
        Binding("d", "delete_selected", "Delete"),
        Binding("e", "export", "Export"),
        Binding("enter", "open_detail", "Detail"),
        Binding("o", "open_website", "Open URL"),
        Binding("c", "copy_email", "Copy Email"),
        Binding("t", "cycle_status", "Toggle Status"),
        Binding("f5", "refresh", "Refresh"),
        Binding("q", "quit", "Quit"),
    ]

    _search = reactive("")
    _status_filter = reactive("")
    _has_email_filter = reactive(False)
    _has_phone_filter = reactive(False)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="filter-bar"):
            yield Input(placeholder="Search companies, emails…", id="in-search")
            yield Select(
                [("All Statuses", ""), ("New", "new"), ("Contacted", "contacted"),
                 ("Qualified", "qualified"), ("Rejected", "rejected")],
                value="",
                id="sel-status-filter",
                allow_blank=False,
            )
            yield Checkbox("Email", False, id="chk-email")
            yield Checkbox("Phone", False, id="chk-phone")
        yield Static("", id="stats-bar")
        yield DataTable(id="leads-table", cursor_type="row", zebra_stripes=True)
        yield Footer()

    def on_mount(self) -> None:
        self._setup_table()
        self._load_leads()

    def _setup_table(self) -> None:
        table = self.query_one("#leads-table", DataTable)
        table.add_columns(
            "#", "Company", "Email", "Phone", "Website", "Niche", "Src", "Conf", "Status"
        )

    def _load_leads(self) -> None:
        table = self.query_one("#leads-table", DataTable)
        table.clear()

        rows = db.fetch_all(
            search=self._search,
            status=self._status_filter,
            has_email=self._has_email_filter,
            has_phone=self._has_phone_filter,
        )

        for i, r in enumerate(rows, 1):
            emails = r["emails"]
            phones = r["phones"]
            status = r["status"]
            conf = r["confidence"]

            email_str = emails[0][:30] if emails else "—"
            phone_str = phones[0] if phones else "—"
            website = r["website"]
            short_web = website.removeprefix("https://").removeprefix("http://").removeprefix("www.")[:28]
            conf_color = "green" if conf >= 0.7 else "yellow" if conf >= 0.4 else "red"
            status_color = STATUS_COLORS.get(status, "white")

            table.add_row(
                str(i),
                (r["company_name"] or _domain_from(website))[:32],
                email_str,
                phone_str,
                short_web,
                r["niche"][:18],
                ", ".join(r["sources"])[:10],
                Text(f"{conf:.0%}", style=conf_color),
                Text(status.title(), style=f"bold {status_color}"),
                key=str(r["id"]),
            )

        s = db.stats()
        self.query_one("#stats-bar", Static).update(
            f" Total: [bold]{s.get('total',0)}[/bold]  "
            f"[cyan]email: {s.get('with_email',0)}[/cyan]  "
            f"[yellow]phone: {s.get('with_phone',0)}[/yellow]  "
            f"[green]both: {s.get('with_both',0)}[/green]  "
            f"[dim]| new: {s.get('status_new',0)}  "
            f"contacted: {s.get('status_contacted',0)}  "
            f"qualified: {s.get('status_qualified',0)}[/dim]  "
            f"[dim]Showing: {len(rows)}[/dim]"
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _cursor_lead_id(self) -> int | None:
        """Return the DB id of the currently highlighted row, or None."""
        table = self.query_one("#leads-table", DataTable)
        if table.cursor_row is None:
            return None
        try:
            row_key = table.cursor_row
            rk = list(table.rows.keys())[row_key]
            return int(rk.value)
        except Exception:
            return None

    # ── Actions ──────────────────────────────────────────────────────────────

    def action_new_scrape(self) -> None:
        def on_result(result) -> None:
            if result:
                self.app.push_screen(
                    ScrapeScreen(result["niche"], result["location"], result["max_leads"])
                )
        self.app.push_screen(ScrapeModal(), on_result)

    def action_focus_search(self) -> None:
        self.query_one("#in-search", Input).focus()

    def action_refresh(self) -> None:
        self._load_leads()

    def action_delete_selected(self) -> None:
        lead_id = self._cursor_lead_id()
        if lead_id is None:
            return
        db.delete(lead_id)
        self._load_leads()

    def action_open_detail(self) -> None:
        lead_id = self._cursor_lead_id()
        if lead_id is None:
            return
        self.app.push_screen(LeadDetailModal(lead_id), lambda _: self._load_leads())

    def action_open_website(self) -> None:
        lead_id = self._cursor_lead_id()
        if lead_id is None:
            return
        lead = db.fetch_by_id(lead_id)
        if lead and lead["website"]:
            webbrowser.open(lead["website"])

    def action_copy_email(self) -> None:
        lead_id = self._cursor_lead_id()
        if lead_id is None:
            return
        lead = db.fetch_by_id(lead_id)
        if lead and lead["emails"]:
            email = lead["emails"][0]
            subprocess.run(["pbcopy"], input=email.encode(), check=False)
            self.notify(f"Copied: {email}", title="Clipboard")

    def action_cycle_status(self) -> None:
        lead_id = self._cursor_lead_id()
        if lead_id is None:
            return
        lead = db.fetch_by_id(lead_id)
        if not lead:
            return
        current = lead["status"]
        idx = STATUS_CYCLE.index(current) if current in STATUS_CYCLE else 0
        db.update_status(lead_id, STATUS_CYCLE[(idx + 1) % len(STATUS_CYCLE)])
        self._load_leads()

    def action_export(self) -> None:
        rows = db.fetch_all(
            search=self._search,
            status=self._status_filter,
            has_email=self._has_email_filter,
            has_phone=self._has_phone_filter,
        )
        ids = [r["id"] for r in rows]
        if not ids:
            return

        def on_result(result) -> None:
            if result:
                self.notify(
                    f"Saved:\n{result['csv']}\n{result['xlsx']}",
                    title="Export Complete",
                )
        self.app.push_screen(ExportModal(ids), on_result)

    def action_quit(self) -> None:
        self.app.exit()

    # ── Filter handlers ───────────────────────────────────────────────────────

    @on(Input.Changed, "#in-search")
    def search_changed(self, event: Input.Changed) -> None:
        self._search = event.value
        self._load_leads()

    @on(Select.Changed, "#sel-status-filter")
    def status_filter_changed(self, event: Select.Changed) -> None:
        self._status_filter = str(event.value) if event.value else ""
        self._load_leads()

    @on(Checkbox.Changed, "#chk-email")
    def email_filter_changed(self, event: Checkbox.Changed) -> None:
        self._has_email_filter = event.value
        self._load_leads()

    @on(Checkbox.Changed, "#chk-phone")
    def phone_filter_changed(self, event: Checkbox.Changed) -> None:
        self._has_phone_filter = event.value
        self._load_leads()

    @on(DataTable.RowSelected)
    def row_selected(self, event: DataTable.RowSelected) -> None:
        # Double-click or Enter opens detail — handled by action_open_detail
        pass


def _domain_from(url: str) -> str:
    from urllib.parse import urlparse
    d = urlparse(url).netloc.lower().removeprefix("www.")
    return d.split(".")[0].replace("-", " ").title()


# ─── App ──────────────────────────────────────────────────────────────────────

class LeadManagerApp(App):
    TITLE = "Lead Manager — Scraper"
    SUB_TITLE = f"DB: {db.DB_PATH}"
    CSS = """
    App { background: $background; }
    """

    def on_mount(self) -> None:
        self.push_screen(LeadsScreen())

    def on_screen_resume(self, event) -> None:
        # Refresh leads table whenever we pop back to it
        if isinstance(self.screen, LeadsScreen):
            self.screen._load_leads()


def run_app() -> None:
    LeadManagerApp().run()

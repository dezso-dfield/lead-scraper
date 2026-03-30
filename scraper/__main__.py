"""
Entry point:
  python -m scraper          → Web UI  (http://localhost:7337)
  python -m scraper --tui    → Terminal TUI
  python -m scraper <query>  → Headless CLI scrape
"""
import sys
import warnings
import urllib3
warnings.filterwarnings("ignore")
urllib3.disable_warnings()


def main_entry():
    """Entry point for the `scraper` console script."""
    args = sys.argv[1:]
    if "--tui" in args:
        from scraper.tui.app import run_app
        run_app()
    elif not args or args == ["--web"]:
        from scraper.web.server import run_server
        run_server()
    else:
        from scraper.cli import main
        main()


if __name__ == "__main__":
    main_entry()

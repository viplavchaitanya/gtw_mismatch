# run_app.py
import os
import sys

def main():
    """
    Minimal launcher that runs Streamlit 'app.py' the same way as 'streamlit run app.py'.
    Keeps your app code unchanged. Works well with PyInstaller.
    """
    # Always run from this script's directory (stable relative paths)
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Compose arguments for Streamlit
    streamlit_args = [
        "streamlit", "run", "app.py",
        "--global.developmentMode=false",       # avoid dev mode banner
        "--server.headless=true",               # no auto browser on first start (exe UX)
        "--browser.gatherUsageStats=false",     # silence analytics
        "--logger.level=info"                   # or "warning"
    ]

    # Hand off to Streamlit's CLI
    # (supported way to run programmatically)
    from streamlit.web import cli as stcli  # Streamlit's documented entrypoint
    sys.argv = streamlit_args
    sys.exit(stcli.main())

if __name__ == "__main__":
    main()
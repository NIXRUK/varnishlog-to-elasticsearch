"""
Command-line interface for varnishlog-to-elasticsearch.
"""

import sys
from .parser import main_loop

def main():
    """Main entry point for the CLI."""
    try:
        main_loop(sys.stdin)
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 
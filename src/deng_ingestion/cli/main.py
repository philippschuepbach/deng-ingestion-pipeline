from __future__ import annotations

from deng_ingestion.cli.handlers import dispatch
from deng_ingestion.cli.parser import build_parser
from deng_ingestion.core.logging import configure_logging


def main() -> None:
    configure_logging()

    parser = build_parser()
    args = parser.parse_args()
    dispatch(args)


if __name__ == "__main__":
    main()

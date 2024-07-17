import typer

from . import cli


def main() -> None:
    typer.run(cli.make_report)


if __name__ == "__main__":
    main()

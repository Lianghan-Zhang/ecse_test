#!/usr/bin/env python3
"""
MV Splitter: Split mv_candidates.sql into individual SQL files.

Each MV is saved as a separate file with its corresponding comments.
"""

import re
from pathlib import Path


def split_mv_candidates(
    input_file: Path,
    output_dir: Path,
) -> int:
    """
    Split mv_candidates.sql into individual SQL files.

    Args:
        input_file: Path to mv_candidates.sql
        output_dir: Directory to save split files

    Returns:
        Number of files created
    """
    # Read input file
    content = input_file.read_text(encoding="utf-8")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Split by the separator line
    separator = "-- ============================================================"

    # Find all MV blocks
    # Pattern: separator + comments + separator + CREATE VIEW ... ;
    mv_pattern = re.compile(
        r'(-- ={60,}\n'           # Opening separator
        r'-- MV: (mv_\d+)\n'      # MV name line (capture group 2)
        r'.*?'                     # Other comment lines
        r'-- ={60,}\n'            # Closing separator
        r'CREATE VIEW .*?;)',     # CREATE VIEW statement ending with ;
        re.DOTALL
    )

    matches = mv_pattern.findall(content)

    count = 0
    for mv_block, mv_name in matches:
        # Create output file path
        output_file = output_dir / f"{mv_name}.sql"

        # Write MV block to file
        output_file.write_text(mv_block, encoding="utf-8")
        count += 1
        print(f"Created: {output_file.name}")

    return count


def main():
    """Main entry point."""
    # Default paths
    project_root = Path(__file__).parent
    input_file = project_root / "output" / "mv_candidates.sql"
    output_dir = project_root / "output" / "splited_mv"

    print(f"Input: {input_file}")
    print(f"Output directory: {output_dir}")
    print("-" * 40)

    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return 1

    count = split_mv_candidates(input_file, output_dir)

    print("-" * 40)
    print(f"Done! Created {count} SQL files in {output_dir}")

    return 0


if __name__ == "__main__":
    exit(main())

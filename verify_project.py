"""Run the original regression and the verified-sampling business checks.

Exits non-zero if either phase fails.  The verified phase runs the F-batch
demonstration in a throwaway directory: omission rejection, standby completion
to 18 L, and retrieval after reopening only the isolation store.
"""
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import check_project
from lake_cli import demo_verified


def main():
    regression = check_project.main()
    if regression != 0:
        return regression
    try:
        with TemporaryDirectory(prefix='qingyuan-verify-') as temporary:
            demo_verified(Path(temporary))
    except BaseException as error:  # report, then fail
        print(f'VERIFIED CHECK FAILED {type(error).__name__}: {error}', file=sys.stderr)
        return 1
    print('VERIFIED SUMMARY result=PASS')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

"""Tests for new_file_notification.get_primes."""

import numpy as np
import pytest

from new_file_notification.get_primes import (
    NPRIMES_FUDGE,
    get_primes,
    get_primes_parallel,
)

# ---------------------------------------------------------------------------
# Known reference values
# ---------------------------------------------------------------------------

# The first 25 primes.
FIRST_25_PRIMES = [
    2, 3, 5, 7, 11, 13, 17, 19, 23, 29,
    31, 37, 41, 43, 47, 53, 59, 61, 67, 71,
    73, 79, 83, 89, 97,
]


# ===========================================================================
# Tests for the sequential get_primes
# ===========================================================================

class TestGetPrimesSequential:
    def test_max_value_basic(self):
        """Primes below 100 match the known list."""
        result = get_primes(max_value=100)
        assert list(result) == FIRST_25_PRIMES

    def test_max_value_two_excluded(self):
        """max_value uses strict upper bound (np.arange upper-bound exclusive)."""
        result = get_primes(max_value=10)
        assert list(result) == [2, 3, 5, 7]

    def test_max_value_prime_boundary(self):
        """A max_value that is itself prime should NOT be included."""
        result = get_primes(max_value=7)
        assert 7 not in result
        assert list(result) == [2, 3, 5]

    def test_numprimes_returns_enough(self):
        """Asking for N primes should return at least N primes.

        NPRIMES_FUDGE=1.3 is tight for very small N; the approximation is
        reliable for N >= 20.
        """
        for n in (20, 100, 500):
            result = get_primes(numprimes=n)
            assert result is not None
            assert len(result) >= n, f"Expected >= {n} primes, got {len(result)}"

    def test_no_args_returns_none(self, capsys):
        """Calling with neither argument returns None and prints a message."""
        result = get_primes()
        assert result is None
        captured = capsys.readouterr()
        assert "Aborting" in captured.out

    def test_result_is_sorted(self):
        result = get_primes(max_value=200)
        assert list(result) == sorted(result)

    def test_all_entries_are_prime(self):
        """Every number in the result must actually be prime."""
        result = get_primes(max_value=200)
        for p in result:
            assert _is_prime(p), f"{p} is not prime"

    def test_no_composites_missing(self):
        """No prime below max_value should be absent from the result."""
        result = set(get_primes(max_value=200))
        for p in FIRST_25_PRIMES:  # all <= 97 < 200
            assert p in result


# ===========================================================================
# Tests for the parallel get_primes_parallel
# ===========================================================================

class TestGetPrimesParallel:
    def test_max_value_basic(self):
        result = get_primes_parallel(max_value=100)
        assert list(result) == FIRST_25_PRIMES

    def test_no_args_returns_none(self, capsys):
        result = get_primes_parallel()
        assert result is None
        captured = capsys.readouterr()
        assert "Aborting" in captured.out

    def test_matches_sequential_small(self):
        """Parallel result must equal the sequential result for a small n."""
        seq = get_primes(max_value=500)
        par = get_primes_parallel(max_value=500, num_workers=2)
        np.testing.assert_array_equal(seq, par)

    def test_matches_sequential_large(self):
        """Parallel result must equal the sequential result for a larger n."""
        seq = get_primes(max_value=100_000)
        par = get_primes_parallel(max_value=100_000, num_workers=4)
        np.testing.assert_array_equal(seq, par)

    def test_single_worker(self):
        """num_workers=1 should still produce the correct answer."""
        seq = get_primes(max_value=1_000)
        par = get_primes_parallel(max_value=1_000, num_workers=1)
        np.testing.assert_array_equal(seq, par)

    def test_more_workers_than_segments(self):
        """Works correctly even when num_workers exceeds natural segment count."""
        seq = get_primes(max_value=50)
        par = get_primes_parallel(max_value=50, num_workers=32)
        np.testing.assert_array_equal(seq, par)

    def test_result_is_sorted(self):
        result = get_primes_parallel(max_value=200)
        assert list(result) == sorted(result)

    def test_numprimes_returns_enough(self):
        """NPRIMES_FUDGE=1.3 is reliable for N >= 20."""
        for n in (20, 100, 500):
            result = get_primes_parallel(numprimes=n)
            assert result is not None
            assert len(result) >= n

    def test_small_n(self):
        """Very small n values should not crash and should return valid results."""
        for n in (2, 3, 4, 5, 10):
            seq = get_primes(max_value=n)
            par = get_primes_parallel(max_value=n)
            np.testing.assert_array_equal(seq, par)


# ===========================================================================
# Helper
# ===========================================================================

def _is_prime(n):
    """Naïve primality check used only in tests."""
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(n ** 0.5) + 1, 2):
        if n % i == 0:
            return False
    return True

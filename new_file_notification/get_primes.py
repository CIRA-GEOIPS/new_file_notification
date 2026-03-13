#!/usr/bin/env python
"""Prime number generation via sequential and parallel Sieve of Eratosthenes.

Two public functions are provided:

* :func:`get_primes`          – original sequential sieve.
* :func:`get_primes_parallel` – parallelized segmented sieve that splits the
  work across multiple CPU cores using :mod:`multiprocessing`.
"""

import multiprocessing
from multiprocessing import Pool

import numpy as np

# Safety margin used when estimating an upper bound for the n-th prime via the
# prime-counting approximation  n * ln(n).
NPRIMES_FUDGE = 1.3


# ---------------------------------------------------------------------------
# Sequential sieve
# ---------------------------------------------------------------------------

def get_primes(numprimes=None, max_value=None):
    """Return prime numbers using a sequential Sieve of Eratosthenes.

    Parameters
    ----------
    numprimes : int, optional
        Approximate number of primes to return.  An upper-bound estimate is
        derived from the prime-number theorem.
    max_value : int, optional
        Return all primes strictly less than this value.

    Returns
    -------
    numpy.ndarray or None
        1-D array of prime numbers in ascending order, or ``None`` if neither
        *numprimes* nor *max_value* is supplied.
    """
    if max_value:
        n = max_value
    elif numprimes:
        n = int(numprimes * np.log(numprimes) * NPRIMES_FUDGE)
    else:
        print("Neither numprimes nor max_value set. Aborting!")
        return None

    s = np.arange(3, n, 2)
    for m in range(3, int(n ** 0.5) + 1, 2):
        if s[(m - 3) // 2]:
            s[(m * m - 3) // 2::m] = 0

    return np.r_[2, s[s > 0]]


# ---------------------------------------------------------------------------
# Parallel segmented sieve – worker
# ---------------------------------------------------------------------------

def _sieve_segment(args):
    """Sieve one segment of odd integers and return the primes found in it.

    This is the worker function used by :func:`get_primes_parallel`.

    Parameters
    ----------
    args : tuple
        ``(low, high, small_primes)``

        * *low*  – first (odd) integer of the segment, inclusive.
        * *high* – last integer of the segment, inclusive.
        * *small_primes* – list of all primes up to ``sqrt(overall_n)``;
          these are the only factors that can produce composites in any
          segment up to *n*.

    Returns
    -------
    numpy.ndarray
        1-D int64 array of primes found in ``[low, high]``.
    """
    low, high, small_primes = args

    # Guarantee that *low* is odd so that the index arithmetic below is valid.
    if low % 2 == 0:
        low += 1

    if low > high:
        return np.array([], dtype=np.int64)

    # Build a boolean sieve over the odd integers in [low, high].
    # Element *i* represents the odd integer  low + 2*i.
    size = (high - low) // 2 + 1
    is_prime = np.ones(size, dtype=bool)

    for p in small_primes:
        if p == 2:
            continue  # segment contains only odd numbers; skip the even prime

        # Locate the first odd multiple of *p* that lies within [low, high].
        # Step 1 – find the smallest multiple of p that is >= low.
        start = ((low + p - 1) // p) * p
        # Step 2 – if that multiple is even, advance by p to get the odd one.
        if start % 2 == 0:
            start += p

        if start > high:
            continue

        # Consecutive odd multiples of p differ by 2p, so they occupy every
        # p-th slot in our array of odd numbers (slot spacing = 2).
        # Mark them composite with a single numpy slice assignment.
        start_idx = (start - low) // 2
        is_prime[start_idx::p] = False

    return (low + 2 * np.where(is_prime)[0]).astype(np.int64)


# ---------------------------------------------------------------------------
# Parallel segmented sieve – public interface
# ---------------------------------------------------------------------------

def get_primes_parallel(numprimes=None, max_value=None, num_workers=None):
    """Return prime numbers using a parallelized segmented Sieve of Eratosthenes.

    The algorithm proceeds in two phases:

    1. **Sequential phase** – find every prime up to ``sqrt(n)`` with the
       standard sieve.  Because ``sqrt(n) << n`` this step is very fast.

    2. **Parallel phase** – partition the odd integers in ``(sqrt(n), n)``
       into *num_workers* roughly equal segments and sieve each segment
       independently in a separate worker process, using the primes from
       phase 1.  Segments are embarrassingly parallel: each worker writes
       only to its own output array.

    Parameters
    ----------
    numprimes : int, optional
        Approximate number of primes to return.
    max_value : int, optional
        Return all primes strictly less than this value.
    num_workers : int, optional
        Number of worker processes.  Defaults to
        :func:`multiprocessing.cpu_count`.

    Returns
    -------
    numpy.ndarray or None
        1-D int64 array of primes in ascending order, or ``None`` if neither
        *numprimes* nor *max_value* is supplied.
    """
    if max_value:
        n = max_value
    elif numprimes:
        n = int(numprimes * np.log(numprimes) * NPRIMES_FUDGE)
    else:
        print("Neither numprimes nor max_value set. Aborting!")
        return None

    if num_workers is None:
        num_workers = multiprocessing.cpu_count()

    # ------------------------------------------------------------------
    # Phase 1 – sequential sieve for the "small" primes up to sqrt(n).
    # Use limit+1 as the max_value so that any prime equal to limit is
    # included (np.arange's upper-bound is exclusive).
    # The "+ 2" ensures limit > sqrt(n) even after integer truncation,
    # so every prime needed to sieve the segments is captured.
    # ------------------------------------------------------------------
    limit = int(n ** 0.5) + 2
    small_primes = list(get_primes(max_value=limit + 1))

    # If n is so small that the sequential sieve already covers it,
    # delegate entirely to avoid spawning unnecessary worker processes.
    if limit >= n:
        return get_primes(max_value=n).astype(np.int64)

    # ------------------------------------------------------------------
    # Phase 2 – parallel segmented sieve over the odd integers in
    # (limit, n).  Each segment is processed by one worker.
    # ------------------------------------------------------------------
    # First odd integer > limit.
    seg_start = limit + 1 if (limit + 1) % 2 != 0 else limit + 2

    # Divide the odd integers in [seg_start, n) into num_workers chunks.
    total_odds = (n - seg_start) // 2 + 1
    odds_per_chunk = max(total_odds // num_workers, 1)

    segments = []
    lo = seg_start
    while lo < n:
        # Upper bound of this chunk (last odd integer, inclusive).
        hi = lo + 2 * (odds_per_chunk - 1)
        hi = min(hi, n - 1)
        if hi % 2 == 0:
            hi -= 1
        segments.append((lo, hi, small_primes))
        lo = hi + 2

    with Pool(processes=num_workers) as pool:
        results = pool.map(_sieve_segment, segments)

    # Combine: small_primes covers [2, limit); results cover [limit, n).
    # np.concatenate handles empty arrays, so no filtering is required.
    all_primes = np.concatenate(
        [np.array(small_primes, dtype=np.int64)] + results
    )
    return all_primes

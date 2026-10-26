"""Retry dengan exponential backoff untuk kegagalan sementara (transient)."""

import time


class TransientSourceError(Exception):
    """Kegagalan sementara pada sumber data; layak dicoba kembali."""


# Hanya error jenis ini yang di-retry. ValueError, KeyError, dll. dianggap
# permanen (bug atau data salah) sehingga langsung dilempar tanpa retry.
RETRYABLE_ERRORS = (TransientSourceError, ConnectionError, TimeoutError)


def with_retry(
    func,
    *,
    max_attempts=4,
    base_delay=1.0,
    max_delay=30.0,
    retryable=RETRYABLE_ERRORS,
    sleep=time.sleep,
    log=print,
):
    """Jalankan func(); ulangi saat error transient dengan jeda 1s, 2s, 4s, ...

    Jeda = min(base_delay * 2**(percobaan-1), max_delay). Setelah
    max_attempts gagal, error terakhir dilempar ulang.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts minimal 1")

    for attempt in range(1, max_attempts + 1):
        try:
            return func()

        except retryable as exc:
            if attempt == max_attempts:
                log(
                    f"  Percobaan {attempt}/{max_attempts} gagal "
                    f"({exc}); batas percobaan habis."
                )
                raise

            delay = min(base_delay * 2 ** (attempt - 1), max_delay)
            log(
                f"  Percobaan {attempt}/{max_attempts} gagal ({exc}); "
                f"retry dalam {delay:g} detik..."
            )
            sleep(delay)

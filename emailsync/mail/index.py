"""In-memory index of what already exists in the destination, with counting semantics.

Each destination copy of a message can "satisfy" exactly one source message. This keeps legitimate
duplicates (several distinct source messages sharing a Message-ID) correct while never re-copying a
message that is already there, whether we or another tool put it there.
"""

import threading
from collections import Counter, defaultdict


class DestIndex:
    def __init__(self, scope: str = "folder"):
        self.scope = scope
        self._counts: dict[str, Counter] = defaultdict(Counter)
        self._global: Counter = Counter()
        self._refs: dict[str, list[tuple[str, frozenset]]] = defaultdict(list)
        self._indexed: set[str] = set()
        self._lock = threading.Lock()

    def mark_indexed(self, folder: str) -> None:
        self._indexed.add(folder)

    def is_indexed(self, folder: str) -> bool:
        return folder in self._indexed

    def add(self, folder: str, key: str) -> None:
        with self._lock:
            self._counts[folder][key] += 1
            self._global[key] += 1

    def add_ref(self, key: str, ref: str, folders) -> None:
        """Remember a destination object (e.g. JMAP email id) and the folders it is in."""
        with self._lock:
            self._refs[key].append((ref, frozenset(folders)))

    def ref_for(self, key: str) -> tuple[str, frozenset] | None:
        refs = self._refs.get(key)
        return refs[0] if refs else None

    def count(self, folder: str, key: str) -> int:
        return self._counts[folder][key]

    def size(self) -> int:
        return sum(self._global.values())

    def _consume(self, folder: str, key: str) -> bool:
        if self._counts[folder][key] > 0:
            self._counts[folder][key] -= 1
            return True
        return False

    def decide(self, key: str, targets: list[str]) -> list[str]:
        """Consume matching copies; return the targets still missing in the destination."""
        with self._lock:
            if self.scope == "account":
                if self._global[key] > 0:
                    self._global[key] -= 1
                    for t in targets:
                        self._consume(t, key)
                    return []
                return list(targets)
            missing = []
            for t in targets:
                if self._consume(t, key):
                    self._global[key] -= 1
                else:
                    missing.append(t)
            return missing

    def account_for(self, key: str, targets: list[str]) -> None:
        """A source message already settled in the ledger: its copies are not available to others."""
        self.decide(key, targets)

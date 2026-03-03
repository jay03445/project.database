#!/usr/bin/env python3
import sys
import os

DATA_FILE = "data.db"


class KeyValueStore:
    """
    Simple append-only, persistent key-value store.

    - Uses data.db as an append-only log.
    - In-memory index is a list of [key, value] pairs (no dicts).
    - On startup, replays the log so the latest SET wins.
    """

    def __init__(self):
        # In-memory index: list of [key, value]
        self.pairs = []

        # Open the log file in append+read mode, create if it doesn't exist
        self.log_file = open(DATA_FILE, "a+", encoding="utf-8")

        # Rebuild in-memory index from the log
        self._replay_log()

    def _replay_log(self):
        """
        Read the entire log from the beginning and rebuild the in-memory index.
        """
        # Go to the beginning of the file
        self.log_file.seek(0)

        for line in self.log_file:
            line = line.strip()
            if not line:
                continue

            # Format: SET <key> <value>
            parts = line.split(" ", 2)
            if len(parts) < 3:
                continue

            cmd, key, value = parts[0], parts[1], parts[2]
            if cmd == "SET":
                self._set_in_memory(key, value)

        # Move file pointer back to the end for future appends
        self.log_file.seek(0, os.SEEK_END)

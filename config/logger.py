"""Module providing logging functionality for the project"""

import logging
import os
import sys


class LoggerConfig:
    """
    A class that provides logging functionality.

    Parameters
    ----------
    name : str
        The logger name.
    stream_handler : bool
        If True, a StreamHandler will be created.
    file_handler : bool
        If True, a FileHandler will be created.

    Methods
    -------
    get_logger()
        Returns the logger object.
    """

    def __init__(
        self,
        name: str,
        stream_handler: bool = True,
        file_handler: bool = True,
    ):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        if not self.logger.handlers:
            self._configure_handlers(stream_handler, file_handler)

    def _configure_handlers(self, stream_handler, file_handler):
        """Set handlers for the logger"""

        # Formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )

        # StreamHandler
        if stream_handler:
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(10)
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        # FileHandler
        if file_handler:
            path = os.path.abspath(".")
            log_file_path = os.path.join(path, f"{self.logger.name}.log")

            fh = logging.FileHandler(log_file_path, mode="w", encoding="utf-8")
            fh.setLevel(10)
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)


    def get_logger(self):
        """
        Returns the logger object.
        """
        return self.logger


# Centralized logger configuration
logger = LoggerConfig("logs").get_logger()

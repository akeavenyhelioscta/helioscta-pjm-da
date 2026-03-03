"""Pipeline logging utilities — adapted from da-model."""
import io
import logging
import sys
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Union
from contextlib import contextmanager


_logger_instance: Optional['PipelineLogger'] = None


class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"
    WHITE = "\033[37m"
    BG_RED = "\033[41m"


LEVEL_COLORS = {
    logging.DEBUG: Colors.BRIGHT_BLACK,
    logging.INFO: Colors.BRIGHT_GREEN,
    logging.WARNING: Colors.BRIGHT_YELLOW,
    logging.ERROR: Colors.BRIGHT_RED,
    logging.CRITICAL: Colors.BOLD + Colors.BG_RED + Colors.WHITE,
}

LEVEL_ICONS = {
    logging.DEBUG: "🔍",
    logging.INFO: "ℹ️ ",
    logging.WARNING: "⚠️ ",
    logging.ERROR: "❌",
    logging.CRITICAL: "🔥",
}


def supports_color() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    if not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
        return False
    if sys.platform == "win32":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
            return True
        except Exception:
            return os.environ.get("TERM") == "xterm"
    return True


def _get_mst_timestamp() -> datetime:
    mst = timezone(timedelta(hours=-7))
    return datetime.now(mst)


class ColoredFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, use_colors=True, use_icons=True):
        super().__init__(fmt, datefmt)
        self.use_colors = use_colors and supports_color()
        self.use_icons = use_icons
        self._original_fmt = fmt
        if self.use_colors and fmt:
            self._colored_fmt = fmt.replace(
                "%(filename)s:%(funcName)s:%(lineno)d",
                "%(colored_location)s"
            )
        else:
            self._colored_fmt = fmt

    def format(self, record):
        original_levelname = record.levelname
        original_msg = record.msg
        if self.use_colors:
            color = LEVEL_COLORS.get(record.levelno, Colors.RESET)
            record.levelname = f"{color}{record.levelname}{Colors.RESET}"
            record.colored_location = (
                f"{Colors.CYAN}{record.filename}{Colors.RESET}:"
                f"{Colors.BRIGHT_MAGENTA}{record.funcName}{Colors.RESET}:"
                f"{Colors.YELLOW}{record.lineno}{Colors.RESET}"
            )
            if record.levelno >= logging.WARNING:
                record.msg = f"{color}{record.msg}{Colors.RESET}"
            self._style._fmt = self._colored_fmt
        if self.use_icons:
            icon = LEVEL_ICONS.get(record.levelno, "")
            record.levelname = f"{icon} {record.levelname}"
        result = super().format(record)
        record.levelname = original_levelname
        record.msg = original_msg
        if self.use_colors:
            self._style._fmt = self._original_fmt
        return result


class PlainFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, use_icons=False):
        super().__init__(fmt, datefmt)
        self.use_icons = use_icons

    def format(self, record):
        if self.use_icons:
            original_levelname = record.levelname
            icon = LEVEL_ICONS.get(record.levelno, "")
            record.levelname = f"{icon} {record.levelname}"
            result = super().format(record)
            record.levelname = original_levelname
            return result
        return super().format(record)


def get_logger() -> logging.Logger:
    if _logger_instance is not None:
        return _logger_instance.logger
    return logging.getLogger()


def init_logging(
    name: str = "logger",
    log_dir: Union[str, Path] = "logs",
    level: int = logging.INFO,
    log_to_file: bool = True,
    delete_if_no_errors: bool = True,
    use_colors: bool = True,
    use_icons: bool = True,
    capture_root: bool = True,
) -> 'PipelineLogger':
    global _logger_instance
    if _logger_instance is not None:
        _logger_instance.close()
    _logger_instance = PipelineLogger(
        name=name, log_dir=log_dir, level=level,
        log_to_file=log_to_file, delete_if_no_errors=delete_if_no_errors,
        use_colors=use_colors, use_icons=use_icons, capture_root=capture_root,
    )
    return _logger_instance


def close_logging() -> None:
    global _logger_instance
    if _logger_instance is not None:
        _logger_instance.close()
        _logger_instance = None


class PipelineLogger:
    def __init__(
        self, name="pipeline", log_dir="logs", level=logging.INFO,
        log_to_file=True, log_to_console=True, delete_if_no_errors=True,
        log_format=None, date_format="%Y-%m-%d %H:%M:%S",
        use_colors=True, use_icons=True, capture_root=True,
    ):
        self.name = name
        self.log_dir = Path(log_dir)
        self.level = level
        self.log_to_file = log_to_file
        self.log_to_console = log_to_console
        self.delete_if_no_errors = delete_if_no_errors
        self.date_format = date_format
        self.use_colors = use_colors
        self.use_icons = use_icons
        self.capture_root = capture_root
        self.log_format = log_format or "%(asctime)s | %(levelname)-8s | %(filename)s:%(funcName)s:%(lineno)d | %(message)s"
        self._log_file_path: Optional[Path] = None
        self._file_handler: Optional[logging.FileHandler] = None
        self._console_handler: Optional[logging.StreamHandler] = None
        self._has_errors = False
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.handlers = []
        self.logger.propagate = False
        self._setup_logging()

    def _setup_logging(self):
        if self.log_to_file:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            mst_timestamp = _get_mst_timestamp()
            current_datetime = mst_timestamp.strftime('%a_%b_%d_%H%M').lower()
            self._log_file_path = self.log_dir / f"{self.name}_{current_datetime}.log"
            file_formatter = PlainFormatter(self.log_format, datefmt=self.date_format)
            self._file_handler = logging.FileHandler(self._log_file_path, encoding='utf-8')
            self._file_handler.setLevel(logging.INFO)
            self._file_handler.setFormatter(file_formatter)
            self.logger.addHandler(self._file_handler)
        if self.log_to_console:
            console_formatter = ColoredFormatter(
                self.log_format, datefmt=self.date_format,
                use_colors=self.use_colors, use_icons=self.use_icons,
            )
            stdout_stream = sys.stdout
            if sys.platform == "win32" and hasattr(sys.stdout, "buffer"):
                stdout_stream = io.TextIOWrapper(
                    sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
                )
            self._console_handler = logging.StreamHandler(stdout_stream)
            self._console_handler.setLevel(self.level)
            self._console_handler.setFormatter(console_formatter)
            self.logger.addHandler(self._console_handler)
        if self.capture_root:
            root_logger = logging.getLogger()
            root_logger.setLevel(self.level)
            root_logger.handlers = []
            if self._file_handler:
                root_logger.addHandler(self._file_handler)
            if self._console_handler:
                root_logger.addHandler(self._console_handler)
        self._silence_noisy_loggers()

    def _silence_noisy_loggers(self):
        for name in ["azure", "urllib3", "httpx", "asyncio"]:
            logging.getLogger(name).setLevel(logging.WARNING)

    @property
    def log_file_path(self): return self._log_file_path
    @property
    def has_errors(self): return self._has_errors

    def debug(self, msg): self.logger.debug(msg)
    def info(self, msg): self.logger.info(msg)
    def warning(self, msg): self.logger.warning(msg)
    def error(self, msg): self._has_errors = True; self.logger.error(msg)
    def exception(self, msg): self._has_errors = True; self.logger.exception(msg)
    def critical(self, msg): self._has_errors = True; self.logger.critical(msg)

    def success(self, msg):
        if self.use_colors and supports_color():
            self.logger.info(f"{Colors.BRIGHT_GREEN}✓ {msg}{Colors.RESET}")
        else:
            self.logger.info(f"✓ {msg}")

    def header(self, title, char="=", length=60):
        if self.use_colors and supports_color():
            line = char * length
            centered = f" {title} ".center(length, char)
            self.info(f"{Colors.BRIGHT_CYAN}{line}{Colors.RESET}")
            self.info(f"{Colors.BOLD}{Colors.BRIGHT_CYAN}{centered}{Colors.RESET}")
            self.info(f"{Colors.BRIGHT_CYAN}{line}{Colors.RESET}")
        else:
            self.info(char * length)
            self.info(f" {title} ".center(length, char))
            self.info(char * length)

    def section(self, title):
        if self.use_colors and supports_color():
            self.info("")
            self.info(f"{Colors.BRIGHT_BLUE}{'─' * 10} {title} {'─' * 10}{Colors.RESET}")
        else:
            self.info("")
            self.info(f"{'─' * 10} {title} {'─' * 10}")

    @contextmanager
    def timer(self, name):
        start_time = datetime.now()
        self.info(f"Starting: {name}")
        try:
            yield
        finally:
            elapsed = (datetime.now() - start_time).total_seconds()
            self.info(f"Completed: {name} ({elapsed:.2f}s)")

    def close(self):
        if self.capture_root:
            root_logger = logging.getLogger()
            if self._file_handler and self._file_handler in root_logger.handlers:
                root_logger.removeHandler(self._file_handler)
            if self._console_handler and self._console_handler in root_logger.handlers:
                root_logger.removeHandler(self._console_handler)
        if self._file_handler:
            self._file_handler.close()
            self.logger.removeHandler(self._file_handler)
        if self._console_handler:
            self._console_handler.close()
            self.logger.removeHandler(self._console_handler)
        if self.delete_if_no_errors and self._log_file_path and self._log_file_path.exists():
            if not self._has_errors:
                os.remove(self._log_file_path)

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.exception(f"Exception: {exc_val}")
        self.close()
        return False

import logging
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Union, List
from contextlib import contextmanager

from prefect.logging import get_run_logger

from helioscta_api_scrapes.utils import (
    file_utils,
)

# Global logger instance
_logger_instance: Optional['PipelineLogger'] = None


# ANSI color codes
class Colors:
    """ANSI escape codes for terminal colors."""
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    
    # Foreground colors
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    
    # Bright foreground colors
    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"
    
    # Background colors
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"


# Level-to-color mapping
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
    """Check if the terminal supports color output."""
    # Check for explicit disable
    if os.environ.get("NO_COLOR"):
        return False
    
    # Check for explicit enable
    if os.environ.get("FORCE_COLOR"):
        return True
    
    # Check if stdout is a TTY
    if not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
        return False
    
    # Windows-specific handling
    if sys.platform == "win32":
        try:
            # Enable ANSI escape sequences on Windows 10+
            import ctypes
            kernel32 = ctypes.windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
            return True
        except Exception:
            return os.environ.get("TERM") == "xterm"
    
    return True


class ColoredFormatter(logging.Formatter):
    """Custom formatter that adds colors to log output."""
    
    def __init__(
        self,
        fmt: Optional[str] = None,
        datefmt: Optional[str] = None,
        use_colors: bool = True,
        use_icons: bool = True,
    ):
        super().__init__(fmt, datefmt)
        self.use_colors = use_colors and supports_color()
        self.use_icons = use_icons
        
        # Store the original format and create a colored version
        self._original_fmt = fmt
        if self.use_colors and fmt:
            # Replace the location placeholders with a single custom field
            self._colored_fmt = fmt.replace(
                "%(filename)s:%(funcName)s:%(lineno)d",
                "%(colored_location)s"
            )
        else:
            self._colored_fmt = fmt
    
    def format(self, record: logging.LogRecord) -> str:
        # Save original values
        original_levelname = record.levelname
        original_msg = record.msg
        
        if self.use_colors:
            color = LEVEL_COLORS.get(record.levelno, Colors.RESET)
            
            # Color the level name
            record.levelname = f"{color}{record.levelname}{Colors.RESET}"
            
            # Create colored location string
            record.colored_location = (
                f"{Colors.CYAN}{record.filename}{Colors.RESET}:"
                f"{Colors.BRIGHT_MAGENTA}{record.funcName}{Colors.RESET}:"
                f"{Colors.YELLOW}{record.lineno}{Colors.RESET}"
            )
            
            # Color the message based on level
            if record.levelno >= logging.ERROR:
                record.msg = f"{color}{record.msg}{Colors.RESET}"
            elif record.levelno == logging.WARNING:
                record.msg = f"{color}{record.msg}{Colors.RESET}"
            
            # Temporarily swap format string
            self._style._fmt = self._colored_fmt
        
        # Add icons
        if self.use_icons:
            icon = LEVEL_ICONS.get(record.levelno, "")
            record.levelname = f"{icon} {record.levelname}"
        
        # Format the record
        result = super().format(record)
        
        # Restore original values and format
        record.levelname = original_levelname
        record.msg = original_msg
        if self.use_colors:
            self._style._fmt = self._original_fmt
        
        return result


class PlainFormatter(logging.Formatter):
    """Plain formatter without colors (for file output)."""

    def __init__(
        self,
        fmt: Optional[str] = None,
        datefmt: Optional[str] = None,
        use_icons: bool = False,
    ):
        super().__init__(fmt, datefmt)
        self.use_icons = use_icons

    def format(self, record: logging.LogRecord) -> str:
        if self.use_icons:
            original_levelname = record.levelname
            icon = LEVEL_ICONS.get(record.levelno, "")
            record.levelname = f"{icon} {record.levelname}"
            result = super().format(record)
            record.levelname = original_levelname
            return result
        return super().format(record)


class PrefectHandler(logging.Handler):
    """Handler that forwards log records to the Prefect run logger.

    Only emits when running inside a Prefect flow/task run context.
    Falls back silently if no active run context exists (e.g. local execution).
    Uses a re-entrancy guard to prevent infinite recursion (our handler -> prefect logger -> our handler -> ...).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._emitting = False

    def emit(self, record: logging.LogRecord) -> None:
        # Guard against infinite recursion
        if self._emitting:
            return

        try:
            prefect_logger = get_run_logger()
        except Exception:
            # Not inside a Prefect flow/task run — nothing to do
            return

        # Prevent the Prefect run logger from duplicating output to stdout.
        # Our own console handler already writes to stdout, so we strip any
        # console/stream handlers from the underlying logger and keep only
        # the APILogHandler that ships logs to the Prefect server / Web UI.
        # Note: get_run_logger() returns a PrefectLogAdapter (LoggerAdapter),
        # so the real logger with .handlers is on .logger underneath.
        real_logger = getattr(prefect_logger, "logger", prefect_logger)
        real_logger.propagate = False
        for h in list(real_logger.handlers):
            if isinstance(h, logging.StreamHandler) and "API" not in type(h).__name__:
                real_logger.removeHandler(h)

        self._emitting = True
        try:
            msg = self.format(record)
            level = record.levelno

            if level >= logging.CRITICAL:
                prefect_logger.critical(msg)
            elif level >= logging.ERROR:
                prefect_logger.error(msg)
            elif level >= logging.WARNING:
                prefect_logger.warning(msg)
            elif level >= logging.INFO:
                prefect_logger.info(msg)
            else:
                prefect_logger.debug(msg)
        finally:
            self._emitting = False


def get_logger() -> logging.Logger:
    """Get the configured logger."""
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
    """Initialize the global logging configuration.
    
    Args:
        name: Logger name
        log_dir: Directory for log files
        level: Logging level
        log_to_file: Whether to write logs to file
        delete_if_no_errors: Delete log file if no errors occurred
        use_colors: Enable colored output
        use_icons: Enable emoji icons
        capture_root: Also configure root logger to capture standard logging calls
    """
    global _logger_instance
    
    if _logger_instance is not None:
        _logger_instance.close()
    
    _logger_instance = PipelineLogger(
        name=name,
        log_dir=log_dir,
        level=level,
        log_to_file=log_to_file,
        delete_if_no_errors=delete_if_no_errors,
        use_colors=use_colors,
        use_icons=use_icons,
        capture_root=capture_root,
    )
    
    return _logger_instance


def close_logging() -> None:
    """Close and cleanup the global logger."""
    global _logger_instance
    if _logger_instance is not None:
        _logger_instance.close()
        _logger_instance = None


class PipelineLogger:
    """Simple pipeline logger with file and console output, with color support."""
    
    def __init__(
        self,
        name: str = "pipeline",
        log_dir: Union[str, Path] = "logs",
        level: int = logging.INFO,
        log_to_file: bool = True,
        log_to_console: bool = True,
        delete_if_no_errors: bool = True,
        log_format: Optional[str] = None,
        date_format: str = "%Y-%m-%d %H:%M:%S",
        use_colors: bool = True,
        use_icons: bool = True,
        capture_root: bool = True,
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
        self._prefect_handler: Optional[PrefectHandler] = None
        self._has_errors = False
        
        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.handlers = []  # Clear existing handlers
        self.logger.propagate = False  # Add this line
        
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Configure file and console handlers."""
        # File handler (no colors)
        if self.log_to_file:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            
            mst_timestamp = file_utils.get_mst_timestamp()
            current_datetime = mst_timestamp.strftime('%a_%b_%d_%H%M').lower()

            self._log_file_path = self.log_dir / f"{self.name}_{current_datetime}.log"
            
            file_formatter = PlainFormatter(self.log_format, datefmt=self.date_format)
            
            self._file_handler = logging.FileHandler(self._log_file_path, encoding='utf-8')
            # self._file_handler.setLevel(self.level)
            self._file_handler.setLevel(logging.INFO)
            self._file_handler.setFormatter(file_formatter)
            self.logger.addHandler(self._file_handler)
        
        # Console handler (with colors)
        if self.log_to_console:
            console_formatter = ColoredFormatter(
                self.log_format,
                datefmt=self.date_format,
                use_colors=self.use_colors,
                use_icons=self.use_icons,
            )
            
            self._console_handler = logging.StreamHandler(sys.stdout)
            self._console_handler.setLevel(self.level)
            self._console_handler.setFormatter(console_formatter)
            self.logger.addHandler(self._console_handler)
        
        # Prefect handler (forwards logs to Prefect UI)
        prefect_formatter = PlainFormatter(self.log_format, datefmt=self.date_format)
        self._prefect_handler = PrefectHandler()
        self._prefect_handler.setLevel(self.level)
        self._prefect_handler.setFormatter(prefect_formatter)
        self.logger.addHandler(self._prefect_handler)

        # Also configure root logger to capture standard logging.info() calls etc.
        if self.capture_root:
            root_logger = logging.getLogger()
            root_logger.setLevel(self.level)
            root_logger.handlers = []  # Clear existing handlers
            
            # Add file handler to root logger
            if self._file_handler:
                root_logger.addHandler(self._file_handler)
            
            # Add console handler to root logger
            if self._console_handler:
                root_logger.addHandler(self._console_handler)

            # Add Prefect handler to root logger
            if self._prefect_handler:
                root_logger.addHandler(self._prefect_handler)
    
        # NOTE: Silence Prefect loggers
        self._silence_noisy_loggers()


    def _silence_noisy_loggers(self):
        """Silence verbose third-party loggers.

        NOTE: We silence specific prefect sub-loggers rather than the top-level
        "prefect" logger.  The run logger (prefect.flow_runs / prefect.task_runs)
        must stay at INFO so that PrefectHandler can forward pipeline logs to the
        Prefect server and they appear in the Web UI "Logs" tab.
        """
        noisy_loggers = [
            "azure",
            "azure.core.pipeline.policies.http_logging_policy",
            # Silence noisy Prefect internals but NOT the run loggers
            "prefect.client",
            "prefect.server",
            "prefect.infrastructure",
            "prefect.worker",
            "prefect.concurrency",
            "prefect._internal",
            "prefect.utilities",
            "urllib3",
            "httpx",
            "asyncio",
        ]
        for name in noisy_loggers:
            logging.getLogger(name).setLevel(logging.WARNING)

    @property
    def log_file_path(self) -> Optional[Path]:
        """Return the path to the current log file."""
        return self._log_file_path
    
    @property
    def has_errors(self) -> bool:
        """Check if any errors have been logged."""
        return self._has_errors
    
    # Logging methods
    def debug(self, msg: str) -> None:
        self.logger.debug(msg)
    
    def info(self, msg: str) -> None:
        self.logger.info(msg)
    
    def warning(self, msg: str) -> None:
        self.logger.warning(msg)
    
    def error(self, msg: str) -> None:
        self._has_errors = True
        self.logger.error(msg)
    
    def exception(self, msg: str) -> None:
        self._has_errors = True
        self.logger.exception(msg)
    
    def critical(self, msg: str) -> None:
        self._has_errors = True
        self.logger.critical(msg)
    
    def success(self, msg: str) -> None:
        """Log a success message (INFO level with green color)."""
        if self.use_colors and supports_color():
            colored_msg = f"{Colors.BRIGHT_GREEN}✓ {msg}{Colors.RESET}"
            self.logger.info(colored_msg)
        else:
            self.logger.info(f"✓ {msg}")
    
    # Formatting utilities
    def header(self, title: str, char: str = "=", length: int = 60) -> None:
        """Print a colored header."""
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
    
    def section(self, title: str) -> None:
        """Print a colored section divider."""
        if self.use_colors and supports_color():
            self.info("")
            self.info(f"{Colors.BRIGHT_BLUE}{'─' * 10} {title} {'─' * 10}{Colors.RESET}")
        else:
            self.info("")
            self.info(f"{'─' * 10} {title} {'─' * 10}")
    
    def divider(self, char: str = "─", length: int = 40) -> None:
        """Print a simple divider line."""
        if self.use_colors and supports_color():
            self.info(f"{Colors.DIM}{char * length}{Colors.RESET}")
        else:
            self.info(char * length)
    
    @contextmanager
    def timer(self, name: str):
        """Context manager for timing operations with colored output."""
        start_time = datetime.now()
        if self.use_colors and supports_color():
            self.info(f"{Colors.BRIGHT_MAGENTA}⏱️  Starting: {name}{Colors.RESET}")
        else:
            self.info(f"⏱️  Starting: {name}")
        try:
            yield
        finally:
            elapsed = (datetime.now() - start_time).total_seconds()
            if self.use_colors and supports_color():
                self.info(f"{Colors.BRIGHT_GREEN}✅ Completed: {name} ({elapsed:.2f}s){Colors.RESET}")
            else:
                self.info(f"✅ Completed: {name} ({elapsed:.2f}s)")
    
    def progress(self, current: int, total: int, prefix: str = "", width: int = 30) -> None:
        """Print a colored progress bar."""
        percent = current / total if total > 0 else 0
        filled = int(width * percent)
        bar = "█" * filled + "░" * (width - filled)
        
        if self.use_colors and supports_color():
            color = Colors.BRIGHT_GREEN if percent >= 1 else Colors.BRIGHT_YELLOW
            msg = f"{prefix} {color}[{bar}]{Colors.RESET} {percent:.1%} ({current}/{total})"
        else:
            msg = f"{prefix} [{bar}] {percent:.1%} ({current}/{total})"
        
        self.info(msg)
    
    def close(self) -> None:
        """Clean up handlers."""
        # Remove handlers from root logger first
        if self.capture_root:
            root_logger = logging.getLogger()
            if self._file_handler and self._file_handler in root_logger.handlers:
                root_logger.removeHandler(self._file_handler)
            if self._console_handler and self._console_handler in root_logger.handlers:
                root_logger.removeHandler(self._console_handler)
            if self._prefect_handler and self._prefect_handler in root_logger.handlers:
                root_logger.removeHandler(self._prefect_handler)

        if self._file_handler:
            self._file_handler.close()
            self.logger.removeHandler(self._file_handler)

        if self._console_handler:
            self._console_handler.close()
            self.logger.removeHandler(self._console_handler)

        if self._prefect_handler:
            self._prefect_handler.close()
            self.logger.removeHandler(self._prefect_handler)
        
        # Delete log file if no errors
        if self.delete_if_no_errors and self._log_file_path and self._log_file_path.exists():
            if not self._has_errors:
                os.remove(self._log_file_path)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.exception(f"Exception: {exc_val}")
        self.close()
        return False
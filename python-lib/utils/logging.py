import logging


class LazyLogger:
    _logger = None
    _initialized = False

    @classmethod
    def _initialize_logger(cls):
        if not cls._initialized:
            # Default log level is INFO
            log_level = 'INFO'

            # Map log level to logging constants
            level = getattr(logging, log_level.upper(), logging.INFO)
            if not isinstance(level, int):
                raise ValueError(f'Invalid log level: {log_level}')

            # Initialize the logger
            if cls._logger is None:
                cls._logger = logging.getLogger(__name__)
            cls._logger.setLevel(level)

            # Add a stream handler if no handlers exist
            if not cls._logger.handlers:
                handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)
                cls._logger.addHandler(handler)
                cls._logger.propagate = False

            cls._initialized = True

    @classmethod
    def set_level(cls, level):
        """
        Dynamically updates the logging level.
        :param level: The new logging level (e.g., 'DEBUG', 'INFO', 'WARNING', etc.)
        """
        cls._initialize_logger()
        level_constant = getattr(logging, level.upper(), None)
        if not isinstance(level_constant, int):
            raise ValueError(f'Invalid log level: {level}')
        cls._logger.setLevel(level_constant)
        cls._logger.info(f"Logging level changed to {level.upper()}")

    def isEnabledFor(self, level):
        """
        Check if the logger is enabled for the specified level.
        :param level: The logging level to check (e.g., logging.DEBUG, logging.INFO)
        :return: True if the logger is enabled for the specified level, False otherwise
        """
        self._initialize_logger()
        return self._logger.isEnabledFor(level)

    def debug(self, msg, *args, **kwargs):
        self._initialize_logger()
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._initialize_logger()
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._initialize_logger()
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._initialize_logger()
        self._logger.error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self._initialize_logger()
        self._logger.critical(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self._initialize_logger()
        self._logger.exception(msg, *args, **kwargs)


logger = LazyLogger()

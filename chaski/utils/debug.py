import logging

try:
    import colorama
except ImportError:
    pass


# ----------------------------------------------------------------------
def styled_logger(logger):
    """"""
    if not "colorama" in globals():
        return logger

    colorama.init(autoreset=True)

    # Desactivar la propagación para evitar mensajes duplicados
    logger.propagate = False

    # Eliminar todos los manejadores existentes del logger
    logger.handlers.clear()

    class CustomFormatter(logging.Formatter):
        FORMATS = {
            logging.DEBUG: colorama.Fore.GREEN
            + "%(levelname)s|%(name)s|%(asctime)s: %(message)s",
            logging.INFO: colorama.Fore.BLUE
            + "%(levelname)s|%(name)s|%(asctime)s: %(message)s",
            logging.WARNING: colorama.Fore.YELLOW
            + "%(levelname)s|%(name)s|%(asctime)s: %(message)s",
            logging.ERROR: colorama.Fore.RED
            + "%(levelname)s|%(name)s|%(asctime)s: %(message)s",
            logging.CRITICAL: colorama.Fore.RED
            + colorama.Back.WHITE
            + "%(levelname)s|%(name)s|%(asctime)s: %(message)s",
        }

        def format(self, record):
            log_fmt = self.FORMATS.get(record.levelno)
            formatter = logging.Formatter(log_fmt)
            return formatter.format(record)

    handler = logging.StreamHandler()
    handler.setFormatter(CustomFormatter())
    logger.addHandler(handler)

    return logger

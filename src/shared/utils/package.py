import importlib
import importlib.util
import pkgutil
import sys

from src.shared.logger import logger


def import_package(package_name: str) -> None:
    try:
        root_package = importlib.import_module(package_name)
    except Exception:
        logger.exception("Failed to import package '%s'", package_name)
        return

    package_path = getattr(root_package, "__path__", None)
    if package_path is None:
        return

    for module_info in pkgutil.walk_packages(package_path, prefix=f"{package_name}."):
        module_name = module_info.name
        if module_name in sys.modules:
            continue
        module_spec = importlib.util.find_spec(module_name)
        if module_spec is None:
            continue
        try:
            importlib.import_module(module_name)
        except Exception:
            logger.exception("Failed to import module '%s'", module_name)

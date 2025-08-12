import logging
from typing import Optional

class Logger:
    """Centralized logging utility"""
    
    _logger = None
    
    @classmethod
    def get_logger(cls, name: Optional[str] = "rapido-pipeline") -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger(name)
            cls._logger.setLevel(logging.INFO)
            
            # Create console handler
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            
            # Create formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            
            # Add handler to logger
            cls._logger.addHandler(ch)
            
        return cls._logger
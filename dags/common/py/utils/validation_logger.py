import logging

def get_validation_logger():
    """Returns a logger with [VALIDATION] prefix for QA/validation tasks."""
    
    qa_logger = logging.getLogger("VALIDATION")
    qa_logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers if the logger is reused
    if not qa_logger.handlers:
        
        class ValidationFormatter(logging.Formatter):
            def format(self, record):
                if "[VALIDATION]" not in record.getMessage():
                    record.msg = f"[VALIDATION] - {record.getMessage()}"
                return super().format(record)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(ValidationFormatter("%(asctime)s - %(message)s"))
        qa_logger.addHandler(stream_handler)
        qa_logger.propagate = False

    return qa_logger
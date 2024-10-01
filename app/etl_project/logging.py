import logging
import time

def setup_pipeline_logging(pipeline_name: str, log_folder_path: str):
    # Initialize logger
    logger = logging.getLogger(pipeline_name)
    logger.setLevel(logging.INFO)

    # Create log file path
    file_path = f"{log_folder_path}/{pipeline_name}_{time.time()}.log"

    # Create handlers
    file_handler = logging.FileHandler(file_path)
    stream_handler = logging.StreamHandler()

    # Set logging levels
    file_handler.setLevel(logging.INFO)
    stream_handler.setLevel(logging.INFO)

    # Create formatters and add them to the handlers
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger, file_path


def get_logs(file_path: str) -> str:
    with open(file_path, "r") as file:
        return "".join(file.readlines())

# Example usage:
# logger, log_file = setup_pipeline_logging("pipeline_name", "/path/to/logs")
# logger.info("This is an info log message.")
# logs = get_logs(log_file)
# print(logs)